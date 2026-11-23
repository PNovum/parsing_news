from contextlib import ExitStack, closing
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class WebHDFSHook:
    """Создаёт клиент WebHDFS с HTTPS и Kerberos для запуска из ноутбука."""

    def __init__(self, host, ca_cert, port=443):
        self.url = f"https://{host}:{port}"
        self.ca_cert = ca_cert

    def get_conn(self):
        """Использует текущий Kerberos-билет и корпоративный CA-сертификат."""
        from hdfs.ext.kerberos import KerberosClient
        from requests import Session

        session = Session()
        session.verify = self.ca_cert
        return KerberosClient(self.url, session=session)


class MonthWriter:
    """Публикует DataFrame через Parquet, WebHDFS и внешнюю таблицу Hive."""

    target = "iceberg__write_access.sandbox_cc.operator_topic_fact"
    groups = {
        "varchar": (
            "session_id segment client_id login portal_id city fio su med_name "
            "bl_type allocation_method level1 level2 level3 model_version"
        ),
        "date": "start_dt report_month",
        "timestamp(6)": "seg_start seg_end employment_date fired_date",
        "bigint": "n_topics is_estimated train_operator_topic_episodes model_alpha",
        "boolean": "topic_candidate",
        "double": (
            "handle_duration tenure_days aht_duration allocation_weight topic_duration "
            "topic_plan_seconds episode_plan_pct mean_3m_sec within_score peer_effect "
            "peer_score peer_score_pct"
        ),
    }

    def __init__(self, lake, hdfs, location="hdfs:///data/sandbox/cc/operator_topic_fact"):
        self.lake = lake
        self.hdfs = hdfs
        self.location = location
        self.types = {}
        for sql_type, names in self.groups.items():
            for name in names.split():
                self.types[name] = sql_type

    def _check_input(self, frame, day):
        """Возвращает причину пропуска выгрузки или пустую строку."""
        if not self.location or not self.location.startswith("hdfs://") or "'" in self.location:
            return "Проверьте HDFS-путь витрины."
        if frame.empty:
            return "За выбранный месяц нет данных."
        if not frame.columns.is_unique or set(frame.columns) != set(self.types):
            return "Колонки DataFrame отличаются от схемы витрины."
        months = frame["report_month"]
        if months.isna().any() or not months.eq(day).all():
            return "В report_month ожидается первое число выбранного месяца."
        key = ["bl_type", "session_id", "segment", "login", "seg_start", "level1", "level2", "level3"]
        if frame.duplicated(key).any():
            return "Есть повторы по ключу сегмента и темы."
        return ""

    def _upload_parts(self, frame, folder):
        """Записывает по 100 000 строк в локальный Parquet и загружает каждый файл в HDFS."""
        arrow_types = {
            "varchar": pa.string(),
            "timestamp(6)": pa.string(),
            "date": pa.date32(),
            "bigint": pa.int64(),
            "boolean": pa.bool_(),
            "double": pa.float64(),
        }
        batch_size = 100_000
        with TemporaryDirectory() as local_dir:
            for start in range(0, len(frame), batch_size):
                batch = frame.iloc[start:start + batch_size]
                arrays = {}
                for name, sql_type in self.types.items():
                    values = batch[name]
                    if sql_type == "varchar":
                        values = values.astype("string")
                    elif sql_type == "date":
                        values = pd.to_datetime(values).dt.date
                    elif sql_type == "timestamp(6)":
                        values = pd.to_datetime(values).dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                    arrays[name] = pa.array(values, type=arrow_types[sql_type], from_pandas=True)

                path = Path(local_dir) / f"part_{start:09d}.parquet"
                pq.write_table(pa.table(arrays), path)
                self.hdfs.upload(hdfs_path=f"{folder}/{path.name}", local_path=str(path))
                path.unlink()

    @staticmethod
    def _run(cursor, sql):
        """Дожидается завершения запроса Trino."""
        cursor.execute(sql)
        return cursor.fetchall()

    def _publish(self, cursor, stage, day, columns, projection):
        """Добавляет новый месяц или заменяет существующий срез одной операцией Iceberg."""
        existing = self._run(cursor, f"""
            SELECT 1 FROM {self.target}
            WHERE report_month = DATE '{day}'
            LIMIT 1
        """)
        if not existing:
            self._run(cursor, f"""
                INSERT INTO {self.target} ({columns})
                SELECT {projection} FROM {stage}
            """)
            return

        source_columns = []
        for name in self.types:
            source_columns.append(f"s.{name}")
        values = ", ".join(source_columns)
        self._run(cursor, f"""
            MERGE INTO {self.target} t
            USING (
                SELECT {projection}, false AS clear_month FROM {stage}
                UNION ALL
                SELECT {projection}, true AS clear_month FROM (SELECT * FROM {stage} LIMIT 1)
            ) s
            ON s.clear_month AND t.report_month = s.report_month
            WHEN MATCHED THEN DELETE
            WHEN NOT MATCHED AND NOT s.clear_month THEN
                INSERT ({columns}) VALUES ({values})
        """)

    def write(self, frame, month):
        """Возвращает число опубликованных строк. При пропуске печатает причину и возвращает 0."""
        day = month.start_time.date()
        reason = self._check_input(frame, day)
        if reason:
            print("Выгрузка пропущена.", reason)
            return 0

        name = "tmp_" + uuid4().hex
        folder = f"/data/sandbox/cc/tmp/{name}"
        stage = f"hive__write_access.sandbox_cc.{name}"

        hive_fields = []
        iceberg_fields = []
        select_fields = []
        for column, sql_type in self.types.items():
            iceberg_fields.append(f"{column} {sql_type}")
            if sql_type == "timestamp(6)":
                hive_fields.append(f"{column} varchar")
                select_fields.append(f"CAST({column} AS timestamp(6)) AS {column}")
            else:
                hive_fields.append(f"{column} {sql_type}")
                select_fields.append(column)

        with ExitStack() as cleanup:
            connection = cleanup.enter_context(closing(self.lake.connect()))
            cursor = cleanup.enter_context(closing(connection.cursor()))

            # 1. Загрузка Parquet в HDFS
            self.hdfs.makedirs(folder)
            cleanup.callback(self.hdfs.delete, folder, recursive=True)
            self._upload_parts(frame, folder)

            # 2. Регистрация внешней таблицы Hive и сверка числа строк
            hive_schema = ", ".join(hive_fields)
            self._run(cursor, f"""
                CREATE TABLE {stage} ({hive_schema})
                WITH (format = 'PARQUET', external_location = '{folder}/')
            """)
            cleanup.callback(self._run, cursor, f"DROP TABLE IF EXISTS {stage}")
            result = self._run(cursor, f"SELECT count(*) FROM {stage}")
            hive_rows = result[0][0]
            if hive_rows != len(frame):
                print(f"Выгрузка пропущена. В Hive {hive_rows} строк, в DataFrame {len(frame)}.")
                return 0

            # 3. Создание витрины и публикация месяца в Iceberg
            iceberg_schema = ", ".join(iceberg_fields)
            self._run(cursor, f"""
                CREATE TABLE IF NOT EXISTS {self.target} ({iceberg_schema})
                WITH (
                    location = '{self.location}',
                    format = 'PARQUET',
                    partitioning = ARRAY['report_month'],
                    format_version = 2
                )
            """)
            columns = ", ".join(self.types)
            projection = ", ".join(select_fields)
            self._publish(cursor, stage, day, columns, projection)

        return len(frame)
