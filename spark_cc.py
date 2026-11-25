from pathlib import Path
from uuid import uuid4

import pandas as pd
from pyspark.sql import functions as F


class SparkCC:
    def __init__(self, spark):
        self.spark = spark

    @classmethod
    def connect(cls, principal, keytab_path, app_name="yarn"):
        from spark_magic.utils import get_spark_session

        return cls(get_spark_session(app_name, principal=principal, keytab_path=keytab_path))

    def create_table(self, table, ddl_path, **params):
        self.sql_file(ddl_path, table=table, **params).collect()

    def drop_table(self, table):
        self.spark.sql(f"DROP TABLE IF EXISTS {table}").collect()

    def to_spark(self, frame, schema):
        """Подготовить pandas DataFrame по StructType таблицы."""
        data = {}
        for field in schema:
            column = frame[field.name]
            kind = field.dataType.simpleString()
            try:
                if kind in ("tinyint", "smallint", "int", "bigint"):
                    column = column.astype("Int64")
                elif kind in ("float", "double"):
                    column = column.astype("Float64")
                elif kind == "string":
                    column = column.astype("string")
                elif kind == "boolean":
                    column = column.astype("boolean")
                elif kind == "date":
                    column = pd.to_datetime(column).dt.date
                elif kind in ("timestamp", "timestamp_ntz"):
                    dates = pd.to_datetime(column)
                    if kind == "timestamp_ntz" and dates.dt.tz is not None:
                        raise ValueError("Для TIMESTAMP_NTZ укажите локальное время без часового пояса")
                    column = pd.Series(
                        [v.to_pydatetime() if pd.notna(v) else None for v in dates],
                        index=frame.index, dtype=object,
                    )
                else:
                    raise TypeError(f"Неподдерживаемый тип {kind}")
                data[field.name] = column.astype(object).where(column.notna(), None)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"Поле {field.name} ({kind}): {exc}") from exc

        rows = pd.DataFrame(data).itertuples(index=False, name=None)
        return self.spark.createDataFrame(rows, schema)

    def sql_file(self, path, **params):
        query = Path(path).read_text(encoding="utf-8").format(**params)
        return self.spark.sql(query)

    def insert_missing(self, table, frame, keys=None):
        schema = self.spark.table(table).schema
        if isinstance(frame, pd.DataFrame):
            frame = self.to_spark(frame, schema)
        columns = schema.names
        data = frame.select(*columns).distinct()
        if keys is None:
            keys = columns
        else:
            if not isinstance(keys, (list, tuple)) or not keys or set(keys) - set(columns):
                raise ValueError(f"Укажите ключи из полей таблицы: {columns}")
            counts = data.groupBy(*keys).agg(F.count("*").alias("__rows"))
            if counts.where(counts["__rows"] > 1).take(1):
                raise ValueError(f"В выгрузке есть разные строки с одинаковым ключом {keys}")

        view = "incoming_" + uuid4().hex
        condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)
        data.createOrReplaceTempView(view)
        try:
            self.spark.sql(f"""
                MERGE INTO {table} t USING {view} s ON {condition}
                WHEN NOT MATCHED THEN INSERT *
            """).collect()
        finally:
            self.spark.catalog.dropTempView(view)
