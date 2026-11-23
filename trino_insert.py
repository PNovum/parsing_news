"""Прямая запись месячного DataFrame в Iceberg через Trino."""
from contextlib import closing

import pandas as pd


TABLE = "iceberg__write_access.sandbox_cc.operator_topic_fact"
LOCATION = "hdfs:///data/sandbox/cc/operator_topic_fact"

# Порядок полей общий для SQL и входного пакета.
GROUPS = {
    "varchar": "session_id segment client_id login portal_id city fio su med_name bl_type allocation_method level1 level2 level3 model_version",
    "date": "start_dt report_month",
    "timestamp(6)": "seg_start seg_end employment_date fired_date",
    "bigint": "n_topics is_estimated train_operator_topic_episodes model_alpha",
    "boolean": "topic_candidate",
    "double": "handle_duration tenure_days aht_duration allocation_weight topic_duration topic_plan_seconds episode_plan_pct mean_3m_sec within_score peer_effect peer_score peer_score_pct",
}

KEY = ["bl_type", "session_id", "segment", "login", "seg_start", "level1", "level2", "level3"]


def insert_fact(lake, frame, month):
    """Дописывает отсутствующие ключи месяца. Один пишущий процесс на месяц."""
    columns = []
    fields = []
    expressions = []
    for sql_type, names in GROUPS.items():
        for name in names.split():
            index = len(columns)
            columns.append(name)
            fields.append(f"{name} {sql_type}")
            expressions.append(f"CAST(json_extract_scalar(item, '$[{index}]') AS {sql_type}) AS {name}")

    day = month.start_time.date()
    if frame.empty:
        print("За месяц нет строк.")
        return
    if not frame.columns.is_unique or set(columns) != set(frame.columns):
        print("Проверьте колонки витрины:", sorted(set(columns) ^ set(frame.columns)))
        return
    dates = frame["report_month"]
    if dates.isna().any() or not dates.eq(day).all() or frame.duplicated(KEY).any():
        print("Проверьте отчётный месяц и повторы ключа сегмент × тема.")
        return

    # INSERT пропускает ключи, записанные при предыдущем запуске.
    conditions = []
    for name in KEY:
        conditions.append(f"old.{name} IS NOT DISTINCT FROM src.{name}")
    match = " AND ".join(conditions)
    names = ", ".join(columns)
    projection = ", ".join(expressions)
    insert_sql = f"""
        INSERT INTO {TABLE} ({names})
        WITH src AS (
            SELECT {projection}
            FROM UNNEST(CAST(json_parse(?) AS ARRAY(JSON))) AS rows_in(item)
        )
        SELECT src.* FROM src
        WHERE NOT EXISTS (
            SELECT 1 FROM {TABLE} old
            WHERE old.report_month = DATE '{day}' AND {match}
        )
    """

    with closing(lake.connect()) as conn, closing(conn.cursor()) as cur:
        schema = ", ".join(fields)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} ({schema})
            WITH (location = '{LOCATION}', format = 'PARQUET',
                  partitioning = ARRAY['report_month'], format_version = 2)
        """)
        cur.fetchall()

        batch_size = 1000
        for start in range(0, len(frame), batch_size):
            batch = frame.iloc[start:start + batch_size].loc[:, columns].copy()
            for name in GROUPS["timestamp(6)"].split():
                batch[name] = pd.to_datetime(batch[name]).dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            for name in GROUPS["date"].split():
                batch[name] = pd.to_datetime(batch[name]).dt.strftime("%Y-%m-%d")
            for name in GROUPS["varchar"].split():
                batch[name] = batch[name].astype("string")

            payload = batch.to_json(orient="values", double_precision=15)
            cur.execute(insert_sql, [payload])
            cur.fetchall()

        cur.execute(f"SELECT count(*) FROM {TABLE} WHERE report_month = DATE '{day}'")
        count = cur.fetchone()[0]
        print(f"{day:%Y-%m}: в DataFrame {len(frame)}, в таблице {count} строк.")
