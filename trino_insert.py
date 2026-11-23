"""Запись месячного pandas DataFrame через существующее подключение Trino."""
from contextlib import closing

import pandas as pd


TABLE = "iceberg__write_access.sandbox_cc.operator_topic_fact"
KEY = ["report_month", "bl_type", "session_id", "segment", "login",
       "seg_start", "level1", "level2", "level3"]


def insert_fact(lake, frame, month):
    """Дописывает отсутствующие строки. Для месяца запускайте одну загрузку за раз."""
    if frame.empty:
        return
    if not frame["report_month"].isin([month.start_time.date()]).all():
        raise ValueError("В выгрузке есть строки другого месяца или без даты")
    if frame.duplicated(KEY).any():
        raise ValueError("В DataFrame есть повтор ключа сегмент × тема")

    with closing(lake.connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(f"SHOW COLUMNS FROM {TABLE}")
        types = {row[0]: row[1] for row in cur.fetchall()}
        data = frame.copy(deep=False)
        fields = []
        for i, col in enumerate(frame.columns):
            kind = types[col]
            if kind == "date":
                data[col] = pd.to_datetime(frame[col]).dt.strftime("%Y-%m-%d")
            elif kind.startswith("timestamp"):
                data[col] = pd.to_datetime(frame[col]).dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            elif kind == "bigint":
                data[col] = frame[col].astype("Int64")
            elif kind.startswith("varchar"):
                data[col] = frame[col].astype("string")
            fields.append(f"CAST(json_extract_scalar(item, '$[{i}]') AS {types[col]})")
        names = ", ".join(frame.columns)
        match = " AND ".join(f"old.{k} IS NOT DISTINCT FROM src.{k}" for k in KEY)
        sql = f"""
            INSERT INTO {TABLE} ({names})
            WITH src ({names}) AS (
                SELECT {", ".join(fields)}
                FROM UNNEST(CAST(json_parse(?) AS ARRAY(JSON))) AS t(item)
            )
            SELECT src.* FROM src
            WHERE NOT EXISTS (SELECT 1 FROM {TABLE} old WHERE {match})
        """

        payload = data.to_json(orient="values", force_ascii=False, double_precision=15)
        cur.execute(sql, [payload])
        cur.fetchall()
