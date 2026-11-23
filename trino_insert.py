from contextlib import closing


def insert_fact(lake, source_table: str, month):
    """Добавляет строки месяца из таблицы Trino с той же схемой и порядком колонок."""
    catalog, schema, table = source_table.split(".")
    source = ".".join('"' + part.replace('"', '""') + '"' for part in (catalog, schema, table))
    sql = f"""
        INSERT INTO iceberg__write_access.sandbox_cc.operator_topic_fact
        SELECT *
        FROM {source}
        WHERE report_month = ?
    """
    with closing(lake.connect()) as conn, closing(conn.cursor()) as cursor:
        cursor.execute(sql, [month.start_time.date()])
        cursor.fetchall()
