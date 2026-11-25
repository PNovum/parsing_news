from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F


class SparkCC:
    def __init__(self, spark):
        self.spark = spark

    def sql_file(self, path, **params):
        query = Path(path).read_text(encoding="utf-8").format(**params)
        return self.spark.sql(query)

    def insert_missing(self, table, frame, keys=None):
        columns = self.spark.table(table).columns
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
