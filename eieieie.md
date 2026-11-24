# Загрузка данных в Iceberg из ноутбука

При первом запуске выполните ячейки 1–4. В следующий раз передайте свежую выгрузку в ячейку 4. Для записи в Iceberg достаточно `db.insert_missing(table, sdf, keys)`, когда таблица уже создана и типы полей совпадают. Блок копирования HDFS можно пропустить.

## 1. Подключиться

Скопируйте класс из приложения в ячейку ноутбука и выполните её один раз за сессию. Если корпоративное ядро уже предоставило `spark`, выполните:

```python
db = SparkCC(spark)
```

Для подключения по keytab создайте рядом с ноутбуком личный файл `.env`:

```dotenv
SPARK_APP_NAME=analytics
SPARK_PRINCIPAL=your_login@YOUR_REALM
SPARK_KEYTAB=/home/your_login/private/your_login.keytab
```

```python
# Вариант для нового подключения
db = SparkCC.connect()
spark = db.spark
```

## 2. Вписать имя таблицы, поля и ключи

Ниже пример для витрины, где одна строка соответствует одному сегменту сессии. Для своей витрины поменяйте имя, описание полей и ключ. Ключ должен описывать одну строку именно ваших данных.

```python
table_name = "callflow_demo"
table = f"sandbox_cc.{table_name}"
location = f"hdfs:///data/sandbox/cc/{table_name}"

fields = {
    "session_id": "STRING",
    "segment": "INT",
    "start_dt": "DATE",
    "handle_duration": "INT",
}
keys = ["session_id", "segment"]  # Вместе определяют одну строку
```

## 3. Создать пустую таблицу

```python
db.create_table(table, fields, location)
```

Внутри выполняется `CREATE TABLE IF NOT EXISTS … USING iceberg LOCATION … TBLPROPERTIES …`. Для новой таблицы укажите свободный путь. Если таблица уже есть, её схема сохранится. Для дозагрузки в существующую витрину эту ячейку можно пропустить.

## 4. Дозагрузить готовый DataFrame

**Если данные уже в pandas**, подставьте свою переменную вместо `result_pdf`. Пример ниже подготавливает поле даты. Для уже подготовленного DataFrame достаточно строки с `createDataFrame`.

```python
import pandas as pd

pdf = result_pdf.copy()  # Готовый результат вашего расчёта
pdf["start_dt"] = pd.to_datetime(
    pdf["start_dt"],
    dayfirst=True,     # Для 03/04/2026 предпочесть 3 апреля
    # format="mixed",  # Включите при разных форматах в одном столбце (pandas 2+)
).dt.date              # Оставить только дату для поля DATE
sdf = spark.createDataFrame(pdf)  # Вся выгрузка из pandas в Spark
```

Если формат известен заранее, укажите его явно, например `format="%d.%m.%Y"`. Для поля `TIMESTAMP` уберите `.dt.date`, чтобы сохранить часы, минуты и секунды. Если выгрузка пуста или столбец целиком состоит из пропусков, при создании Spark DataFrame задайте совместимую явную схему.

**Если данные уже в Spark**, начните здесь. Вместо `sdf` подставьте готовый Spark DataFrame. Типы входных значений должны соответствовать целевой схеме.

```python
from pyspark.sql import functions as F

# Выбрать поля приёмника и привести типы (например, bigint из pandas → int в таблице)
incoming = sdf.select([F.col(name).cast(dtype).alias(name) for name, dtype in fields.items()])
db.insert_missing(table, incoming, keys)  # Записать в Iceberg только новые ключи
```

Если поля и типы уже совпадают, вся дозагрузка — **одна строка**:

```python
db.insert_missing(table, sdf, keys)
```

| Что пришло в выгрузке | Что сделает метод |
| --- | --- |
| Ключ уже есть в таблице, даже если изменился handle_duration | Сохранит прежнюю строку |
| Ключа ещё нет | Добавит строку |
| Одна строка пришла несколько раз | Учтёт её один раз |
| В одной поставке один ключ и разные значения остальных полей | Остановится до записи и назовёт поля ключа |

Метод сравнивает весь входной DataFrame с таблицей одним `MERGE`. Два NULL в одном поле ключа считаются совпадением. Можно повторно передать всю выгрузку после завершения записи. Загружайте поставки с пересекающимися ключами последовательно. В сессии нужна поддержка Iceberg SQL extensions. Исключения Spark выводятся в ячейке.

## Другие операции · независимые ячейки

**Прочитать нужные столбцы в pandas**

```python
columns = ["session_id", "handle_duration"]
sdf_read = spark.table(table).select(*columns)
pdf_read = sdf_read.limit(1000).toPandas()  # Ограничение только для просмотра
# sdf_all = spark.table(table)              # Все поля и строки остаются в Spark
```

**Загрузить файлы из обычной поставки Parquet**

```python
path = "hdfs:///data/sandbox/cc/incoming/callflow"  # Папка входной выгрузки
incoming = spark.read.parquet(path).select(*fields)  # Прочитать файлы в Spark DataFrame
db.insert_missing(table, incoming, keys)
```

Для зарегистрированной Iceberg-таблицы читайте `spark.table(source_table)`. Чтение её физической папки `data/` как Parquet обходит метаданные текущего снимка и может вернуть старые или удалённые строки.

**Дописать все строки, включая повторные ключи**

```python
incoming.writeTo(table).append()  # Каждый запуск снова добавляет всю поставку
```

**Создать независимую таблицу с текущими данными под именем с суффиксом _del**

```python
copy_table = f"sandbox_cc.{table_name}_del"
copy_path = f"hdfs:///data/sandbox/cc/{table_name}_del"

# Для нового имени и свободного пути. Spark сам запишет data и metadata.
spark.sql(f"""
    CREATE TABLE {copy_table} USING iceberg
    LOCATION '{copy_path}'
    AS SELECT * FROM {table}
""")
```

Так копируются строки текущего снимка. У новой таблицы будет собственная история. Если нужны конкретные свойства или разбиение, задайте их в DDL.

**Служебная операция · сделать файловую копию в HDFS**

Запускайте эту ячейку, только если нужны копии папок `data/` и `metadata/` по другому пути. `FileUtil.copy` копирует файлы. Строки целевой Iceberg-таблицы при этом не меняются. Требуется классический PySpark с доступом к JVM. Целевой путь должен быть свободен. Для согласованной файловой копии выполняйте её при остановленных записях и обслуживании исходной таблицы.

```python
src = location
dst = f"{location}_files_copy"

sc = spark.sparkContext
conf = sc._jsc.hadoopConfiguration()
hadoop = sc._jvm.org.apache.hadoop.fs
Path = hadoop.Path
fs = Path(src).getFileSystem(conf)

if fs.exists(Path(dst)):
    raise FileExistsError(f"Укажите свободную папку: {dst}")
if not fs.mkdirs(Path(dst)):
    raise OSError(f"Не удалось создать {dst}")

for folder in ["data", "metadata"]:
    ok = hadoop.FileUtil.copy(fs, Path(f"{src}/{folder}"), fs, Path(f"{dst}/{folder}"), False, conf)
    if not ok:
        raise OSError(f"Не удалось скопировать {folder} в {dst}")
```

Метаданные могут содержать абсолютные пути к исходным файлам. Для независимой рабочей таблицы используйте предыдущую ячейку. Для переноса с историей применяют переписывание путей и регистрацию конкретного metadata.json, если процедуры поддерживает ваш каталог. [Процедуры Apache Iceberg](https://iceberg.apache.org/docs/latest/spark-procedures/#table-replication)

**Удалить таблицу из каталога**

```python
# spark.sql(f"DROP TABLE IF EXISTS {copy_table}")  # Раскомментируйте для удаления копии
```

## Приложение · класс SparkCC

Скопируйте блок целиком в первую ячейку. В примерах используются обычные имена таблиц и столбцов из букв, цифр и подчёркиваний. Имена и DDL задаёт автор ноутбука.

```python
from uuid import uuid4
from pyspark.sql import functions as F


class SparkCC:
    def __init__(self, spark):
        self.spark = spark

    @classmethod
    def connect(cls, path=".env"):
        from dotenv import dotenv_values
        from spark_magic.utils import get_spark_session

        config = dotenv_values(path)
        spark = get_spark_session(
            config["SPARK_APP_NAME"],
            principal=config["SPARK_PRINCIPAL"],
            keytab_path=config["SPARK_KEYTAB"],
        )
        return cls(spark)

    def create_table(self, table, fields, location):
        columns = ", ".join(f"`{name}` {dtype}" for name, dtype in fields.items())
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table} ({columns})
            USING iceberg
            LOCATION '{location}'
            TBLPROPERTIES (
                'format-version' = '2',
                'write.delete.mode' = 'merge-on-read',
                'write.update.mode' = 'merge-on-read',
                'write.merge.mode' = 'merge-on-read',
                'write.target-file-size-bytes' = '536870912',
                'write.distribution-mode' = 'hash',
                'write.parquet.compression-codec' = 'zstd',
                'write.parquet.compression-level' = '3',
                'commit.manifest-merge.enabled' = 'true'
            )
        """)

    def insert_missing(self, table, frame, keys):
        columns = self.spark.table(table).columns
        if not isinstance(keys, (list, tuple)) or not keys or set(keys) - set(columns):
            raise ValueError(f"Укажите ключи списком из полей таблицы: {columns}")

        # Убираем точные повторы. Разные строки с одним ключом требуют решения автора выгрузки.
        data = frame.select(*columns).distinct()
        duplicates = data.groupBy(*keys).agg(F.count("*").alias("__rows_per_key"))
        if duplicates.where(F.col("__rows_per_key") > 1).take(1):
            raise ValueError(f"В выгрузке разные строки с одним ключом {keys}. Уточните ключ или выберите нужную строку.")

        view = "incoming_" + uuid4().hex
        condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)
        data.createOrReplaceTempView(view)
        try:
            self.spark.sql(f"""
                MERGE INTO {table} AS t
                USING {view} AS s ON {condition}
                WHEN NOT MATCHED THEN INSERT *
            """).collect()
        finally:
            self.spark.catalog.dropTempView(view)
```




```python
import os
from spark_magic.utils import get_spark_session
from spark_connect import SparkCC  # Класс из твоего файла spark_connect.py

spark = get_spark_session(
    "operator_topic_fact",
    principal=os.environ["DATALAB_KERBEROS_PRINCIPAL"],
    keytab_path=os.environ["DATALAB_KERBEROS_KEYTAB"],
)

db = SparkCC(spark)
spark.sql("SELECT 1 AS ok").show()  # Проверяем, что сессия выполняет запросы






table = "sandbox_cc.operator_topic_fact"
spark.table(table).printSchema()








from pyspark.sql import types as T

pdf = operator_topic_fact
report_day = month.start_time.date()

if not pdf["report_month"].isin([report_day]).all():
    raise ValueError(f"В выгрузке есть строки без даты или за другой месяц. Ожидается {report_day}.")

if pdf.duplicated(keys).any():
    raise ValueError("В результате модели повторяется ключ «сегмент × тема».")

if pdf.empty:
    print(f"За {report_day:%Y-%m} нет строк для загрузки.")
else:
    data = pdf.loc[:, target_schema.names].copy()  # Порядок полей как в таблице

    for field in target_schema:
        col = field.name
        dtype = field.dataType

        if isinstance(dtype, T.DateType):
            data[col] = pd.to_datetime(data[col]).dt.date
        elif isinstance(dtype, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
            data[col] = pd.to_numeric(data[col]).astype("Int64")
        elif isinstance(dtype, (T.FloatType, T.DoubleType)):
            data[col] = pd.to_numeric(data[col]).astype(float)
        elif isinstance(dtype, T.StringType):
            data[col] = data[col].astype("string")

    data = data.astype(object).where(data.notna(), None)  # Пропуски pandas → NULL
    sdf = spark.createDataFrame(data, schema=target_schema)

    db.insert_missing(table, sdf, keys)
    print(f"Загрузка за {report_day:%Y-%m} завершена. Строк в поставке: {len(pdf):,}")

```