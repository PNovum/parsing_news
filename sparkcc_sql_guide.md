# SparkCC в Jupyter

Положите `spark_cc.py` рядом с ноутбуком. Пример создаёт Iceberg-таблицу с полями `key_1`, `key_2` и `amount`, читает выгрузку за месяц и добавляет недостающие записи. Каждый блок ниже — отдельная ячейка.

## 1. Инициализировать Spark

Подставьте свой Kerberos principal и путь к keytab, доступному из ноутбука. Если сессия `spark` уже открыта, достаточно импортировать класс и выполнить `db = SparkCC(spark)`.

```python
from spark_cc import SparkCC

db = SparkCC.connect(principal="login@REALM", keytab_path="/path/to/user.keytab")
spark = db.spark
```

## 2. Задать таблицы и создать папку для SQL

Замените имена таблиц и укажите отдельный каталог HDFS для новой таблицы. В примере источник содержит поля `source_id`, `source_date` и `source_amount`.

```python
from pathlib import Path

Path("sql_spark").mkdir(exist_ok=True)
table = "sandbox_cc.table"
source = "sandbox_cc.source_table"
location = "hdfs:///data/sandbox/cc/table"
```

## 3. Создать таблицу через create_table

Выполните ячейку с `%%writefile` в первой строке. Jupyter сохранит SQL в файл. В скобках `{table}` и `{location}` указываются параметры, значения которых передадим из Python. В определении таблицы задайте названия полей и их типы.

```sql
%%writefile sql_spark/create_table.sql
CREATE TABLE IF NOT EXISTS {table} (
    key_1  STRING,
    key_2  DATE,
    amount DOUBLE
)
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
```

В следующей ячейке выполните файл. При повторном запуске существующая таблица сохранит данные и схему.

```python
db.create_table(table, "sql_spark/create_table.sql", location=location)
```

## 4. Прочитать выгрузку через sql_file

Во втором SQL-файле укажите источник, сопоставьте его поля с колонками приёмника и задайте фильтр по дате. Нижняя граница входит в период, верхняя исключается.

```sql
%%writefile sql_spark/select_batch.sql
SELECT
    CAST(source_id AS STRING)     AS key_1,
    CAST(source_date AS DATE)     AS key_2,
    CAST(source_amount AS DOUBLE) AS amount
FROM {source}
WHERE source_date >= DATE '{date_from}'
  AND source_date <  DATE '{date_to}'
```

Прочитайте октябрь в Spark DataFrame. Метод подставляет параметры в текст SQL, поэтому передавайте свои значения из настроек ноутбука.

```python
frame = db.sql_file(
    "sql_spark/select_batch.sql",
    source=source,
    date_from="2026-10-01",
    date_to="2026-11-01",
)
frame.show(5)
```

## 5. Дозагрузить данные через insert_missing

Выберите **одну** из двух ячеек в зависимости от того, что считается повтором.

### Вариант А. Совпадение по ключу

```python
db.insert_missing(table, frame, keys=["key_1", "key_2"])
```

Если пара уже есть в таблице, метод пропустит входящую запись, даже когда изменился `amount`. Новая пара добавится. Если в самой выгрузке одной паре соответствуют разные суммы, метод остановится до записи и сообщит о конфликте.

### Вариант Б. Совпадение по всей строке

```python
db.insert_missing(table, frame)
```

Без аргумента `keys` метод сравнит все поля приёмника. При той же паре ключей и новой сумме добавится ещё одна строка. Полное совпадение будет пропущено.

Оба варианта убирают точные повторы внутри выгрузки. Два значения `NULL` в сравниваемом поле считаются равными.

Проверить результат можно в той же сессии:

```python
spark.table(table).show(10)
```

## 6. Записать результат из pandas

Передайте pandas DataFrame прямо в метод дозагрузки. Класс прочитает схему существующей таблицы, возьмёт нужные колонки в её порядке и преобразует значения. Поддерживаются STRING, целые типы, FLOAT, DOUBLE, BOOLEAN, DATE, TIMESTAMP и TIMESTAMP_NTZ. Для BOOLEAN используйте True/False или 1/0. Пропуски становятся NULL. Для готового Spark DataFrame подготовьте типы в исходном SELECT.

```python
db.insert_missing(table, pdf, keys=["key_1", "key_2"])
```

Чтобы получить Spark DataFrame для дальнейших вычислений отдельно от записи:

```python
sdf = db.to_spark(pdf, spark.table(table).schema)
```

В ноутбуке модели после создания fact-таблицы весь блок подготовки и записи заменяет одна строка:

```python
db.insert_missing(fact_table, operator_topic_fact, keys=fact_keys)
```

Запускайте загрузки в одну таблицу последовательно, если ключи могут пересекаться.

## 7. Удалить таблицу

Отдельная операция для случая, когда таблицу действительно нужно удалить из каталога:

```python
db.drop_table(table)
```
