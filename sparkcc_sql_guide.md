# SparkCC в Jupyter

Положите `spark_cc.py` рядом с ноутбуком. Пример создаёт Iceberg-таблицу с полями `key_1`, `key_2` и `amount`, читает выгрузку за месяц и добавляет недостающие записи. Python-блоки выполняйте в ячейках Jupyter. SQL-блоки сохраните в указанные файлы.

## 1. Инициализировать Spark

Подставьте свой Kerberos principal и путь к keytab, доступному из ноутбука. Если сессия `spark` уже открыта, достаточно импортировать класс и выполнить `db = SparkCC(spark)`.

```python
from spark_cc import SparkCC

db = SparkCC.connect(principal="login@REALM", keytab_path="/path/to/user.keytab")
spark = db.spark
```

## 2. Создать таблицу

Сначала задайте имя таблицы и отдельную папку HDFS для её данных. Папка `sql_spark` рядом с ноутбуком предназначена для SQL-файлов.

```python
from pathlib import Path

sql_dir = Path("sql_spark")
sql_dir.mkdir(exist_ok=True)

def query(filename, **params):
    return (sql_dir / filename).read_text(encoding="utf-8").format(**params)

table = "sandbox_cc.table"
source = "sandbox_cc.source_table"
location = "hdfs:///data/sandbox/cc/table"
```

Создайте файл `sql_spark/create_table.sql` и запишите в него определение таблицы.

```sql
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

В следующей ячейке прочитайте файл, подставьте имя таблицы и путь, затем выполните SQL.

```python
spark.sql(query("create_table.sql", table=table, location=location))
```

## 3. Прочитать данные за период

В файле `sql_spark/select_batch.sql` укажите поля своей исходной таблицы. В примере выбираем данные за октябрь, до 1 ноября исключительно.

```sql
SELECT
    CAST(source_id AS STRING)     AS key_1,
    CAST(source_date AS DATE)     AS key_2,
    CAST(source_amount AS DOUBLE) AS amount
FROM {source}
WHERE source_date >= DATE '{date_from}'
  AND source_date <  DATE '{date_to}'
```

```python
frame = spark.sql(query(
    "select_batch.sql",
    source=source, date_from="2026-10-01", date_to="2026-11-01"
))
frame.show(5)
```

## 4. Дозагрузить данные через insert_missing

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

Если выгрузка уже находится в pandas DataFrame, передайте его в тот же метод. Например, для результата модели:

```python
db.insert_missing(fact_table, operator_topic_fact, keys=fact_keys)
```

Загрузки с пересекающимися ключами в одну таблицу запускайте последовательно.

