### SQL запрос

Сначала создали срез данных c информацией о дате приема и увольнения: 
```sql
CREATE TABLE iceberg__write_access.sandbox_cc.operator_theme_ml_model_v2 AS
WITH episodes AS (

SELECT
    CASE
        WHEN d1.naumen_login IS NOT NULL THEN d1.employment_date
        ELSE d2.employment_date
    END AS employment_date,

    CASE
        WHEN d1.naumen_login IS NOT NULL THEN d1.fired_date
        ELSE d2.fired_date
    END AS fired_date,

    CASE WHEN COUNT(t.start_dt) = COUNT(*) AND MIN(t.start_dt) = MAX(t.start_dt)
         THEN MIN(t.start_dt) END AS start_dt,
    COUNT_IF(t.channel = 'Chat') = COUNT(*) AS is_chat,
    t.session_id,
    t.segment,
    MAX(t.client_id) AS client_id,
    t.login,
    t.portal_id,
    t.city,
    t.fio,
    t.su,
    t.seg_start,
    t.seg_end,
    t.med_name,
    t.bl_type,
    t.handle_duration,

    ARRAY_AGG(
        CAST(
            ROW(t.theme, t.level1, t.level2, t.level3)
            AS ROW(
                theme VARCHAR,
                level1 VARCHAR,
                level2 VARCHAR,
                level3 VARCHAR
            )
        )
        ORDER BY t.created_date NULLS LAST
    ) AS theme_sequence,

    COUNT(*) - COUNT(t.created_date) AS missing_theme_dates,

    COUNT(t.created_date) - COUNT(DISTINCT t.created_date)
        AS duplicated_theme_dates

FROM iceberg__write_access.sandbox_cc.themesflow_new t

LEFT JOIN iceberg__write_access.sandbox_cc.dict_emp_raw_new d1
    ON LOWER(TRIM(d1.naumen_login)) = LOWER(TRIM(t.login))

LEFT JOIN iceberg__write_access.sandbox_cc.dict_emp_raw_new d2
    ON d1.naumen_login IS NULL
    AND d2.portal_id = t.portal_id

WHERE t.taken_flg = 1
    AND t.bl_type IN ('Голос ФЛ', 'Чат ФЛ')

GROUP BY
    CASE
        WHEN d1.naumen_login IS NOT NULL THEN d1.employment_date
        ELSE d2.employment_date
    END,
    CASE
        WHEN d1.naumen_login IS NOT NULL THEN d1.fired_date
        ELSE d2.fired_date
    END,
    t.session_id,
    t.segment,
    t.login,
    t.portal_id,
    t.city,
    t.fio,
    t.su,
    t.seg_start,
    t.seg_end,
    t.med_name,
    t.bl_type,
    t.handle_duration
),
lookup_keys AS (
    SELECT DISTINCT login, med_name, start_dt, CAST(seg_start AS DATE) AS plan_date
    FROM episodes
    WHERE is_chat AND bl_type = 'Чат ФЛ' AND start_dt IS NOT NULL
),
plans AS (
    SELECT k.login, k.med_name, k.start_dt, k.plan_date,
           CASE WHEN COUNT(p.aht_target) = COUNT(*)
                     AND MIN(CAST(p.aht_target AS DOUBLE)) = MAX(CAST(p.aht_target AS DOUBLE))
                     AND MIN(CAST(p.aht_target AS DOUBLE)) > 0
                THEN MIN(CAST(p.aht_target AS DOUBLE)) END AS aht_target
    FROM lookup_keys k
    LEFT JOIN iceberg__write_access.sandbox_cc.dict_chat_sessions dcs
        ON LOWER(k.login) = LOWER(dcs.naumen_login)
        AND CAST(k.start_dt AS TIMESTAMP(6)) BETWEEN dcs.startdate AND dcs.enddate
    LEFT JOIN iceberg__write_access.sandbox_cc.dict_emp_experience_new s
        ON LOWER(k.login) = LOWER(s.naumen_login)
        AND k.start_dt = s.dt
        AND k.med_name = s.project_name
    LEFT JOIN iceberg__write_access.sandbox_cc.dict_target_aht p
        ON k.med_name = p.project_name
        AND k.plan_date BETWEEN CAST(p.valid_from AS DATE) AND CAST(p.valid_to AS DATE)
        AND s.experience_months = p.experience
        AND COALESCE(CAST(dcs.field_value_2 AS INTEGER), 3) = p.sessions_cnt
    GROUP BY k.login, k.med_name, k.start_dt, k.plan_date
)
 , transfers AS (
    SELECT session_id, segment, MIN(flag) AS flag,
           MIN(flag) = MAX(flag) AS consistent
    FROM (
        SELECT session_id, segment,
               CASE WHEN transfer_med_name = 'CC_INBOUND_2LINE' THEN 0
                    ELSE COALESCE(transfer_flg, 0) END AS flag
        FROM iceberg__write_access.sandbox_cc.callflow_transfers_new
    ) b
    GROUP BY session_id, segment
),
voice_keys AS (
    SELECT DISTINCT e.login, e.med_name, e.start_dt, e.seg_start,
           COALESCE(b.flag, 0) AS flag
    FROM episodes e
    LEFT JOIN transfers b ON e.session_id = b.session_id AND e.segment = b.segment
    WHERE e.bl_type = 'Голос ФЛ' AND COALESCE(b.consistent, TRUE)
),
voice_plans AS (
    SELECT k.login, k.med_name, k.start_dt, k.seg_start, k.flag,
           CASE WHEN COUNT(p.aht_target) = COUNT(*)
                  AND MIN(CAST(p.aht_target AS DOUBLE)) = MAX(CAST(p.aht_target AS DOUBLE))
                  AND MIN(CAST(p.aht_target AS DOUBLE)) > 0
                THEN MIN(CAST(p.aht_target AS DOUBLE)) END AS aht_target
    FROM voice_keys k
    LEFT JOIN iceberg__write_access.sandbox_cc.dict_emp_experience_new s
        ON LOWER(k.login) = LOWER(s.naumen_login)
       AND k.start_dt = s.dt AND k.med_name = s.project_name
    LEFT JOIN iceberg__write_access.sandbox_cc.dict_target_aht p
        ON k.med_name = p.project_name
       AND k.seg_start BETWEEN p.valid_from AND p.valid_to
       AND k.flag = COALESCE(p.transfer_flg, 0)
       AND s.experience_months = p.experience
    GROUP BY k.login, k.med_name, k.start_dt, k.seg_start, k.flag
)
SELECT e.*, CASE WHEN e.bl_type = 'Голос ФЛ' THEN v.aht_target
                 ELSE p.aht_target END AS aht_target
FROM episodes e
LEFT JOIN plans p
    ON e.is_chat AND e.bl_type = 'Чат ФЛ'
    AND e.login = p.login AND e.med_name = p.med_name
    AND e.start_dt = p.start_dt AND CAST(e.seg_start AS DATE) = p.plan_date
LEFT JOIN transfers b
    ON e.bl_type = 'Голос ФЛ' AND e.session_id = b.session_id AND e.segment = b.segment
LEFT JOIN voice_plans v
    ON e.bl_type = 'Голос ФЛ' AND COALESCE(b.consistent, TRUE)
   AND e.login = v.login AND e.start_dt = v.start_dt
   AND e.med_name = v.med_name AND e.seg_start = v.seg_start
   AND COALESCE(b.flag, 0) = v.flag;
```

### Загрузка данных в DataFrame

Данный скрипт выполняет подключение к Trino и загрузки dataframe: 

```python
from dataclasses import dataclass, field
import os
from dotenv import dotenv_values
from contextlib import closing
import pandas as pd
from trino.auth import BasicAuthentication
from trino.dbapi import connect


@dataclass(frozen=True)
class TrinoConfig:
    host: str
    user: str
    password: str = field(repr=False)
    verify: str
    port: int = 443
    catalog: str = "iceberg__write_access"
    schema: str = "sandbox_cc"
    timezone: str = "Europe/Moscow"
    timeout: float = 60
  
    @classmethod
    def from_env(cls, env_file=".env"):
        values = {**dotenv_values(env_file), **os.environ}

        return cls(
            host=values["TRINO_HOST"],
            user=values["TRINO_USER"],
            password=values["TRINO_PASSWORD"],
            port=int(values.get("TRINO_PORT", 443)),
            catalog=values.get("TRINO_CATALOG", "iceberg__write_access"),
            schema=values.get("TRINO_SCHEMA", "sandbox_cc"),
            verify=values["TRINO_CA_BUNDLE"],
            timeout=float(values.get("TRINO_TIMEOUT", 60)),
        )


class DatalakeClient:
    def __init__(self, config: TrinoConfig):
        self.config = config
  

    def connect(self):
        cfg = self.config
  

        return connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            http_scheme="https",
            auth=BasicAuthentication(cfg.user, cfg.password),
            verify=cfg.verify,
            catalog=cfg.catalog,
            schema=cfg.schema,
            timezone=cfg.timezone,
            request_timeout=cfg.timeout,
            extra_credential=[
                ("gp-user", cfg.user),
                ("gp-password", cfg.password),
            ],
        )
  

    def read_dataframe(self, sql, params=None, batch_size=50_000):
        """Прочитать данные порциями и собрать один DataFrame."""
        with closing(self.connect()) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(sql, params)
  
                columns = [column[0] for column in cursor.description]
                parts = []
  
                while rows := cursor.fetchmany(batch_size):
                    parts.append(
                        pd.DataFrame.from_records(rows, columns=columns)
                    )
  

        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=columns)

config = TrinoConfig.from_env()
lake = DatalakeClient(config)

year = 2025
sql_25 = f"""
SELECT *
FROM iceberg__write_access.sandbox_cc.operator_theme_ml_model_v2
WHERE seg_start >= TIMESTAMP '{year}-01-01 00:00:00'
  AND seg_start < TIMESTAMP '{year + 1}-01-01 00:00:00'
"""

df = lake.read_dataframe(sql_25)
#df = pd.concat([df_2025, df_2026], ignore_index=True)
#del df_2025, df_2026
```

### Первичная обработка данных

Проверяем размер датафрейма `df` (количество строк и столбцов), типы данных и количество пропущенных значений по каждому столбцу.

```python
print("Строк:", len(df))
print("Столбцов:", len(df.columns))

display(
    pd.DataFrame({
        "dtype": df.dtypes.astype(str),
        "missing_count": df.isna().sum(),
        "missing_pct": df.isna().mean().mul(100).round(2),
    })
)
```

Приводим даты и длительность обработки к правильным типам данных, затем сравниваем голосовые и чат-обращения по количеству эпизодов, операторов, периоду наблюдения и длительности обработки + считаем стаж работника:

```python
for column in ["seg_start", "seg_end", "employment_date", "fired_date"]:
    df[column] = pd.to_datetime(df[column], errors="coerce")

df["handle_duration"] = pd.to_numeric(
    df["handle_duration"],
    errors="coerce",
)

df["aht_target"] = pd.to_numeric(df["aht_target"], errors="coerce")

has_plan = df["aht_target"].gt(0)
coverage = df.assign(has_plan=has_plan).groupby("bl_type").agg(
    episodes=("has_plan", "size"),
    episodes_with_plan=("has_plan", "sum"),
    coverage_pct=("has_plan", "mean"),
)
coverage["coverage_pct"] = (100 * coverage["coverage_pct"]).round(1)
display(coverage)

df["tenure_days"] = (
    df["seg_start"].dt.normalize()
    - df["employment_date"].dt.normalize()
).dt.days

display(
    df.groupby("bl_type").agg(
        episodes=("session_id", "size"),
        operators=("login", "nunique"),
        date_from=("seg_start", "min"),
        date_to=("seg_start", "max"),
        median_duration=("handle_duration", "median"),
        mean_duration=("handle_duration", "mean"),
    )
)

display(
    df[[
        "login",
        "seg_start",
        "employment_date",
        "fired_date",
        "tenure_days",
    ]].head(10)
)
```

Проверяем качество данных: ищем дубли эпизодов, пропущенные значения в ключевых полях, пустые логины операторов и некорректную длительность обработки:

```python
episode_key = [
    "bl_type",
    "session_id",
    "segment",
    "login",
    "seg_start",
]
  
duplicate_mask = df.duplicated(
    subset=episode_key,
    keep=False,
)
  
quality = pd.Series({
    "Всего строк": len(df),
    "Строк с повторяющимся ключом эпизода": duplicate_mask.sum(),
    "Строк с пропусками в ключе": (
        df[episode_key].isna().any(axis=1).sum()
    ),
    "Пустой login": (
        df["login"].fillna("").str.strip().eq("").sum()
    ),
    "Нет длительности": df["handle_duration"].isna().sum(),
    "Длительность <= 0": df["handle_duration"].le(0).sum(),
    "Конец раньше начала": (
        df["seg_end"] < df["seg_start"]
    ).sum(),
})
  
display(quality)
```

Очищаем последовательности тематик от пустых значений и подряд идущих повторов, затем считаем количество уникальных тем в каждом эпизоде + убираем тех. проблемные темы:

```python
bad_levels = {
("техсбой", "тишина", "оператор не слышит клиента"),
("техсбой", "тишина", "клиент не слышит оператора"),
}

mask = df["theme_sequence"].apply(
    lambda seq: any(
        tuple("".join(str(v).lower().split()) if i == 0 else str(v).strip().lower() for i, v in enumerate(topic[1:4])) in bad_levels
        for topic in seq
    )
)

df = df.loc[~mask].copy()

def clean_sequence(sequence):
    result = []

    for item in sequence:
        topic = tuple(
            value.strip() if value and value.strip() else None
            for value in item[1:4]
        )

        if all(value is None for value in topic):
            continue

        if not result or topic != result[-1]:
            result.append(topic)

    return result


df["topics"] = df["theme_sequence"].apply(clean_sequence)
df["n_topics"] = df["topics"].apply(lambda topics: len(set(topics)))

display(df[["bl_type", "login", "topics", "n_topics"]].head())
print("Строк без темы:", df["n_topics"].eq(0).sum())
```

Отбираем эпизоды, в которых есть хотя бы одна тема и положительная длительность обработки, затем считаем количество таких эпизодов по каналам обслуживания и количеству уникальных тем + собираем df_model:

```python
df_model = df[
    df["n_topics"].gt(0)
    & df["handle_duration"].gt(0)
].copy()

display(
    df_model.groupby(["bl_type", "n_topics"])
    .size()
    .rename("episodes")
    .reset_index()
)
```

### Модель Ridge

Начнем с проверки работоспособности Ridge модели: 

```python
import json
import numpy as np

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import Ridge


data = df_model.loc[
    df_model["n_topics"].isin([1, 2]),
    [
        "session_id", "login", "bl_type", "med_name",
        "seg_start", "tenure_days", "topics", "handle_duration", "aht_target",
    ],
].copy()

data["topics"] = data["topics"].apply(lambda x: list(dict.fromkeys(x)))

cutoff = data["seg_start"].max().to_period("M").start_time

train = data[data["seg_start"] < cutoff].copy()
test = data[data["seg_start"] >= cutoff].copy()

train = train[~train["session_id"].isin(test["session_id"])]

tenure_median = train["tenure_days"].where(
    train["tenure_days"].ge(0)
).median()

print("Обучение:", len(train))
print("Проверка:", len(test))

```

Заполняем пропуски в стаже: 

```python
for frame in [train, test]:
    frame["tenure_missing"] = (
        frame["tenure_days"].isna()
        | frame["tenure_days"].lt(0)
    )

    frame["tenure_days"] = frame["tenure_days"].mask(
        frame["tenure_missing"], tenure_median
    )
```

Переводим одну строку в признаки: 

```python
def make_features(frame):
    for row in frame.itertuples():
        features = {
            "bl_type": row.bl_type,
            "med_name": str(row.med_name),
            "operator": json.dumps([row.bl_type, row.login]),
            "tenure": np.log1p(row.tenure_days),
            "tenure_missing": int(row.tenure_missing),
            "n_topics": len(row.topics),
        }

        for topic in row.topics:
            topic_key = json.dumps([row.bl_type, topic])
            pair_key = json.dumps([row.bl_type, row.login, topic])

            features["topic_" + topic_key] = 1 / len(row.topics)
            features["pair_" + pair_key] = 1 / len(row.topics)

        yield features
```

Обучаем модель: 

```python
vectorizer = DictVectorizer(dtype=np.float32)

X_train = vectorizer.fit_transform(make_features(train))
X_test = vectorizer.transform(make_features(test))

model = Ridge(alpha=30, solver="lsqr")
model.fit(X_train, np.log(train["handle_duration"]))
```

Эффект "Оператор - тема": 

```python
rows = []

for name, effect in zip(
    vectorizer.get_feature_names_out(), model.coef_
):
    if name.startswith("pair_"):
        bl_type, login, topic = json.loads(name[5:])
        rows.append([bl_type, login, tuple(topic), effect])

result = pd.DataFrame(
    rows,
    columns=["bl_type", "login", "topic", "effect"],
)
```

Добавляем число эпизодов: 

```python
counts = (
    train.explode("topics")
    .groupby(["bl_type", "login", "topics"])
    .size()
    .reset_index(name="episodes")
    .rename(columns={"topics": "topic"})
)

result = result.merge(counts, on=["bl_type", "login", "topic"])
```

Темы с наибольшим отклонением внутри каждого сотрудника: 

```python
usual_effect = (
    result.groupby(["bl_type", "login"])["effect"]
    .transform("median")
)

result["within_score"] = result["effect"] - usual_effect


```

MAE и улучшение относительно baseline, сравниваем прогноз модели с реальным временем

```python
keys = ["bl_type", "med_name", "topic"]

train_single = train[train["topics"].str.len() == 1].copy()
train_single["topic"] = train_single["topics"].str[0]

topic_median = (
    train_single.groupby(keys, dropna=False)["handle_duration"]
    .median()
    .rename("topic_baseline")
    .reset_index()
)

channel_median = train.groupby("bl_type")["handle_duration"].median()

metrics = []

for name, frame, X in [("train", train, X_train), ("test", test, X_test)]:
    check = frame.copy()
    check["prediction"] = np.exp(model.predict(X))
    check["channel_baseline"] = check["bl_type"].map(channel_median)
    check["topic"] = check["topics"].str[0]
    check = check.merge(topic_median, on=keys, how="left", validate="many_to_one")
    check["topic_baseline"] = check["topic_baseline"].fillna(
        check["channel_baseline"]
    )

    for scope in ["все", "одна тема"]:
        sample = check if scope == "все" else check[check["topics"].str.len() == 1]

        for bl_type, part in sample.groupby("bl_type"):
            y = part["handle_duration"]
            model_mae = (y - part["prediction"]).abs().mean()
            baseline = "channel_baseline" if scope == "все" else "topic_baseline"
            baseline_mae = (y - part[baseline]).abs().mean()

            metrics.append({
                "sample": name,
                "scope": scope,
                "bl_type": bl_type,
                "episodes": len(part),
                "model_mae": model_mae,
                "baseline_mae": baseline_mae,
                "improvement_pct": 100 * (1 - model_mae / baseline_mae)
                if baseline_mae > 0 else np.nan,
            })

display(pd.DataFrame(metrics).round(1))
```

Достаем coef сравнения с другими сотрудниками: 

```python
coef = dict(zip(vectorizer.get_feature_names_out(), model.coef_))

rating = result[result["episodes"] >= 30].copy()

rating["total_effect"] = rating["effect"] + [
    coef["operator=" + json.dumps([bl_type, login])]
    for bl_type, login in zip(rating["bl_type"], rating["login"])
]

groups = rating.groupby(["bl_type", "topic"])["total_effect"]

rating["score"] = rating["total_effect"] - groups.transform("median")
rating = rating.loc[groups.transform("size").ge(3)].copy()
rating["difference_pct"] = (np.expm1(rating["score"]) * 100).round(1)


```

Сохранилось ли отставание выбранных сотрудников на следующем месяце? Список сотрудников и тем для проверки: 

```python
keys = ["bl_type", "med_name", "topic"]
rating_keys = ["bl_type", "login", "topic"]
count_keys = ["bl_type", "login", "med_name", "topic"]

test_counts = (
    test[["bl_type", "login", "med_name", "topics"]]
    .explode("topics")
    .rename(columns={"topics": "topic"})
    .groupby(count_keys, dropna=False)
    .size()
    .reset_index(name="test_episodes_all")
)

test_single = test[test["topics"].str.len() == 1].copy()
test_single["topic"] = test_single["topics"].str[0]

rows = []

for (bl_type, med_name, topic), part in test_single.groupby(keys, dropna=False):
    for login, operator in part.groupby("login"):
        peers = part[part["login"] != login]

        rows.append({
            "bl_type": bl_type,
            "med_name": med_name,
            "topic": topic,
            "login": login,
            "test_episodes": len(operator),
            "peer_episodes": len(peers),
            "peer_operators": peers["login"].nunique(),
            "test_operator_median": operator["handle_duration"].median(),
            "test_peer_median": peers["handle_duration"].median(),
        })

validation = pd.DataFrame(rows, columns=[
    *count_keys, "test_episodes", "peer_episodes", "peer_operators",
    "test_operator_median", "test_peer_median",
])

validation["test_difference_pct"] = (
    validation["test_operator_median"] / validation["test_peer_median"] - 1
) * 100

test_summary = test_counts.merge(
    validation, on=count_keys, how="left", validate="one_to_one"
)

checked = (
    rating[rating_keys + ["episodes", "within_score", "score"]]
    .rename(columns={
        "episodes": "train_episodes",
        "within_score": "train_within_score",
        "score": "train_score",
    })
    .merge(test_summary, on=rating_keys, how="left", validate="one_to_many")
)

for column in ["test_episodes_all", "test_episodes"]:
    checked[column] = checked[column].fillna(0).astype(int)

enough = (
    checked["test_episodes"].ge(20)
    & checked["peer_episodes"].ge(30)
    & checked["peer_operators"].ge(3)
)

confirmed = enough & checked["test_difference_pct"].ge(20)

checked["status"] = np.where(
    ~enough,
    "Недостаточно данных",
    np.where(confirmed, "Отставание сохраняется", "Отставание не подтверждено"),
)

as_of = test["seg_start"].max().normalize()
active_logins = df.loc[
    df["employment_date"].le(as_of)
    & (df["fired_date"].isna() | df["fired_date"].gt(as_of)),
    "login",
]

final = checked[
    confirmed
    & checked["login"].isin(active_logins)
    & checked["train_episodes"].ge(30)
    & checked["train_score"].gt(0)
    & checked["train_within_score"].gt(0)
].sort_values("train_score", ascending=False)

print("Строк в итоговой таблице:", len(final))
print("Уникальных сотрудников:", final["login"].nunique())

columns = {
    "login": "Сотрудник",
    "bl_type": "Канал",
    "med_name": "Технический канал",
    "topic": "Тема для проверки и обучения",
    "test_episodes_all": "Всего обращений с этой темой на test",
    "test_episodes": "Из них однотемных",
    "train_within_score": "Score внутри сотрудника",
    "train_score": "Score относительно коллег",
    "test_difference_pct": "Отставание от коллег на test, %",
    "status": "Статус проверки",
}

display(
    final[list(columns)]
    .round({
        "train_within_score": 3,
        "train_score": 3,
        "test_difference_pct": 1,
    })
    .rename(columns=columns)
)
```


Что осталось проверить: 

```bash
- Учесть иерархию тем: общую ветку и конкретную подтему.
- Подобрать период обучения и минимальное число эпизодов для надёжных выводов.
- Если у одного коллеги 300 эпизодов, а у двух других по 20, медиана будет почти целиком медианой этого одного коллеги. Условие `peer_operators >= 3` проверяет, что коллег трое, но не что их вклад сравним. Устойчивее сравнивать с медианой из медиан по операторам:
- `test_difference_pct` считается без наблюдаемого сотрудника (`peers = part[part["login"] != login]`). Медиана на train, из которой получается `score` («Score относительно коллег»), считается вместе с сотрудником: `groups.transform("median")` берёт всех операторов на теме, включая его самого - надо бы в один стиль сделать
- GridSearch бы сделать для альфа или MixedML
```


## Аналитический отчёт по выполнению AHT за период test

Выполни следующие ячейки после модели. В SQL плановая длительность называется `aht_target` (бизнес-название — `aht_duration`). Выполнение плана за период = 100 × сумма плановых секунд / сумма фактических секунд по одним и тем же эпизодам. Ровно 100% и выше — план выполнен. Превышение в секундах = факт минус план. Положительное значение показывает, сколько секунд сверх норматива накопилось за период.

### 1. Период и покрытие нормативами

```python
import numpy as np
import pandas as pd

aht = test.loc[test["topics"].str.len().isin([1, 2]),
               ["seg_start", "bl_type", "med_name", "login", "topics",
                "handle_duration", "aht_target"]].copy()
if aht.empty:
    raise ValueError("В test нет эпизодов с одной или двумя темами.")

print("Даты эпизодов:", aht["seg_start"].min().date(), "—", aht["seg_start"].max().date())
for col in ["aht_target", "handle_duration"]:
    aht[col] = pd.to_numeric(aht[col], errors="coerce")
aht["has_plan"] = np.isfinite(aht["aht_target"]) & aht["aht_target"].gt(0)
aht["included"] = aht["has_plan"] & np.isfinite(aht["handle_duration"]) & aht["handle_duration"].gt(0)
coverage = aht.groupby("bl_type", dropna=False).agg(
    episodes=("included", "size"), with_plan=("has_plan", "sum"),
    in_report=("included", "sum"),
)
coverage["coverage_pct"] = 100 * coverage["with_plan"] / coverage["episodes"]
display(coverage.round(1))
aht = aht.loc[aht["included"]].copy()
```

### 2. Группировка тем и расчёт показателей

Пары А + Б и Б + А образуют одну группу. Вся длительность эпизода относится к паре один раз. Общий результат сотрудника охватывает эпизоды с одной или двумя темами в его проекте и канале за период test.

```python
def topic_key(topic):
    return tuple((v is not None, v or "") for v in topic)

aht["topic_group"] = aht["topics"].map(lambda x: tuple(sorted(x, key=topic_key)))

def summarize_aht(keys):
    result = aht.groupby(keys, dropna=False, sort=False).agg(
        episodes=("handle_duration", "size"),
        fact_sec=("handle_duration", "sum"),
        plan_sec=("aht_target", "sum"),
    ).reset_index()
    result["fact_avg_sec"] = result["fact_sec"] / result["episodes"]
    result["plan_avg_sec"] = result["plan_sec"] / result["episodes"]
    result["plan_pct"] = 100 * result["plan_sec"] / result["fact_sec"]
    result["gap_sec"] = result["fact_sec"] - result["plan_sec"]
    result["plan_met"] = result["fact_sec"].le(result["plan_sec"])
    if "topic_group" in keys:
        result["topic"] = result["topic_group"].map(
            lambda group: " + ".join(" / ".join(v or "—" for v in topic) for topic in group)
        )
        result["n_topics"] = result["topic_group"].str.len()
    return result.sort_values("gap_sec", ascending=False).reset_index(drop=True)
```

### 3. Кто и по каким темам не выполнил план

`plan_pct` — выполнение по теме или паре. `operator_plan_pct` — выполнение по всем включённым темам этого сотрудника в том же канале и проекте. Статус определяется до округления.

```python
operator_keys = ["bl_type", "med_name", "login"]
operator_aht = summarize_aht(operator_keys)
operator_topic_aht = summarize_aht(operator_keys + ["topic_group"]).merge(
    operator_aht[operator_keys + ["plan_pct"]].rename(columns={"plan_pct": "operator_plan_pct"}),
    on=operator_keys, how="left", validate="many_to_one",
)
over_plan = operator_topic_aht.loc[~operator_topic_aht["plan_met"]].copy()
min_episodes = 20
over_plan_main = over_plan.loc[over_plan["episodes"].ge(min_episodes)]
cols = ["login", "bl_type", "med_name", "topic", "n_topics", "episodes",
        "fact_avg_sec", "plan_avg_sec", "plan_pct", "gap_sec", "operator_plan_pct"]
print("Групп с невыполненным планом:", len(over_plan))
print("Из них с достаточным числом эпизодов:", len(over_plan_main))
display(over_plan_main[cols].head(50).round(2))
```

### 4. Сводки по каналу, проекту и темам

В сводках участвуют все включённые эпизоды. Порог 20 применяется только к списку для разбора выше.

```python
channel_aht = summarize_aht(["bl_type"])
project_aht = summarize_aht(["bl_type", "med_name"])
topic_keys = ["bl_type", "med_name", "topic_group"]
topic_aht = summarize_aht(topic_keys)
counts = (
    operator_topic_aht.assign(
        missed_login=operator_topic_aht["login"].where(~operator_topic_aht["plan_met"])
    ).groupby(topic_keys, dropna=False, sort=False)
    .agg(operators=("login", "nunique"), operators_below_plan=("missed_login", "nunique"))
    .reset_index()
)
topic_aht = topic_aht.merge(counts, on=topic_keys, validate="one_to_one")
assert topic_aht["episodes"].sum() == len(aht)
for col in ["fact_sec", "plan_sec"]:
    assert np.isclose(topic_aht[col].sum(), channel_aht[col].sum())

print("Каналы")
display(channel_aht.round(2))
print("Проекты")
display(project_aht.round(2))
print("Темы с наибольшим превышением в секундах")
display(topic_aht.loc[~topic_aht["plan_met"]].drop(columns="topic_group").head(30).round(2))
```
