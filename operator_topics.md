# Темы для проверки и обучения операторов

SQL выполняется один раз для создания таблицы. Далее выполняй Python-ячейки сверху вниз.

### SQL запрос

Сначала создали срез данных c информацией о дате приема и увольнения: 
```sql
CREATE TABLE iceberg__write_access.sandbox_cc.operator_theme_ml_model_aht AS
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
SELECT e.*, p.aht_target
FROM episodes e
LEFT JOIN plans p
    ON e.is_chat AND e.bl_type = 'Чат ФЛ'
    AND e.login = p.login AND e.med_name = p.med_name
    AND e.start_dt = p.start_dt AND CAST(e.seg_start AS DATE) = p.plan_date;
```

### Загрузка данных в DataFrame

Загружаем эпизоды за выбранный год в df. 

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
FROM iceberg__write_access.sandbox_cc.operator_theme_ml_model_aht
WHERE seg_start >= TIMESTAMP '{year}-01-01 00:00:00'
  AND seg_start < TIMESTAMP '{year + 1}-01-01 00:00:00'
"""

df = lake.read_dataframe(sql_25)
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
    "bl_type", "med_name", "topic", "login", "test_episodes", "peer_episodes",
    "peer_operators", "test_operator_median", "test_peer_median",
])
validation["test_difference_pct"] = (
    validation["test_operator_median"] / validation["test_peer_median"] - 1
) * 100

checked = rating[
    ["bl_type", "login", "topic", "episodes", "within_score", "score"]
].rename(columns={
    "episodes": "train_episodes",
    "score": "train_score",
    "within_score": "train_within_score",
}).merge(
    validation,
    on=["bl_type", "login", "topic"],
    how="left",
    validate="one_to_many",
)

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

display(
    final[
        ["login", "bl_type", "med_name", "topic", "test_episodes",
         "train_within_score", "train_score", "test_difference_pct", "status"]
    ].rename(columns={
        "login": "Сотрудник",
        "bl_type": "Канал",
        "med_name": "Технический канал",
        "topic": "Тема для проверки и обучения",
        "test_episodes": "Однотемных обращений на проверке",
        "train_within_score": "Score внутри сотрудника",
        "train_score": "Score относительно коллег",
        "test_difference_pct": "Отставание от коллег на test, %",
        "status": "Статус проверки",
    }).round({
        "Score внутри сотрудника": 3,
        "Score относительно коллег": 3,
        "Отставание от коллег на test, %": 1,
    })
)
```


Для кадрового фильтра принимаем, что дата увольнения исключается. Сотрудники без известной даты приёма требуют сверки с кадровым справочником. Статус в итоговой таблице относится к однотемным эпизодам в указанном техническом канале.

Что осталось проверить: 

```bash
- Учесть иерархию тем: общую ветку и конкретную подтему.
- Доработать score: сделать понятное сравнение оператора с коллегами на той же теме.
- Проверить устойчивость рейтинга: на следующем месяце и при разных настройках регуляризации.
- Проверить, меняется ли med_name при смене level1 внутри сессии.
- Подобрать период обучения и минимальное число эпизодов для надёжных выводов.
```
