### SQL запрос

Сначала создали срез данных c информацией о дате приема и увольнения: 
```sql
CREATE TABLE iceberg__write_access.sandbox_cc.operator_theme_ml_model_v3
WITH (
    location = ''
)
AS
WITH
-- 1. Эпизоды для модели.

episodes AS (
    SELECT
        a.session_id,
        a.segment,
        a.login,
        a.portal_id,
        a.city,
        a.fio,
        a.su,
        a.med_name,
        a.bl_type,
        a.seg_start,
        a.seg_end,
        a.handle_duration,
        MAX(a.client_id) AS client_id,

        CASE
            WHEN COUNT(a.start_dt) = COUNT(*)
             AND MIN(a.start_dt) = MAX(a.start_dt)
            THEN MIN(a.start_dt)
        END AS start_dt,

        COUNT_IF(a.channel = 'Chat') = COUNT(*) AS is_chat,

        ARRAY_AGG(
            CAST(
                ROW(a.theme, a.level1, a.level2, a.level3)
                AS ROW(
                    theme  VARCHAR,
                    level1 VARCHAR,
                    level2 VARCHAR,
                    level3 VARCHAR
                )
            )
            ORDER BY a.created_date NULLS LAST
        ) AS theme_sequence

    FROM iceberg__write_access.sandbox_cc.themesflow_new a
    WHERE a.taken_flg = 1
      AND a.bl_type IN ('Чат ФЛ', 'Голос ФЛ')
    GROUP BY
        a.session_id, a.segment, a.login, a.portal_id,
        a.city, a.fio, a.su, a.med_name, a.bl_type,
        a.seg_start, a.seg_end, a.handle_duration
),

-- 2. Ключи чата

chat_keys AS (
    SELECT DISTINCT
        login,
        med_name,
        start_dt,
        seg_start
    FROM episodes
    WHERE bl_type = 'Чат ФЛ'
      AND is_chat
      AND start_dt IS NOT NULL
),

-- 3. Норматив чата

chat_matches AS (
    SELECT
        a.login,
        a.med_name,
        a.start_dt,
        a.seg_start,
        CAST(p.aht_target AS DOUBLE) AS target_sec

    FROM chat_keys a

    LEFT JOIN iceberg__write_access.sandbox_cc.dict_chat_sessions c
        ON LOWER(a.login) = LOWER(c.naumen_login)
       AND CAST(a.start_dt AS TIMESTAMP(6))
           BETWEEN c.startdate AND c.enddate

    LEFT JOIN iceberg__write_access.sandbox_cc.dict_emp_experience_new s
        ON LOWER(a.login) = LOWER(s.naumen_login)
       AND a.start_dt = s.dt
       AND a.med_name = s.project_name

    LEFT JOIN iceberg__write_access.sandbox_cc.dict_target_aht p
        ON a.med_name = p.project_name
       AND a.seg_start BETWEEN CAST(p.valid_from AS DATE)
                           AND CAST(p.valid_to AS DATE)
       AND s.experience_months = p.experience
       AND COALESCE(CAST(c.field_value_2 AS INTEGER), 3) = p.sessions_cnt
),

-- 4. Один AHT на ключ чата

chat_plans AS (
    SELECT
        login,
        med_name,
        start_dt,
        seg_start,
        CASE
            WHEN COUNT(target_sec) = COUNT(*)
             AND MIN(target_sec) = MAX(target_sec)
             AND MIN(target_sec) > 0
            THEN MIN(target_sec)
        END AS aht_target
    FROM chat_matches
    GROUP BY login, med_name, start_dt, seg_start
),

-- 5. Норматив голоса

voice_matches AS (
    SELECT
        a.session_id,
        a.segment,
        LOWER(a.login) AS login_key,
        a.med_name,
        COALESCE(
            CASE
                WHEN b.transfer_med_name = 'CC_INBOUND_2LINE' THEN 0
                ELSE b.transfer_flg
            END,
            0
        ) AS transfer_flag,
        a.start_dt,
        a.seg_start_raw
    FROM iceberg__write_access.sandbox_cc.callflow_new a

    LEFT JOIN iceberg__write_access.sandbox_cc.callflow_transfers_new b
        ON a.session_id = b.session_id
       AND a.segment = b.segment

    WHERE a.taken_flg = 1
),

-- 6. Один AHT на голосовой сегмент оператора

voice_plans AS (
    SELECT
        a.session_id,
        a.segment,
        a.login_key,
        a.med_name,
        CASE
            WHEN MIN(a.transfer_flag) = MAX(a.transfer_flag)
             AND COUNT(p.aht_target) = COUNT(*)
             AND MIN(CAST(p.aht_target AS DOUBLE)) = MAX(CAST(p.aht_target AS DOUBLE))
             AND MIN(CAST(p.aht_target AS DOUBLE)) > 0
            THEN MIN(CAST(p.aht_target AS DOUBLE))
        END AS aht_target

    FROM voice_matches a

    LEFT JOIN iceberg__write_access.sandbox_cc.dict_emp_experience_new s
        ON a.login_key = LOWER(s.naumen_login)
       AND a.start_dt = s.dt
       AND a.med_name = s.project_name

    LEFT JOIN iceberg__write_access.sandbox_cc.dict_target_aht p
        ON a.med_name = p.project_name
       AND a.seg_start_raw BETWEEN p.valid_from AND p.valid_to
       AND s.experience_months = p.experience
       AND a.transfer_flag = COALESCE(p.transfer_flg, 0)

    GROUP BY a.session_id, a.segment, a.login_key, a.med_name
)

-- Итог: эпизод с темами, фактической длительностью, кадровыми датами
SELECT
    e.session_id,
    e.segment,
    e.client_id,
    e.login,
    e.portal_id,
    e.city,
    e.fio,
    e.su,
    e.med_name,
    e.bl_type,
    e.start_dt,
    e.seg_start,
    e.seg_end,
    e.handle_duration,
    e.theme_sequence,

    CASE
        WHEN d1.naumen_login IS NOT NULL THEN d1.employment_date
        ELSE d2.employment_date
    END AS employment_date,

    CASE
        WHEN d1.naumen_login IS NOT NULL THEN d1.fired_date
        ELSE d2.fired_date
    END AS fired_date,

    CASE
        WHEN e.bl_type = 'Чат ФЛ' THEN c.aht_target
        WHEN e.bl_type = 'Голос ФЛ' THEN v.aht_target
    END AS aht_target

FROM episodes e

LEFT JOIN chat_plans c
    ON e.bl_type = 'Чат ФЛ'
   AND e.is_chat
   AND e.login = c.login
   AND e.med_name = c.med_name
   AND e.start_dt = c.start_dt
   AND e.seg_start = c.seg_start

LEFT JOIN voice_plans v
    ON e.bl_type = 'Голос ФЛ'
   AND e.session_id = v.session_id
   AND e.segment = v.segment
   AND LOWER(e.login) = v.login_key
   AND e.med_name = v.med_name

LEFT JOIN iceberg__write_access.sandbox_cc.dict_emp_raw_new d1
    ON LOWER(TRIM(e.login)) = LOWER(TRIM(d1.naumen_login))

LEFT JOIN iceberg__write_access.sandbox_cc.dict_emp_raw_new d2
    ON d1.naumen_login IS NULL
   AND e.portal_id = d2.portal_id;
   
   

---- Проверка ----

SELECT
    bl_type,
    COUNT(*) AS episodes,
    COUNT(*) - COUNT(DISTINCT ROW(session_id, segment, login, seg_start))
        AS extra_rows,
    COUNT(aht_target) AS episodes_with_plan,
    ROUND(100.0 * COUNT(aht_target) / COUNT(*), 1) AS coverage_pct
FROM iceberg__write_access.sandbox_cc.operator_theme_ml_model_v3
GROUP BY bl_type
ORDER BY bl_type;
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
bad_words = ("техсбой", "переводклиента")
session_key = ["session_id", "login"]


def has_bad_topic(sequence):
    for item in sequence:
        for level in item[1:4]:
            if pd.isna(level):
                continue

            text = "".join(str(level).casefold().split())
            if any(word in text for word in bad_words):
                return True

    return False


bad_rows = df["theme_sequence"].map(has_bad_topic)
bad_pairs = df.loc[bad_rows, session_key].drop_duplicates()

all_keys = pd.MultiIndex.from_frame(df[session_key])
bad_keys = pd.MultiIndex.from_frame(bad_pairs)
remove = all_keys.isin(bad_keys)

print("Пар сессия + оператор к удалению:", len(bad_pairs))
print("Строк с запрещённой темой:", int(bad_rows.sum()))
print("Всего удаляемых строк:", int(remove.sum()))

df = df.loc[~remove].copy()


def clean_sequence(sequence):
    result = []

    for item in sequence:
        topic = tuple(
            (str(value).strip() or None) if pd.notna(value) else None
            for value in item[1:4]
        )

        if any(topic) and (not result or topic != result[-1]):
            result.append(topic)

    return result


df["topics"] = df["theme_sequence"].map(clean_sequence)
df["n_topics"] = df["topics"].map(lambda topics: len(set(topics)))

display(df[["session_id", "login", "bl_type", "topics", "n_topics"]].head())
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

### Модель Ridge и тематическая витрина

Начни с готового `df_model`. Выбери отчётный месяц в первой ячейке и выполняй ячейки по порядку. Результат — `operator_topic_fact`, одна строка на тему сегмента сотрудника.

#### 1. Период обучения и расчёта

Отчётный месяц идёт в test. Обучение использует предыдущие месяцы и исключает сессии из test.

```python
import json
import numpy as np
import pandas as pd

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import Ridge

report_month = None  # например, "2026-10"; None — последний месяц выгрузки
alpha_by_channel = {"Чат ФЛ": 30, "Голос ФЛ": 30}
episode_key = ["bl_type", "session_id", "segment", "login", "seg_start"]
eligible = (
    df_model["n_topics"].isin([1, 2])
    & np.isfinite(df_model["handle_duration"])
    & df_model["handle_duration"].gt(0)
)

last_date = df_model.loc[eligible, "seg_start"].max()
month = pd.Period(report_month or last_date, freq="M")
start = month.start_time
end = (month + 1).start_time
data = df_model.loc[eligible]
test = data.loc[data["seg_start"].between(start, end, inclusive="left")].copy()
train = data.loc[data["seg_start"] < start, [
    "session_id", "login", "med_name", "bl_type", "topics", "tenure_days", "handle_duration"
]].copy()
train = train.loc[~train["session_id"].isin(test["session_id"])]

if test.empty:
    raise ValueError("Нет сегментов за выбранный месяц.")
if test[episode_key].isna().any().any() or test.duplicated(episode_key).any():
    raise ValueError("Проверь ключ сегмента: обнаружены пропуски или повторы.")

for frame in (train, test):
    frame["topics"] = frame["topics"].map(lambda seq: list(dict.fromkeys(seq)))
print(month, "train:", len(train), "test:", len(test))
```

#### 2. Признаки сегмента

Каждая из двух уникальных тем получает вес ½ в признаках Ridge. В обоих каналах стаж считается от employment_date до даты сегмента. Для пропусков берём медиану стажа на train соответствующего канала.

```python
def pair_key(login, topic):
    return "pair_" + json.dumps([login, topic], ensure_ascii=False)

def make_features(frame, fill_days):
    for row in frame.itertuples(index=False):
        f = {
            "operator": str(row.login),
            "project": str(row.med_name),
            "n_topics": len(row.topics),
        }

        missing = pd.isna(row.tenure_days) or row.tenure_days < 0
        f["tenure"] = np.log1p(fill_days if missing else row.tenure_days)
        f["tenure_missing"] = int(missing)

        weight = 1 / len(row.topics)
        for topic in row.topics:
            f["topic_" + json.dumps(topic, ensure_ascii=False)] = weight
            f[pair_key(row.login, topic)] = weight
        yield f
```

#### 3. Обучение по каналам

Каждый канал получает собственную модель длительности. Стартовый alpha для обоих каналов равен 30.

```python
models = {}

for channel, history in train.groupby("bl_type"):
    days = history.loc[history["tenure_days"].ge(0), "tenure_days"]
    fill_days = days.median() if len(days) else 0

    vec = DictVectorizer(dtype=np.float32)
    X = vec.fit_transform(make_features(history, fill_days))

    model = Ridge(alpha=alpha_by_channel[channel], solver="lsqr")
    model.fit(X, np.log(history["handle_duration"]))
    models[channel] = (vec, model, fill_days)
    del X
```

#### MAE в секундах на train и test

Для всех эпизодов baseline — медиана канала на train. Для однотемных — медиана канала, проекта и темы на train с подстановкой медианы канала для новой темы. Положительный improvement_pct означает меньшую MAE у Ridge.

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

for name, frame in [("train", train), ("test", test)]:
    check = frame.copy()
    check["prediction"] = np.nan
    for channel, part in check.groupby("bl_type"):
        if channel in models:
            vec, model, fill_days = models[channel]
            X = vec.transform(make_features(part, fill_days))
            check.loc[part.index, "prediction"] = np.exp(model.predict(X))
    check = check.dropna(subset=["prediction"])
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

model_quality = pd.DataFrame(metrics)
display(model_quality.round(1))
```

#### 4. Скоры сотрудника

within_score сравнивает тему с другими темами сотрудника. peer_score_pct сравнивает его с остальными операторами на этой теме в том же канале.

```python
parts = []

for channel, history in train.groupby("bl_type"):
    vec, model, _ = models[channel]
    coef = dict(zip(vec.get_feature_names_out(), model.coef_))

    scores = history[["login", "topics"]].explode("topics").rename(columns={"topics": "topic"})
    scores = scores.groupby(["login", "topic"], sort=False).size().reset_index(name="train_operator_topic_episodes")
    scores["pair_effect"] = [coef[pair_key(login, topic)] for login, topic in zip(scores.login, scores.topic)]
    scores["total_effect"] = scores["pair_effect"] + scores.login.map(lambda login: coef["operator=" + str(login)])
    scores["within_score"] = scores["pair_effect"] - scores.groupby("login")["pair_effect"].transform("median")
    scores["peer_effect"] = np.nan
    scores["train_peer_operators"] = scores.groupby("topic", sort=False)["login"].transform("size") - 1
    for _, group in scores.groupby("topic", sort=False):
        effects = group["total_effect"].to_numpy()
        if len(effects) < 2:
            continue

        # Одинаковые эффекты дают одинаковую медиану остальных.
        # Достаточно трёх вариантов: ниже середины, в середине и выше.
        middle = np.median(effects)
        for mask in [effects < middle, effects == middle, effects > middle]:
            positions = np.flatnonzero(mask)
            if len(positions):
                others = np.delete(effects, positions[0])
                scores.loc[group.index[positions], "peer_effect"] = np.median(others)

    scores["peer_score"] = scores["total_effect"] - scores["peer_effect"]
    scores["peer_score_pct"] = 100 * np.expm1(scores["peer_score"])
    scores["bl_type"] = channel
    parts.append(scores)
```

#### 5. Одна тема в строке

Сохраняем поля сегмента и считаем, сколько однотемных примеров было в истории канала и сотрудника.

```python
single = train.loc[train["topics"].str.len().eq(1), ["bl_type", "login", "topics"]].copy()
single["topic"] = single.pop("topics").str[0]
fact = test.drop(columns=["theme_sequence", "is_chat", "missing_theme_dates",
                          "duplicated_theme_dates"], errors="ignore").copy()
fact["_episode"] = np.arange(len(fact))
fact = fact.explode("topics", ignore_index=True).rename(columns={"topics": "topic", "aht_target": "aht_duration"})
for keys, col in [
    (["bl_type", "topic"], "train_topic_single_episodes"),
    (["bl_type", "login", "topic"], "train_operator_single_episodes"),
]:
    counts = single.groupby(keys, sort=False).size().rename(col)
    fact = fact.join(counts, on=keys, validate="many_to_one")
    fact[col] = fact[col].fillna(0).astype(int)
```

#### 6. Прогнозы для двухтемных сегментов

Для каждой темы пары рассчитываем прогноз, как если бы тема была единственной.

```python
two = fact["n_topics"].eq(2)
fact["is_estimated"] = two.astype("int8")
fact["single_prediction_sec"] = np.nan
for channel, part in fact.loc[two].groupby("bl_type"):
    if channel in models:
        vec, model, fill_days = models[channel]
        inputs = part[["login", "med_name", "tenure_days"]].copy()
        inputs["topics"] = part["topic"].map(lambda topic: [topic])
        X = vec.transform(make_features(inputs, fill_days))
        fact.loc[part.index, "single_prediction_sec"] = np.exp(model.predict(X).astype(float))
```

#### 7. Распределение секунд

Делим фактическое время пропорционально двум прогнозам. Если одна тема не встречалась отдельно в истории канала или прогноз недоступен, обе получают по ½. Метод записывается в allocation_method.

```python
pred = fact["single_prediction_sec"]
supported = pred.gt(0) & np.isfinite(pred) & fact["train_topic_single_episodes"].gt(0)

use_model = two & supported.groupby(fact["_episode"]).transform("all")
total_pred = pred.groupby(fact["_episode"]).transform("sum")

fact["allocation_weight"] = np.where(two, 0.5, 1.0)
fact.loc[use_model, "allocation_weight"] = pred[use_model] / total_pred[use_model]
fact["allocation_method"] = "single_observed"
fact.loc[two, "allocation_method"] = "equal_split_fallback"
fact.loc[use_model, "allocation_method"] = "ridge_proportional"

fact["topic_duration"] = fact["handle_duration"] * fact["allocation_weight"]

plan = fact["aht_duration"].where(fact["aht_duration"].gt(0) & np.isfinite(fact["aht_duration"]))
fact["topic_plan_seconds"] = plan * fact["allocation_weight"]
fact["episode_plan_pct"] = 100 * plan / fact["handle_duration"]
```

#### Проверка отставания на однотемных сегментах test

Ориентир — медиана эпизодов остальных операторов на той же теме, в том же канале и проекте.

```python
keys = ["bl_type", "med_name", "topic"]
count_keys = keys + ["login"]
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

```

#### 8. Итоговый датасет

Добавляем скоры и уровни темы. Проверяем число строк и сумму секунд отдельно для каждого сегмента.

```python
keys = ["bl_type", "login", "topic"]
score_cols = ["train_operator_topic_episodes", "train_peer_operators", "pair_effect", "total_effect",
              "within_score", "peer_effect", "peer_score", "peer_score_pct"]
if parts:
    fact = fact.merge(pd.concat(parts, ignore_index=True)[keys + score_cols],
                      on=keys, how="left", validate="many_to_one")
else:
    for col in score_cols:
        fact[col] = np.nan
fact["train_operator_topic_episodes"] = fact["train_operator_topic_episodes"].fillna(0).astype(int)
fact = fact.merge(validation, on=count_keys, how="left", validate="many_to_one")
fact["test_episodes"] = fact["test_episodes"].fillna(0).astype(int)
enough = (
    fact["test_episodes"].ge(20)
    & fact["peer_episodes"].ge(30)
    & fact["peer_operators"].ge(3)
)
fact["test_status"] = np.select(
    [~enough, fact["test_difference_pct"].ge(20)],
    ["Недостаточно данных", "Отставание на test"],
    default="Отставание не подтверждено",
)
fact["confirmed_lag"] = (
    enough & fact["test_difference_pct"].ge(20)
    & fact["train_operator_topic_episodes"].ge(30)
    & fact["peer_score"].gt(0) & fact["within_score"].gt(0)
)

levels = ["level1", "level2", "level3"]
fact[levels] = pd.DataFrame(fact.pop("topic").tolist(), columns=levels, index=fact.index)
fact["report_month"] = month.start_time.date()
fact["train_before"] = start.date()
fact["model_version"] = "ridge_topic_allocation_v2"
fact["model_alpha"] = fact["bl_type"].map({ch: m.alpha for ch, (_, m, _) in models.items()})

g = fact.groupby("_episode", sort=False)
assert len(g) == len(test) and g.size().eq(g["n_topics"].first()).all()
assert np.allclose(g["allocation_weight"].sum(), 1)
assert np.allclose(g["topic_duration"].sum(), g["handle_duration"].first())
assert not fact.duplicated(episode_key + levels).any()
operator_topic_fact = fact.drop(columns="_episode")
display(operator_topic_fact.head())
```

В двухтемном сегменте `handle_duration` и `aht_duration` повторяются в обеих строках. Складывай `topic_duration` и `topic_plan_seconds`. Флаг `is_estimated` равен 1 для обоих способов разделения пары и 0 для однотемного сегмента.

## Аналитический отчёт за отчётный месяц

#### 1. Подготовить данные

Каждая тема встречается в сегменте один раз. В расчёт плана и факта включаем одни и те же строки с известным нормативом. Для двухтемных сегментов берём выделенную теме долю секунд.

```python
source = operator_topic_fact
topic_keys = ["report_month", "bl_type", "med_name", "level1", "level2", "level3"]
person_keys = topic_keys + ["login"]

known_plan = source["topic_plan_seconds"].gt(0)
report = source.assign(fact_with_plan=source["topic_duration"].where(known_plan))
```

#### 2. Посчитать результат каждого сотрудника на теме

Счётчики проверки уже рассчитаны в модели и повторяются на строках одной группы. Берём их один раз. Эпизоды и распределённые секунды суммируем по строкам темы.

```python
people = report.groupby(person_keys, dropna=False, sort=False).agg(
    episodes=("login", "size"),
    with_plan=("fact_with_plan", "count"),
    estimated=("is_estimated", "sum"),
    fact_sec=("fact_with_plan", "sum"),
    plan_sec=("topic_plan_seconds", "sum"),
    single_episodes=("test_episodes", "first"),
    peer_episodes=("peer_episodes", "first"),
    peer_operators=("peer_operators", "first"),
    peer_gap_pct=("test_difference_pct", "first"),
    confirmed=("confirmed_lag", "first"),
).reset_index()

n = people["with_plan"].replace(0, np.nan)
people["fact_avg_sec"] = people["fact_sec"] / n
people["plan_avg_sec"] = people["plan_sec"] / n
people["plan_pct"] = 100 * people["plan_sec"] / people["fact_sec"].where(n.notna())
people["over_sec"] = (people["fact_sec"] - people["plan_sec"]).where(n.notna())
people["estimated_pct"] = 100 * people["estimated"] / people["episodes"]
```

#### 3. Определить кандидатов на обучение

Сотрудник попадает в список, если по теме превышен норматив и модель подтвердила отставание. confirmed_lag уже требует положительных peer_score и within_score, ≥30 эпизодов на train, ≥20 однотемных у сотрудника на test, ≥30 у коллег, ≥3 коллег и отставание по медиане ≥20%. Для назначений берём сотрудников, работающих на последнюю дату данных отчётного месяца.

```python
as_of = source.groupby("report_month")["seg_start"].transform("max").dt.normalize()
active = source["employment_date"].le(as_of) & (
    source["fired_date"].isna() | source["fired_date"].gt(as_of)
)
staff = source.loc[active, ["report_month", "login"]].drop_duplicates()
people = people.merge(staff.assign(active=True), on=["report_month", "login"],
                      how="left", validate="many_to_one")

people["below_plan"] = people["over_sec"].gt(0)
people["to_train"] = people["below_plan"] & people["confirmed"] & people["active"].eq(True)
```

#### 4. Срез по сотрудникам

Одна строка — одно назначение. У каждого сотрудника первой показываем тему с наибольшим суммарным превышением. Факт и план в среднем рассчитаны на эпизод с известным нормативом.

```python
labels = {
    "report_month": "Месяц", "login": "Сотрудник", "bl_type": "Канал", "med_name": "Проект",
    "level1": "Уровень 1", "level2": "Уровень 2", "level3": "Уровень 3",
    "episodes": "Эпизодов", "with_plan": "С нормативом", "estimated_pct": "Из двухтемных, %",
    "single_episodes": "Однотемных у сотрудника", "peer_episodes": "Однотемных у коллег",
    "peer_operators": "Коллег", "peer_gap_pct": "Дольше коллег, %",
    "plan_avg_sec": "План на эпизод, с", "fact_avg_sec": "Факт на эпизод, с",
    "plan_pct": "Выполнение, %", "over_sec": "Превышение за месяц, с",
}
employee_report = (
    people.loc[people["to_train"]].sort_values(
        ["report_month", "login", "over_sec"], ascending=[True, True, False]
    )[list(labels)].rename(columns=labels).reset_index(drop=True)
)
display(employee_report.round(1))
```

#### 5. Срез по темам

Считаем сотрудников по всей теме внутри месяца, канала и проекта. В число сотрудников ниже плана входят все, у кого факт выше норматива, включая тех, кому не хватило данных для подтверждения. Для организации обучения отдельно показываем число действующих кандидатов и их суммарное превышение.

```python
topic_report = (
    people.assign(training_sec=people["over_sec"].where(people["to_train"], 0))
    .groupby(topic_keys, dropna=False, sort=False)
    .agg(
        operators=("login", "size"),
        evaluated=("plan_pct", "count"),
        missed=("below_plan", "sum"),
        candidates=("to_train", "sum"),
        training_sec=("training_sec", "sum"),
    ).reset_index()
)
topic_report["missed_pct"] = 100 * topic_report["missed"] / topic_report["evaluated"].replace(0, np.nan)
topic_report = topic_report.sort_values(
    ["candidates", "training_sec", "missed"], ascending=False
).rename(columns={**labels, "operators": "Сотрудников", "evaluated": "С известным планом",
                  "missed": "Ниже плана", "missed_pct": "Ниже плана, %",
                  "candidates": "На обучение", "training_sec": "Превышение кандидатов, с"})
display(topic_report.reset_index(drop=True).round(1))
```
