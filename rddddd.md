# Западающие темы операторов · ежемесячный отчёт

## I. SQL · исходные сегменты

Сначала создали срез данных c информацией о дате приема и увольнения: 
```sql
create table iceberg__write_access.sandbox_cc.operator_theme_ml_model_v3
with (
    location = ''
)
as
with

-- 1. Собираем темы и фактическую длительность на уровне сегмента оператора

episodes as (
    select
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
        max(a.client_id) as client_id,

        case
            when count(a.start_dt) = count(*)
             and min(a.start_dt) = max(a.start_dt)
            then min(a.start_dt)
        end as start_dt,

        count_if(a.channel = 'Chat') = count(*) as is_chat,

        array_agg(
            cast(
                row(a.theme, a.level1, a.level2, a.level3)
                as row(
                    theme  varchar,
                    level1 varchar,
                    level2 varchar,
                    level3 varchar
                )
            )
            order by a.created_date nulls last
        ) as theme_sequence

    from iceberg__write_access.sandbox_cc.themesflow_new a

    where a.taken_flg = 1
      and a.bl_type in ('Чат ФЛ', 'Голос ФЛ')

    group by
        a.session_id, a.segment, a.login, a.portal_id,
        a.city, a.fio, a.su, a.med_name, a.bl_type,
        a.seg_start, a.seg_end, a.handle_duration
),

-- 2. Выбираем уникальные сочетания полей, по которым ищем план чата
chat_keys as (
    select distinct
        login,
        med_name,
        start_dt,
        seg_start
    from episodes
    where bl_type = 'Чат ФЛ'
      and is_chat
      and start_dt is not null
),

-- 3. Ищем план чата по проекту, периоду, стажу и числу параллельных сессий
chat_matches as (
    select
        a.login,
        a.med_name,
        a.start_dt,
        a.seg_start,
        cast(p.aht_target as double) as target_sec

    from chat_keys a

    left join iceberg__write_access.sandbox_cc.dict_chat_sessions c
        on lower(a.login) = lower(c.naumen_login)
       and cast(a.start_dt as timestamp(6)) between c.startdate and c.enddate

    left join iceberg__write_access.sandbox_cc.dict_emp_experience_new s
        on lower(a.login) = lower(s.naumen_login)
       and a.start_dt = s.dt
       and a.med_name = s.project_name

    left join iceberg__write_access.sandbox_cc.dict_target_aht p
        on a.med_name = p.project_name
       and cast(a.seg_start as date) between cast(p.valid_from as date)
                           and cast(p.valid_to as date)
       and s.experience_months = p.experience
       and coalesce(cast(c.field_value_2 as integer), 3) = p.sessions_cnt
),

-- 4. Возвращаем одну строку на ключ чата

chat_plans as (
    select
        login,
        med_name,
        start_dt,
        seg_start,
        case
            when count(target_sec) = count(*)
             and min(target_sec) = max(target_sec)
             and min(target_sec) > 0
            then min(target_sec)
        end as aht_target

    from chat_matches
    group by login, med_name, start_dt, seg_start
),

-- 5. Берём принятые голосовые сегменты из callflow_new.

voice_segments as (
    select
        a.session_id,
        a.segment,
        lower(a.login) as login_key,
        a.med_name,
        a.start_dt,
        a.seg_start_raw,

        coalesce(
            case
                when b.transfer_med_name = 'CC_INBOUND_2LINE' then 0
                else b.transfer_flg
            end,
            0
        ) as transfer_flag

    from iceberg__write_access.sandbox_cc.callflow_new a

    left join iceberg__write_access.sandbox_cc.callflow_transfers_new b
        on a.session_id = b.session_id
       and a.segment = b.segment

    where a.taken_flg = 1
),

-- 6. Ищем план голоса по проекту, seg_start_raw, стажу и признаку перевода.

voice_plans as (
    select
        a.session_id,
        a.segment,
        a.login_key,
        a.med_name,

        case
            when min(a.transfer_flag) = max(a.transfer_flag)
             and count(p.aht_target) = count(*)
             and min(cast(p.aht_target as double)) = max(cast(p.aht_target as double))
             and min(cast(p.aht_target as double)) > 0
            then min(cast(p.aht_target as double))
        end as aht_target

    from voice_segments a

    left join iceberg__write_access.sandbox_cc.dict_emp_experience_new s
        on a.login_key = lower(s.naumen_login)
       and a.start_dt = s.dt
       and a.med_name = s.project_name

    left join iceberg__write_access.sandbox_cc.dict_target_aht p
        on a.med_name = p.project_name
       and a.seg_start_raw between p.valid_from and p.valid_to
       and s.experience_months = p.experience
       and a.transfer_flag = coalesce(p.transfer_flg, 0)

    group by a.session_id, a.segment, a.login_key, a.med_name
)

-- Итог: тематический сегмент, кадровые даты и единый aht_target.
select
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

    case
        when d1.naumen_login is not null then d1.employment_date
        else d2.employment_date
    end as employment_date,

    case
        when d1.naumen_login is not null then d1.fired_date
        else d2.fired_date
    end as fired_date,

    case
        when e.bl_type = 'Чат ФЛ' then c.aht_target
        when e.bl_type = 'Голос ФЛ' then v.aht_target
    end as aht_target

from episodes e

left join chat_plans c
    on e.bl_type = 'Чат ФЛ'
   and e.is_chat
   and e.login = c.login
   and e.med_name = c.med_name
   and e.start_dt = c.start_dt
   and e.seg_start = c.seg_start

left join voice_plans v
    on e.bl_type = 'Голос ФЛ'
   and e.session_id = v.session_id
   and e.segment = v.segment
   and lower(e.login) = v.login_key
   and e.med_name = v.med_name

left join iceberg__write_access.sandbox_cc.dict_emp_raw_new d1
    on lower(trim(e.login)) = lower(trim(d1.naumen_login))
   and cast(e.seg_start as date) >= cast(d1.employment_date as date)
   and (d1.fired_date is null or cast(e.seg_start as date) < cast(d1.fired_date as date))

left join iceberg__write_access.sandbox_cc.dict_emp_raw_new d2
    on d1.naumen_login is null
   and e.portal_id = d2.portal_id
   and cast(e.seg_start as date) >= cast(d2.employment_date as date)
   and (d2.fired_date is null or cast(e.seg_start as date) < cast(d2.fired_date as date));

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

## II. Загрузка данных из Trino

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


report_date = pd.Timestamp("2026-10-31")
month = pd.Period("2026-10", freq="M")
if report_date < month.start_time:
    raise ValueError("Отчётная дата раньше выбранного месяца")
cutoff = min(report_date.normalize() + pd.Timedelta(days=1), (month + 1).start_time)
sql = f"""
select *
from iceberg__write_access.sandbox_cc.operator_theme_ml_model_v3
where seg_start < timestamp '{cutoff:%Y-%m-%d %H:%M:%S}'
"""
df = lake.read_dataframe(sql)
```

## III. Первичная обработка

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
if duplicate_mask.any():
    raise ValueError("Обнаружены дубли эпизодов. Проверь соединения в SQL до обучения.")
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

## IV. Модель Ridge и тематическая витрина

### 1. Период обучения и расчёта

Месяц и последний включённый день заданы в разделе II. Ridge обучается на более ранних сегментах без сессий отчётного месяца.

```python
import numpy as np


ALPHA = 30
MIN_TRAIN_EPISODES = 20
MIN_WITHIN_SCORE = 0.15
USE_TENURE_BINS = False

eligible = (
    df_model["n_topics"].isin([1, 2])
    & np.isfinite(df_model["handle_duration"])
    & df_model["handle_duration"].gt(0)
)

start = month.start_time
end = cutoff
data = df_model.loc[eligible]
test = data.loc[data["seg_start"].between(start, end, inclusive="left")].copy()
train = data.loc[data["seg_start"] < start, [
    "session_id", "login", "med_name", "bl_type", "topics", "tenure_days", "handle_duration", "seg_start"
]].copy()
train = train.loc[~train["session_id"].isin(test["session_id"])]

if test.empty:
    raise ValueError("Нет сегментов за выбранный месяц.")
if test[episode_key].isna().any().any() or test.duplicated(episode_key).any():
    raise ValueError("Проверь ключ сегмента: обнаружены пропуски или повторы.")

if train.empty:
    raise ValueError("Для выбранного месяца нет истории обучения.")

for frame in (train, test):
    frame["topics"] = frame["topics"].map(lambda seq: list(dict.fromkeys(seq)))
```

### 2. Признаки сегмента

Учитываем сотрудника, проект, стаж и темы обращения. Каждая из двух тем получает вес ½.

```python
import json
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import Ridge


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

        if USE_TENURE_BINS and not missing:
            bounds = (30, 90, 180, 365)
            f["tenure_bin"] = str(np.searchsorted(bounds, row.tenure_days, side="right"))

        weight = 1 / len(row.topics)
        for topic in row.topics:
            f["topic_" + json.dumps(topic, ensure_ascii=False)] = weight
            f[pair_key(row.login, topic)] = weight
            f["tp_" + json.dumps([str(row.med_name), topic], ensure_ascii=False)] = weight
        yield f
```

### 3. Обучение по каналам

Для голоса и чата обучаем отдельные модели логарифма длительности с alpha=30.

```python
models = {}

for channel, history in train.groupby("bl_type"):
    days = history.loc[history["tenure_days"].ge(0), "tenure_days"]
    fill_days = days.median() if len(days) else 0

    vec = DictVectorizer(dtype=np.float32)
    X = vec.fit_transform(make_features(history, fill_days))

    model = Ridge(alpha=ALPHA, solver="lsqr")
    model.fit(X, np.log(history["handle_duration"]))
    models[channel] = (vec, model, fill_days)
    del X
```

### 4. Скоры сотрудника

История пары должна содержать ≥20 эпизодов, внутренний скор должен превышать 0,15. Отклонение от коллег рассчитывается по той же теме и каналу и выводится в процентах.

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
    for _, group in scores.groupby("topic", sort=False):
        effects = group["total_effect"].to_numpy()
        if len(effects) < 2:
            continue

        middle = np.median(effects)
        for mask in [effects < middle, effects == middle, effects > middle]:
            positions = np.flatnonzero(mask)
            if len(positions):
                others = np.delete(effects, positions[0])
                scores.loc[group.index[positions], "peer_effect"] = np.median(others)

    scores["peer_score"] = scores["total_effect"] - scores["peer_effect"]
    scores["peer_score_pct"] = 100 * np.expm1(scores["peer_score"])
    scores["bl_type"] = channel
    scores["topic_candidate"] = (
        scores["train_operator_topic_episodes"].ge(MIN_TRAIN_EPISODES) & scores["within_score"].gt(MIN_WITHIN_SCORE)
    )
    parts.append(scores[["bl_type", "login", "topic", "train_operator_topic_episodes",
                         "within_score", "peer_effect", "peer_score", "peer_score_pct", "topic_candidate"]])
score_table = pd.concat(parts, ignore_index=True)
```

### 5. Распределение времени тем

Для двух тем распределяем фактические секунды пропорционально однотемным прогнозам. Если прогноз недоступен или одна тема не встречалась отдельно на истории, каждой теме достаётся половина.

```python
single = train.loc[train["topics"].str.len().eq(1), ["bl_type", "topics"]].copy()
single["topic"] = single.pop("topics").str[0]
support = single.groupby(["bl_type", "topic"]).size().rename("train_topic_single_episodes")

def allocate_topic_seconds(segments):
    fact = segments.assign(_episode=np.arange(len(segments)), n_topics=segments["topics"].str.len())
    fact = fact.explode("topics", ignore_index=True).rename(columns={"topics": "topic"})
    fact = fact.join(support, on=["bl_type", "topic"], validate="many_to_one")
    two = fact["n_topics"].eq(2)
    fact["is_estimated"] = two.astype("int8")
    fact["single_prediction_sec"] = np.nan

    for channel, part in fact.loc[two].groupby("bl_type", observed=True):
        if channel not in models:
            continue

        vec, model, fill_days = models[channel]
        inputs = part[["login", "med_name", "tenure_days"]].copy()
        inputs["topics"] = part["topic"].map(lambda topic: [topic])

        X = vec.transform(make_features(inputs, fill_days))
        fact.loc[part.index, "single_prediction_sec"] = np.exp(
            model.predict(X).astype(float)
        )

    pred = fact["single_prediction_sec"]
    episode = fact["_episode"]
    supported = pred.gt(0) & np.isfinite(pred) & fact["train_topic_single_episodes"].gt(0)
    use_model = two & supported.groupby(episode).transform("all")
    total_pred = pred.groupby(episode).transform("sum")

    fact["allocation_weight"] = np.where(two, 0.5, 1.0)
    fact.loc[use_model, "allocation_weight"] = pred[use_model] / total_pred[use_model]

    fact["allocation_method"] = "single_observed"
    fact.loc[two, "allocation_method"] = "equal_split_fallback"
    fact.loc[use_model, "allocation_method"] = "ridge_proportional"

    fact["topic_duration"] = fact["handle_duration"] * fact["allocation_weight"]
    return fact.drop(columns=["single_prediction_sec", "train_topic_single_episodes", "_episode"])
```

### 6. Факт и норматив отчётного месяца

Формируем строку на каждую тему сегмента. Фактическое время и норматив делятся в одинаковых пропорциях.

```python
ref_keys = ["bl_type", "med_name", "topic", "n_topics"]
fact = allocate_topic_seconds(test.drop(columns=["theme_sequence", "is_chat",
    "missing_theme_dates", "duplicated_theme_dates"], errors="ignore"))
fact = fact.rename(columns={"aht_target": "aht_duration"})
plan = fact["aht_duration"].where(fact["aht_duration"].gt(0) & np.isfinite(fact["aht_duration"]))
fact["topic_plan_seconds"] = plan * fact["allocation_weight"]
fact["episode_plan_pct"] = 100 * plan / fact["handle_duration"]
```

### 7. Среднее за три календарных месяца

Берём два месяца до отчётного и доступные дни самого отчётного месяца. Среднее считается по эпизодам всех сотрудников одного канала и проекта, отдельно для каждой темы и числа тем в сегменте.

```python
window_start = (month - 2).start_time
recent_train = train.loc[train["seg_start"].ge(window_start)]
reference_parts = [fact[ref_keys + ["topic_duration"]]]
for offset in range(0, len(recent_train), 100_000):
    chunk = allocate_topic_seconds(recent_train.iloc[offset:offset + 100_000])
    reference_parts.append(chunk[ref_keys + ["topic_duration"]])

mean_3m = (
    pd.concat(reference_parts, ignore_index=True)
    .groupby(ref_keys, dropna=False)["topic_duration"].mean()
    .rename("mean_3m_sec")
)
del reference_parts, recent_train
fact = fact.join(mean_3m, on=ref_keys, validate="many_to_one")
```

### 8. Детальная витрина для БД

Сохраняем все подходящие сегменты отчётного месяца. Для суммирования по темам используйте `topic_duration` и `topic_plan_seconds`. Полное время сегмента повторяется в обеих строках двухтемного обращения. При повторной записи месяца обновляйте его целиком по ключу сегмента и уровням темы.

```python
fact = fact.merge(score_table, on=["bl_type", "login", "topic"], how="left", validate="many_to_one")
fact["train_operator_topic_episodes"] = fact["train_operator_topic_episodes"].fillna(0).astype(int)
fact["topic_candidate"] = fact["topic_candidate"].eq(True)
levels = ["level1", "level2", "level3"]
fact[levels] = pd.DataFrame(fact.pop("topic").tolist(), index=fact.index, columns=levels)
fact["report_month"] = month.start_time.date()
fact["model_version"] = ("ridge_mean3m_tenure_bins" if USE_TENURE_BINS else "ridge_mean3m")
fact["model_alpha"] = ALPHA
operator_topic_fact = fact
```

### 9. Запись детальной витрины

Положите `month_writer.py` рядом с ноутбуком. Один раз при настройке окружения импортируйте ваш корпоративный WebHDFSHook тем же импортом, что в рабочем примере. В примере путь импорта скрыт. Создайте клиент после инициализации lake. В .env задайте WEBHDFS_HOST и WEBHDFS_CA_BUNDLE.

```python
from month_writer import MonthWriter

settings = dict(dotenv_values(".env"))
settings.update(os.environ)
hook = WebHDFSHook(
    webhdfs_conn_id=None,
    host=settings["WEBHDFS_HOST"],
    port=443,
    kerberos=True,
    ca_cert=settings["WEBHDFS_CA_BUNDLE"],
    use_https=True,
)

location = settings["OPERATOR_TOPIC_FACT_LOCATION"]
writer = MonthWriter(lake, hook.get_conn(), location)
```

Ежемесячная выгрузка занимает одну строку. Сначала класс записывает Parquet частями по 100 000 строк, затем загружает части в HDFS и регистрирует внешнюю таблицу Hive. Новый месяц вставляется через insert-select. При пересчёте используется один merge из той же Hive-таблицы. Запускайте одну публикацию месяца одновременно. Частичный месяц заменяет весь ранее сохранённый срез этого месяца.

```python
written_rows = writer.write(operator_topic_fact, month)
print("Записано строк:", written_rows)
```

Если данные не прошли проверку, класс выводит причину и возвращает 0. Ошибки подключения и выполнения запросов видны в ноутбуке.

## V. Аналитический отчёт

### 1. Подготовка данных

Для сравнения с нормативом берём строки с известным планом. Для сравнения со средним используем распределённые секунды каждой темы.

```python
source = operator_topic_fact
topic_keys = ["report_month", "bl_type", "med_name", "level1", "level2", "level3"]
person_keys = topic_keys + ["login"]

known_plan = source["topic_plan_seconds"].gt(0)
report = source.assign(
    fact_with_plan=source["topic_duration"].where(known_plan),
    gap_to_mean_sec=source["topic_duration"] - source["mean_3m_sec"],
)
```

### 2. Показатели сотрудника по теме

Суммируем отклонения от трёхмесячного среднего за отчётный период. Быстрые эпизоды компенсируют медленные. Отрицательный итог заменяем нулём.

```python
people = report.groupby(person_keys, dropna=False, sort=False).agg(
    episodes=("login", "size"),
    with_plan=("fact_with_plan", "count"),
    fact_sec=("fact_with_plan", "sum"),
    plan_sec=("topic_plan_seconds", "sum"),
    topic_candidate=("topic_candidate", "first"),
    train_episodes=("train_operator_topic_episodes", "first"),
    peer_score_pct=("peer_score_pct", "first"),
).reset_index()

gain = (
    report.groupby(person_keys, dropna=False)["gap_to_mean_sec"]
    .sum(min_count=1)
    .clip(lower=0)
    .rename("mean_gain_sec")
)

people = people.join(gain, on=person_keys, validate="one_to_one")

n = people["with_plan"].replace(0, np.nan)
people["fact_avg_sec"] = people["fact_sec"] / n
people["plan_avg_sec"] = people["plan_sec"] / n
people["plan_pct"] = 100 * people["plan_sec"] / people["fact_sec"].where(n.notna())
people["over_sec"] = (people["fact_sec"] - people["plan_sec"]).where(n.notna())
```

### 3. Отбор на обучение

Выбираем действующих сотрудников с историческим сигналом и превышением плана. Дата проверки занятости — последняя доступная дата отчётного периода.

```python
as_of = report_date.normalize()
active = source["employment_date"].le(as_of) & (
    source["fired_date"].isna() | source["fired_date"].gt(as_of)
)
staff = source.loc[active, ["report_month", "login"]].drop_duplicates()
people = people.merge(staff.assign(active=True), on=["report_month", "login"],
                      how="left", validate="many_to_one")

people["below_plan"] = people["over_sec"].gt(0)

people["to_train"] = (
    people["active"].eq(True)
    & people["below_plan"]
    & people["topic_candidate"]
)
```

### 4. Итоговый список назначений

В employee_report сохраняем канал, проект, сотрудника и одну тему. Список отсортирован по потенциалу в секундах. Внутренний скор используется только в отборе.

```python
staff_keys = ["report_month", "bl_type", "med_name", "login"]
staff_info = (
    source[staff_keys + ["seg_start", "session_id", "segment", "su", "city"]]
    .sort_values(["seg_start", "session_id", "segment"])
    .drop_duplicates(staff_keys, keep="last")
)
labels = {
    "report_month": "Месяц", "login": "Сотрудник", "bl_type": "Канал", "med_name": "Проект",
    "su": "Руководитель", "city": "Город сотрудника",
    "level1": "Уровень 1", "level2": "Уровень 2", "level3": "Уровень 3",
    "episodes": "Эпизодов", "plan_avg_sec": "План на эпизод, с", "fact_avg_sec":     "Факт на эпизод, с", "plan_pct": "Выполнение, %",
    "over_sec": "Превышение за месяц, с",
    "mean_gain_sec": "Потенциал до среднего за 3 мес. за отчётный период, с",
    "train_episodes": "Эпизодов на истории",
}
employee_report = (
    people.loc[people["to_train"]]
    .merge(staff_info[staff_keys + ["su", "city"]], on=staff_keys, how="left", validate="many_to_one")
    .sort_values(
        ["mean_gain_sec", "over_sec"], ascending=False
    )[list(labels)].rename(columns=labels).reset_index(drop=True)
)
display(employee_report.round(1))
```

**Как читать строку назначения.** Выполнение плана равно сумме нормативных секунд, делённой на сумму фактических секунд тех же эпизодов, в процентах. Потенциал — сумма разниц между временем темы и средним за три месяца, ограниченная снизу нулём после суммирования. Например, 12 000 с при сумме ориентиров 10 500 с дают 1 500 с потенциала. Это сценарная оценка за отчётный период. Реализованный эффект измеряется после обучения.

## VI. Валидация

| Проверка         | На какой вопрос отвечает                                                                              |
| ---------------- | ----------------------------------------------------------------------------------------------------- |
| 1. Время и MAE   | Есть ли пересечение выборок и выигрывает ли Ridge у базового прогноза?                                |
| 2. Доли двух тем | Какова ошибка деления на псевдопарах, включая штатный переход к 50/50?                                |
| 3. Скоры         | Совпадают ли коэффициенты, число эпизодов и флаг отбора с независимым расчётом?                       |
| 4. Стаж и сигнал | Где прогноз систематически смещён и повторяется ли исторический сигнал на однотемных обращениях test? |

### Подготовка · функция расчёта MAE


```python
def check_model():
    g = operator_topic_fact.groupby(episode_key, sort=False, dropna=False)
    assert len(g) == len(test)
    assert not operator_topic_fact.duplicated(episode_key + levels).any()
    assert np.allclose(g["allocation_weight"].sum(), 1)
    assert np.allclose(g["topic_duration"].sum(), g["handle_duration"].first())
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
    return model_quality.round(1)

```


Следующие ячейки запускаются после месячного отчёта. Они используют обученные модели и сохраняют действующие правила отбора.

### 1. Разделение по времени и ошибка длительности

MAE измеряется в секундах на сегмент. Для всех сегментов используется более простой ориентир по каналу. Положительный improvement_pct означает выигрыш у соответствующего ориентира. При оценке приемлемости ошибки учитывайте её абсолютный размер и потерю качества между train и test.

```python
assert train["seg_start"].lt(start).all()
assert test["seg_start"].between(start, end, inclusive="left").all()
assert not train["session_id"].isin(test["session_id"]).any()
assert set(test["bl_type"]) <= set(models), "Для части test отсутствует модель"
assert not employee_report.duplicated([labels[k] for k in person_keys]).any()
quality = check_model()
display(quality)
```

### 2. Проверка деления времени на примерах с известными долями

Внутри одного канала, проекта и сотрудника случайно соединяем два однотемных эпизода test с разными темами из разных сессий. Каждый исходный эпизод используется максимум один раз. Обе длительности известны. Передаём в рабочую функцию только суммарную длительность и сравниваем ошибки на тех же парах. Включаем также случаи штатного деления пополам. Размер выборки и доля таких случаев помогают интерпретировать результат.

```python
single_test = test.loc[test["topics"].str.len().eq(1)].sample(frac=1, random_state=42).copy()
single_test["first_topic"] = single_test["topics"].str[0]
group = single_test.groupby(["bl_type", "med_name", "login"], dropna=False, sort=False)
single_test["second_topic"] = group["first_topic"].shift(-1)
single_test["second_sec"] = group["handle_duration"].shift(-1)
other_session = group["session_id"].shift(-1)
pairs = single_test.loc[
    group.cumcount().mod(2).eq(0) & single_test["second_sec"].notna()
    & single_test["session_id"].ne(other_session)
    & single_test["first_topic"].ne(single_test["second_topic"])
].copy()

if pairs.empty:
    print("Нет подходящих пар однотемных эпизодов.")
else:
    pairs["true_first_sec"] = pairs["handle_duration"]
    pairs["handle_duration"] = pairs["true_first_sec"] + pairs["second_sec"]
    pairs["topics"] = list(map(list, zip(pairs["first_topic"], pairs["second_topic"])))
    allocated = allocate_topic_seconds(pairs)
    first = allocated.loc[allocated["topic"].eq(allocated["first_topic"])].copy()
    assert len(first) == len(pairs)
    first["model_error"] = (first["topic_duration"] - first["true_first_sec"]).abs()
    first["half_error"] = (first["handle_duration"] / 2 - first["true_first_sec"]).abs()
    first["fallback"] = first["allocation_method"].eq("equal_split_fallback")
    split_quality = first.groupby("bl_type").agg(
        pairs=("login", "size"), fallback_share=("fallback", "mean"),
        ridge_mae_sec=("model_error", "mean"), half_mae_sec=("half_error", "mean"),
    )
    split_quality["gain_sec"] = split_quality["half_mae_sec"] - split_quality["ridge_mae_sec"]
    # Парный выигрыш. При повторной выборке целиком сохраняем кластер сотрудника.
    rng = np.random.default_rng(42)
    first["gain"] = first["half_error"] - first["model_error"]
    for channel, sample in first.groupby("bl_type"):
        clusters = sample.groupby("login")["gain"].agg(["sum", "size"]).to_numpy()
        if len(clusters) >= 2:
            draws = rng.integers(len(clusters), size=(1000, len(clusters)))
            totals = clusters[draws].sum(axis=1)
            ci = np.quantile(totals[:, 0] / totals[:, 1], [0.025, 0.975])
            split_quality.loc[channel, ["ci_low_sec", "ci_high_sec"]] = ci
    display(split_quality.round(3))
```

Положительный gain_sec показывает среднее снижение абсолютной ошибки на тему в этих составленных парах. Ошибки двух долей одной пары равны по модулю, поэтому достаточно первой темы. В реальном двухтемном эпизоде часть действий общая. Для прямой проверки на таких эпизодах нужна независимая разметка времени тем.

### 3. Независимая проверка скоров

На выборке до 200 пар в канале пересчитываем число эпизодов, решение по порогу и процент отклонения от коллег. Для peer_score исключаем сотрудника напрямую и берём медиану оставшихся сотрудников. Случай единственного оператора должен дать NaN.

```python
for channel, history in train.groupby("bl_type"):
    vec, model, _ = models[channel]
    coef = dict(zip(vec.get_feature_names_out(), model.coef_))
    counts = history[["login", "topics"]].explode("topics").groupby(["login", "topics"]).size()
    stored = score_table.loc[score_table["bl_type"].eq(channel)]
    assert not stored.duplicated(["login", "topic"]).any()
    assert len(stored) == len(counts)
    by_login = counts.reset_index().groupby("login")["topics"].agg(list)
    by_topic = counts.reset_index().groupby("topics")["login"].agg(list)
    for row in stored.sample(min(200, len(stored)), random_state=42).itertuples():
        login, topic = row.login, row.topic
        effect = coef[pair_key(login, topic)]
        own = [coef[pair_key(login, t)] for t in by_login.at[login]]
        n = counts.at[(login, topic)]
        assert n == row.train_operator_topic_episodes
        assert np.isclose(effect - np.median(own), row.within_score, atol=1e-6)
        assert bool(n >= MIN_TRAIN_EPISODES and effect - np.median(own) > MIN_WITHIN_SCORE) == row.topic_candidate
        others = [coef["operator=" + str(o)] + coef[pair_key(o, topic)]
                  for o in by_topic.at[topic] if o != login]
        total = coef["operator=" + str(login)] + effect
        expected = 100 * np.expm1(total - np.median(others)) if others else np.nan
        assert np.isclose(expected, row.peer_score_pct, equal_nan=True, atol=1e-4)
print("Проверки скоров прошли.")
```


### 4. Стаж и устойчивость исторического сигнала

Для стажа выводим медианный процент ошибки по каждому каналу и размер группы. Для устойчивости берём однотемные эпизоды отчётного месяца. Убираем из прогноза вклад пары и сравниваем остаток по данной теме с остатком на остальных темах сотрудника. Это наблюдаемый показатель на test. Сравнение охватывает пары с минимум пятью однотемными наблюдениями по теме и пятью по другим темам. Между группами возможны различия в составе. Процент верных назначений из этой проверки получить нельзя.

```python
residuals = []
for channel, sample in test.groupby("bl_type"):
    vec, model, fill_days = models[channel]
    coef = dict(zip(vec.get_feature_names_out(), model.coef_))
    for offset in range(0, len(sample), 100_000):
        batch = sample.iloc[offset:offset + 100_000]
        pred = model.predict(vec.transform(make_features(batch, fill_days)))
        block = batch[["bl_type", "login", "tenure_days"]].copy()
        block["res"] = np.log(batch["handle_duration"]) - pred
        block["topic"] = batch["topics"].str[0]
        block["single"] = batch["topics"].str.len().eq(1)
        block["adjusted"] = block["res"] + [
            coef.get(pair_key(login, topic), 0) for login, topic in zip(block.login, block.topic)
        ]
        residuals.append(block)

res = pd.concat(residuals, ignore_index=True)
res["tenure_group"] = pd.cut(res["tenure_days"], [0, 30, 90, 180, 365, np.inf], right=False)
bias = res.groupby(["bl_type", "tenure_group"], observed=True)["res"].agg(["size", "median"])
bias["median_fact_over_prediction_pct"] = 100 * np.expm1(bias.pop("median"))
display(bias.round(1))

pair_keys = ["bl_type", "login", "topic"]
observed = res.loc[res["single"]].groupby(pair_keys)["adjusted"].agg(["sum", "size"]).reset_index()
totals = observed.groupby(["bl_type", "login"])[["sum", "size"]].transform("sum")
other_n = totals["size"] - observed["size"]
observed["within_test"] = (observed["sum"] / observed["size"]
                           - (totals["sum"] - observed["sum"]) / other_n.where(other_n > 0))
observed = observed.loc[observed["size"].ge(5) & other_n.ge(5)].merge(
    score_table, on=pair_keys, validate="one_to_one")
observed = observed.loc[observed["train_operator_topic_episodes"].ge(MIN_TRAIN_EPISODES)].copy()
observed["peer_group"] = np.select(
    [observed.peer_score_pct.isna(), observed.peer_score_pct.gt(0)],
    ["нет коллег", "выше нуля"], default="ноль или ниже")
display(observed.groupby(["bl_type", "topic_candidate", "peer_group"]).agg(
    pairs=("within_test", "size"), avg_within_test=("within_test", "mean"),
    share_above_zero=("within_test", lambda x: x.gt(0).mean()),
).round(3))
```
