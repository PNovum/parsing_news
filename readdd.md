# AHT чата и голоса и темы для обучения
Если df уже загружен из новой витрины, начни с раздела «Подготовка df». Для старого df расчёт модели возможен, но голосовой AHT появится только после загрузки обновлённых данных.

## SQL витрины
Запрос создаёт отдельную таблицу с суффиксом _v2. Переводы сворачиваются до session_id × segment до присоединения к эпизодам. Для голоса исходный отчёт использует a.seg_start_raw. Здесь использован доступный в присланной витрине t.seg_start. Перед использованием норматива сверь, что это та же метка времени, особенно на границе действия плана. При расхождении выведи исходный seg_start_raw в episodes отдельным полем и используй его в сравнении с valid_from и valid_to.

```sql
CREATE TABLE iceberg__write_access.sandbox_cc.operator_theme_ml_model_aht_v2 AS
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

## Загрузка при необходимости
С существующим подключением lake и датами своей выгрузки:
```python
df = lake.read_dataframe("""SELECT * FROM iceberg__write_access.sandbox_cc.operator_theme_ml_model_aht_v2
WHERE seg_start >= TIMESTAMP '2026-01-01 00:00:00' """)
```

## Подготовка df
```python
import numpy as np
import pandas as pd
for col in ["seg_start","employment_date","fired_date"]:
    df[col] = pd.to_datetime(df[col],errors="coerce")
for col in ["handle_duration","aht_target"]:
    df[col] = pd.to_numeric(df[col],errors="coerce")
df["tenure_days"] = (df.seg_start.dt.normalize()-df.employment_date.dt.normalize()).dt.days
def parse_topics(seq):
    result = []
    for item in seq if seq is not None else ():
        levels = tuple(str(v).strip() if pd.notna(v) and str(v).strip() else None for v in item[1:4])
        a,b,c = ((v or "").lower() for v in levels)
        if "".join(a.split())=="техсбой" and b=="тишина" and c in {"оператор не слышит клиента","клиент не слышит оператора"}:
            return ()  # технический эпизод сохраняется в расчёте AHT
        if any(levels):
            result.append(levels)
    return tuple(dict.fromkeys(result))
df["topics"] = df.theme_sequence.map(parse_topics)
```

## 1. Обычная аналитика
Месячный процент — отношение сумм по эпизодам с известным планом. coverage — доля таких эпизодов. Трёхмесячные поля относятся к концу отчётного периода; история считается по всем доступным месяцам этого окна. При частичном покрытии интерпретировать показатель с учётом доли пропусков. Для модели остаются эпизоды с одной или двумя уникальными темами.

```python
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, hstack
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error
from functools import lru_cache

# Заменить датой первого дня после последнего полностью загруженного месяца.
if df.seg_start.dropna().empty:
    raise ValueError("Нет эпизодов с датой seg_start. Проверь период SQL-выгрузки.")
# Консервативно исключаем последний видимый месяц как потенциально неполный.
# Если выгрузка заканчивается закрытым октябрём, задай end = pd.Period("2026-11", "M").
end = df.seg_start.max().to_period("M")
print("Валидация:", end-2, "Отчёт:", end-1)
df["month"] = pd.to_datetime(df.seg_start).dt.to_period("M")
base = df.loc[df.month.lt(end) & df.handle_duration.gt(0) & np.isfinite(df.handle_duration)].copy()
keys = ["month","bl_type","med_name","login"]
base["plan"] = base.aht_target.where(base.aht_target.gt(0) & np.isfinite(base.aht_target))
base["planned_fact"] = base.handle_duration.where(base.plan.notna())
monthly = base.groupby(keys,observed=True).agg(n=("login","size"), covered=("plan","count"),
    fact=("planned_fact","sum"), budget=("plan","sum")).reset_index()
monthly["coverage"] = monthly.covered/monthly.n
monthly["gap_pct"] = 100*(monthly.fact/monthly.budget-1)
last3 = monthly[monthly.month.ge(end-3)].copy()
last3["over"] = last3.gap_pct.gt(0)
history = last3.groupby(keys[1:],observed=True).agg(fact3=("fact","sum"),budget3=("budget","sum"),
    over_months=("over","sum"), valid_months=("gap_pct","count")).reset_index()
history["gap3_pct"] = 100*(history.fact3/history.budget3-1)
monthly = monthly.merge(history,on=keys[1:],validate="many_to_one")
# Разные представления тем сравниваем на одних и тех же эпизодах.
def unique_topics(items):
    return tuple(dict.fromkeys(tuple(x) for x in items))
base["topics"] = base.topics.map(unique_topics)
sample = base[base.topics.str.len().isin([1,2]) & base.login.notna() & base.med_name.notna()].copy()
# Сессии на границе месяцев исключаем из моделирования.
months = sample.groupby("session_id").month.transform("nunique")
sample = sample.loc[months.eq(1)]
```

## 2. Ridge отдельно для чата и голоса
Разреженная матрица содержит оператора, проект, тему, взаимодействие оператора с темой и числовые признаки. Вклад двух тем имеет веса ½. Словарь категорий построен на всём срезе для общей нумерации колонок; признаки без наблюдений в train имеют нулевой коэффициент. Тестовые длительности используются только после выбора параметров.

```python
def available_median(values):
    valid = values.dropna()
    return valid.median() if not valid.empty else np.nan

def design(d, full, train):
    @lru_cache(None)
    def label(t):
        import json
        return json.dumps(t if full or not any(t[1:]) else t[1:],ensure_ascii=False)
    first = d.topics.map(lambda s: label(s[0]))
    second = d.topics.map(lambda s: label(s[-1]))
    two = second.ne(first).to_numpy()
    row = np.arange(len(d))
    blocks = []
    def block(values, rows, weights):
        codes, names = pd.factorize(values)
        blocks.append(csr_matrix((weights,(rows,codes)),shape=(len(d),len(names))))
    for col in ["login","med_name"]:
        block(d[col], row, np.ones(len(d)))
    rr = np.r_[row,row[two]]
    tt = np.r_[first,second[two]]
    weights = np.r_[1-.5*two,np.full(two.sum(),.5)]
    block(tt,rr,weights)
    pair = pd.MultiIndex.from_arrays([d.login.to_numpy()[rr],tt])
    block(pair,rr,weights)
    extra = [d.topics.str.len().to_numpy()-1]
    if d.bl_type.iloc[0]=="Чат ФЛ":
        days = d.tenure_days.where(d.tenure_days.ge(0))
        median = available_median(days[train])
        extra.extend([np.log1p(days.fillna(median if pd.notna(median) else 0)),days.isna()])
    blocks.append(csr_matrix(np.column_stack(extra)))
    return hstack(blocks,format="csr"),first

trials, details = [], []
for channel,d in sample.groupby("bl_type",observed=True):
    d = d.reset_index(drop=True)
    train = d.month.lt(end-2).to_numpy()
    val = d.month.eq(end-2).to_numpy()
    test = d.month.eq(end-1).to_numpy()
    single = d.topics.str.len().eq(1).to_numpy()
    if not (train.any() and val.any() and (test & single).any() and ((train|val) & single).any()):
        print(channel, "Пропуск. Train, validation, test, однотемных test:",
              train.sum(), val.sum(), test.sum(), (test & single).sum())
        continue
    y = np.log(d.handle_duration.to_numpy())
    best = None
    for full in [False,True]:
        X,topic = design(d,full,train)
        for alpha in [1,10,30,100]:
            m = Ridge(alpha=alpha,solver="lsqr",tol=1e-6).fit(X[train],y[train])
            mae = mean_absolute_error(np.exp(y[val]),np.exp(m.predict(X[val])))
            trials.append(dict(channel=channel,level1=full,alpha=alpha,val_mae=mae))
            if best is None or mae<best[0]:
                best = (mae,full,alpha)
    _,full,alpha = best
    X,topic = design(d,full,train|val)
    model = Ridge(alpha=alpha,solver="lsqr",tol=1e-6).fit(X[train|val],y[train|val])
    pred = model.predict(X[test])
    for trial in trials:
        if (trial["channel"],trial["level1"],trial["alpha"])==(channel,full,alpha):
            trial["test_mae"] = mean_absolute_error(np.exp(y[test]),np.exp(pred))
    # Модельный сигнал берём до тестового месяца, по стандартизированным прогнозам:
    # внутри одного проекта, с исключением вклада числовых признаков. Вычитаем общий эффект оператора отдельно.
    single = d.topics.str.len().eq(1).to_numpy()
    pre = d.loc[(train|val)&single,["login","med_name"]].assign(topic=topic)
    pre["log_prediction"] = model.predict(X[(train|val)&single])
    # Первые блоки матрицы — login, med_name; последние — числовые признаки.
    ix = np.flatnonzero((train|val)&single)
    numeric = 3 if channel=="Чат ФЛ" else 1
    pre["adjusted"] = pre.log_prediction - X[ix,-numeric:] @ model.coef_[-numeric:]
    op_codes = pd.factorize(d.login)[0]
    pre["interaction"] = pre.adjusted - model.coef_[op_codes[ix]]
    group = ["med_name","topic","login"]
    scores = pre.groupby(group).agg(score=("adjusted","mean"),interaction=("interaction","mean"),train_n=("login","size")).reset_index()
    # Убираем основной эффект темы перед сравнением тем одного оператора.
    scores["interaction"] -= scores.groupby(["med_name","topic"]).interaction.transform("median")
    scores["within"] = scores.interaction - scores.groupby(["med_name","login"]).interaction.transform("median")
    observed = d.loc[test&single].assign(topic=topic).groupby(group).handle_duration.agg(["size","median","mean"]).reset_index()
    observed = observed.merge(scores,on=group,how="left")
    parts = []
    for _,part in observed.groupby(group[:2]):
        part = part.copy()
        pool = part[part["size"].ge(10)]
        for idx,r in part.iterrows():
            peers = pool[pool.login.ne(r.login)]
            part.loc[idx,["peer_n","peer_median","peer_mean","peer_score"]] = [
                len(peers),available_median(peers["median"]),
                available_median(peers["mean"]),available_median(peers.score)]
        parts.append(part)
    if not parts:
        continue
    out = pd.concat(parts)
    out["model_gap"] = np.expm1(out.score-out.peer_score)
    out["observed_gap"] = out["median"]/out.peer_median-1
    supported = out["size"].ge(20)&out.peer_n.ge(3)&out.train_n.ge(30)&out.peer_score.notna()
    confirmed = supported & out.model_gap.gt(0)&out.within.gt(0)&out.observed_gap.ge(.20)
    out["status"] = np.select([~supported,confirmed],["Мало данных","Разобрать тему"],default="Без подтверждения")
    out["seconds"] = ((out["mean"]-out.peer_mean).clip(lower=0)*out["size"]).where(confirmed,0)
    out["month"],out["bl_type"] = end-1,channel
    details.append(out)
if details:
    detail = pd.concat(details).merge(monthly,on=keys,how="left",validate="many_to_one")
else:
    print("Нет данных для сравнения тем. Проверь месяцы и число однотемных эпизодов.")
    detail = pd.DataFrame(columns=keys+["topic","status","seconds","size"])
selected = detail[detail.status.eq("Разобрать тему")].copy()
if "fired_date" in df:
    active = df.loc[df.employment_date.le((end-1).end_time) & (
        df.fired_date.isna() | df.fired_date.gt((end-1).end_time)), "login"]
    selected = selected[selected.login.isin(active)].copy()
    detail["seconds"] = detail.seconds.where(detail.login.isin(active),0)
selected["login"] = selected.login.astype(str)
training = selected.groupby(["bl_type","med_name","topic"]).agg(
    operators=("login",", ".join), people=("login","nunique"),episodes=("size","sum"),seconds=("seconds","sum")).reset_index()
training["hours"] = training.seconds/3600
training = training.sort_values("hours",ascending=False)
assert np.isclose(float(training.seconds.sum()),float(detail.seconds.sum()))
sheets = {"Обучение":training,"Сотрудники и темы":detail,"AHT по месяцам":monthly,"Выбор модели":pd.DataFrame(trials, columns=["channel","level1","alpha","val_mae","test_mae"])}
```

## 3. Итоговый Excel
В детализации size — число однотемных эпизодов сотрудника, peer_n — число коллег с минимум десятью такими эпизодами. model_gap и observed_gap выражены в долях (0,20 = 20%). within — центрированный эффект темы в логарифмической шкале. score и peer_score — модельные оценки на логарифмической шкале. Поля fact и budget относятся ко всему месяцу сотрудника в проекте и повторяются по темам. Суммировать их можно только на листе AHT.

Подтверждение требует минимум 20 наблюдений в тесте у сотрудника, 30 в истории, трёх коллег, положительного модельного отставания по обоим сравнениям и фактического разрыва минимум 20%. Для диагностики берём медиану медиан коллег. Для секунд берём медиану **средних** коллег. Сценарий: число однотемных эпизодов × положительная разность между средним сотрудника и этим ориентиром. Объём и состав обращений считаются постоянными. При переводе в деньги согласовать стоимость часа и способ учёта параллельных чатов. Обучение и фактический эффект требуют отдельной проверки.

Для выбранной группы сложи seconds по уникальным строкам детализации.

```python
with pd.ExcelWriter("operator_training.xlsx",engine="xlsxwriter",
                    engine_kwargs={"options":{"strings_to_formulas":False,"strings_to_urls":False}}) as writer:
    for name,table in sheets.items():
        export = table.copy()
        if "month" in export:
            export["month"] = export.month.astype(str)
        export.to_excel(writer,sheet_name=name,index=False)
        sheet = writer.sheets[name]
        sheet.freeze_panes(1,0)
        sheet.autofilter(0,0,len(export),len(export.columns)-1)
        for j,col in enumerate(export):
            width = max(len(col),export[col].astype(str).str.len().max() if len(export) else 0)
            sheet.set_column(j,j,min(65,width+2))
```
