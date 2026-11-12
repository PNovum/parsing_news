# Темы для обучения операторов
Одна строка детализации — сотрудник × канал × проект × тема за последний закрытый месяц. Руководитель начинает с листа «Обучение», выбирает тему по объёму потенциального сокращения длительности и смотрит логины. Лист «Сотрудники и темы» объясняет отбор. Положительный gap_pct означает превышение плана. Пустое значение означает отсутствие норматива.

## Что показал запуск на 2 000 000 эпизодов
120 операторов, два канала, два проекта, январь–август 2025. У каждой операции два пути через lvl1. Для трёх операций одна ветка объективно дольше. Каждому сотруднику задана одна проблемная операция. Есть шум длительности, стаж, смена норматива и 5% трёхтемных эпизодов. Все числа в приложенном Excel синтетические.

|Канал|Лучшая MAE без lvl1, с|Лучшая MAE с lvl1, с|Выбор alpha с lvl1|Август, MAE в секундах|
|---|---:|---:|---:|---:|
|Чат ФЛ|148,69|148,39|10|148,38|
|Голос ФЛ|108,76|108,50|10|108,61|

Подбор по июлю, финальная проверка на августе. На уровне оператора × темы × ветки × канала алгоритм обнаружил 387 из 480 заложенных отставаний (81%). В выборке на обучение 509 строк, точность 76%. Это единичный временной срез. Порог +20% и минимальные объёмы — настройки отбора. Для запуска на реальных данных повторить проверку на нескольких закрытых месяцах.

**Решение.** Таргет — логарифм handle_duration. Норматив нужен для отдельного бизнес-показателя и пока доступен только для чата. Разница MAE между вариантами lvl1 мала; в генераторе зависимость от ветки частично заложена заранее. Поэтому код выбирает вариант заново на данных каждого канала. Если в двух ветках действительно одинаковое обучение, объединять их можно уже на листе «Обучение» по утверждённому справочнику «полный путь → учебный модуль». При объединении складывать секунды и заново считать уникальные логины.

## Как запустить в Jupyter
Пакеты: numpy, pandas, scipy, scikit-learn и xlsxwriter. Сначала выбрать **один** источник ниже. Затем выполнить общие ячейки по порядку. Для воспроизведения примера оставить end равным сентябрю 2025. Для рабочей базы обязательно поменять end — это первый месяц после последнего закрытого. Нужны история для обучения и два следующих полных месяца для проверки.

### Источник A. Рабочие данные
Используй SQL и ячейку загрузки из приложенного варианта с aht_target: его LEFT JOIN сохраняет голос. Начать со свежего df сразу после lake.read_dataframe, до фильтра по плану и темам. Единица наблюдения — готовый эпизод SQL. med_name — проект/технический канал, login — оператор. Проверить, что охват SQL соответствует официальному контуру KPI.

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
            return ()
        if any(levels):
            result.append(levels)
    return tuple(dict.fromkeys(result))
df["topics"] = df.theme_sequence.map(parse_topics)
```

### Источник B. Воспроизведение симуляции

```python
import numpy as np
import pandas as pd
rng = np.random.default_rng(42)
n = 2_000_000
i = np.arange(n)
login = rng.integers(0, 120, n)
month = i * 8 // n
chat = rng.random(n) < .55
t = rng.integers(0, 16, n)
branch = rng.integers(0, 2, n)
second = rng.integers(1, 16, n)
count = rng.choice([1, 2, 3], n, p=[.75, .20, .05])
catalog = [(f"Раздел {b}", f"Процесс {k//4}", f"Операция {k}") for b in range(2) for k in range(16)]
def effect(k):
    return .035*k + .42*(login % 16 == k) + .18*((k < 3) & (branch == 1))
k2 = (t + second) % 16
base = np.where(chat, 310, 225)
tenure = login % 30 * 9 + month * 30
duration = base * np.exp(np.where(count==1, effect(t), (effect(t)+effect(k2))/2)
    + .12*(count-1) + .08*np.sin(login) + .18*np.exp(-tenure/90)
    + rng.normal(0,.40,n))
from functools import lru_cache
@lru_cache(None)
def seq(b,k,j,c):
    ids = [k,j,next(v for v in range(16) if v not in (k,j))]
    return tuple(catalog[16*b+x] for x in ids[:c])
df = pd.DataFrame(dict(session_id=i, login=login, bl_type=pd.Categorical(np.where(chat,"Чат ФЛ","Голос ФЛ")),
    med_name=np.where(login%2,"Проект А","Проект Б"), seg_start=pd.Timestamp("2025-01-01")+pd.to_timedelta(i*243//n,unit="D"),
    tenure_days=tenure, topics=[seq(*x) for x in zip(branch,t,k2,count)], handle_duration=duration,
    aht_target=np.where(chat & (rng.random(n)>.02),480+60*(tenure<90)-30*(month>=4),np.nan)))
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

end = pd.Period("2025-09",freq="M")
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
def unique_topics(items):
    return tuple(dict.fromkeys(tuple(x) for x in items))
base["topics"] = base.topics.map(unique_topics)
sample = base[base.topics.str.len().isin([1,2]) & base.login.notna() & base.med_name.notna()].copy()
months = sample.groupby("session_id").month.transform("nunique")
sample = sample.loc[months.eq(1)]
```

## 2. Ridge отдельно для чата и голоса
Разреженная матрица содержит оператора, проект, тему, взаимодействие оператора с темой и числовые признаки. Вклад двух тем имеет веса ½. Словарь категорий построен на всём срезе для общей нумерации колонок; признаки без наблюдений в train имеют нулевой коэффициент. Тестовые длительности используются только после выбора параметров.

```python
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
        median = days[train].median()
        extra.extend([np.log1p(days.fillna(median if pd.notna(median) else 0)),days.isna()])
    blocks.append(csr_matrix(np.column_stack(extra)))
    return hstack(blocks,format="csr"),first

trials, details = [], []
for channel,d in sample.groupby("bl_type",observed=True):
    d = d.reset_index(drop=True)
    train = d.month.lt(end-2).to_numpy()
    val = d.month.eq(end-2).to_numpy()
    test = d.month.eq(end-1).to_numpy()
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
    single = d.topics.str.len().eq(1).to_numpy()
    pre = d.loc[(train|val)&single,["login","med_name"]].assign(topic=topic)
    pre["log_prediction"] = model.predict(X[(train|val)&single])
    ix = np.flatnonzero((train|val)&single)
    numeric = 3 if channel=="Чат ФЛ" else 1
    pre["adjusted"] = pre.log_prediction - X[ix,-numeric:] @ model.coef_[-numeric:]
    op_codes = pd.factorize(d.login)[0]
    pre["interaction"] = pre.adjusted - model.coef_[op_codes[ix]]
    group = ["med_name","topic","login"]
    scores = pre.groupby(group).agg(score=("adjusted","mean"),interaction=("interaction","mean"),train_n=("login","size")).reset_index()
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
                len(peers),peers["median"].median(),peers["mean"].median(),peers.score.median()]
        parts.append(part)
    out = pd.concat(parts)
    out["model_gap"] = np.expm1(out.score-out.peer_score)
    out["observed_gap"] = out["median"]/out.peer_median-1
    supported = out["size"].ge(20)&out.peer_n.ge(3)&out.train_n.ge(30)
    confirmed = supported & out.model_gap.gt(0)&out.within.gt(0)&out.observed_gap.ge(.20)
    out["status"] = np.select([~supported,confirmed],["Мало данных","Разобрать тему"],default="Без подтверждения")
    out["seconds"] = ((out["mean"]-out.peer_mean).clip(lower=0)*out["size"]).where(confirmed,0)
    out["month"],out["bl_type"] = end-1,channel
    details.append(out)
detail = pd.concat(details).merge(monthly,on=keys,how="left",validate="many_to_one")
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
assert np.isclose(training.seconds.sum(),detail.seconds.sum())
sheets = {"Обучение":training,"Сотрудники и темы":detail,"AHT по месяцам":monthly,"Выбор модели":pd.DataFrame(trials)}
```

## 3. Итоговый Excel
В детализации size — число однотемных эпизодов сотрудника, peer_n — число коллег с минимум десятью такими эпизодами. model_gap и observed_gap выражены в долях (0,20 = 20%). within — центрированный эффект темы в логарифмической шкале. score и peer_score — модельные оценки на логарифмической шкале. Поля fact и budget относятся ко всему месяцу сотрудника в проекте и повторяются по темам. Суммировать их можно только на листе AHT.

Подтверждение требует минимум 20 наблюдений в тесте у сотрудника, 30 в истории, трёх коллег, положительного модельного отставания по обоим сравнениям и фактического разрыва минимум 20%. Для диагностики берём медиану медиан коллег. Для секунд берём медиану **средних** коллег. Сценарий: число однотемных эпизодов × положительная разность между средним сотрудника и этим ориентиром. Объём и состав обращений считаются постоянными. При переводе в деньги согласовать стоимость часа и способ учёта параллельных чатов. Обучение и фактический эффект требуют отдельной проверки.

В синтетическом примере получено 633,16 часа суммарной длительности чатов и звонков. Это не прогноз высвобождения рабочего времени. Для оценки конкретной группы отфильтруй детализацию по каналу, проекту, теме и выбранным логинам, сложи seconds и раздели на 3600.

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
