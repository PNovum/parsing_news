# Профиль данных для настройки синтетики.
# Запускать после ячейки, где собран df_model. Выводит только агрегаты: без логинов, ФИО и названий тем.
import json
import numpy as np
import pandas as pd


def q(s, qs=(0.1, 0.25, 0.5, 0.75, 0.9)):
    s = pd.Series(s).dropna()
    return {str(k): round(float(v), 3) for k, v in s.quantile(list(qs)).items()} if len(s) else None


def profile(df_model):
    d = df_model.loc[df_model["handle_duration"].gt(0)].copy()
    d["topics"] = d["topics"].map(lambda seq: list(dict.fromkeys(seq)))
    d["k"] = d["topics"].str.len()
    d["month"] = d["seg_start"].dt.to_period("M")
    d["logd"] = np.log(d["handle_duration"])
    out = {}

    for ch, c in d.groupby("bl_type"):
        r = {}
        # --- объёмы
        r["rows"] = len(c)
        r["months"] = int(c["month"].nunique())
        r["operators"] = int(c["login"].nunique())
        r["projects"] = int(c["med_name"].nunique())
        r["project_share"] = sorted(c["med_name"].value_counts(normalize=True).round(3).tolist(), reverse=True)
        per_op_month = c.groupby(["login", "month"]).size()
        r["episodes_per_operator_month"] = q(per_op_month)
        r["share_ops_multi_project"] = round(float(c.groupby("login")["med_name"].nunique().gt(1).mean()), 3)
        r["n_topics_share"] = c["k"].clip(upper=3).value_counts(normalize=True).sort_index().round(3).to_dict()

        # --- темы
        single = c[c["k"].eq(1)].assign(topic=lambda x: x["topics"].str[0])
        all_topics = c[["topics"]].explode("topics")["topics"]
        vc = all_topics.value_counts()
        r["unique_topics"] = int(len(vc))
        r["unique_level1"] = int(all_topics.map(lambda t: t[0]).nunique())
        r["unique_level2"] = int(all_topics.map(lambda t: t[:2]).nunique())
        r["top_topic_share"] = {n: round(float(vc.head(n).sum() / vc.sum()), 3) for n in (10, 50, 100, 300)}
        r["topics_with_ge_episodes"] = {n: int((vc >= n).sum()) for n in (100, 1000, 10000)}
        two = c[c["k"].eq(2)]
        r["two_topic_same_level1"] = round(float(two["topics"].map(lambda t: t[0][0] == t[1][0]).mean()), 3) if len(two) else None

        # --- длительность (лог-шкала)
        r["duration_sec"] = q(c["handle_duration"], (0.05, 0.25, 0.5, 0.75, 0.95, 0.99))
        r["log_sd_all"] = round(float(c["logd"].std()), 3)
        tp = single.groupby(["topic", "med_name"])["logd"]
        stats = tp.agg(["median", "std", "size"])
        big = stats[stats["size"] >= 50]
        r["noise_sd_within_topic_project"] = q(big["std"])
        topic_med = single.groupby("topic")["logd"].agg(["median", "size"]).query("size >= 50")["median"]
        r["topic_effect_sd"] = round(float(topic_med.std()), 3)
        proj_med = single.groupby("med_name")["logd"].median()
        r["project_effect_sd"] = round(float(proj_med.std()), 3)
        spread = big["median"].groupby(level=0).std().dropna()
        r["topic_x_project_sd"] = q(spread)
        # хвост: доля эпизодов дольше 3х медианы своей темы×проекта
        med = single.groupby(["topic", "med_name"])["logd"].transform("median")
        r["share_over_3x_topic_median"] = round(float((single["logd"] - med > np.log(3)).mean()), 4)

        # --- остатки однотемных относительно тема×проект
        single = single.assign(res=single["logd"] - med)
        op = single.groupby("login")["res"].agg(["mean", "size"]).query("size >= 200")
        r["operator_effect_sd"] = round(float(op["mean"].std()), 3)
        single = single.join(op["mean"].rename("op_eff"), on="login")
        single["pair_res"] = single["res"] - single["op_eff"]
        pr = single.dropna(subset=["op_eff"]).groupby(["login", "topic"])["pair_res"].agg(["mean", "var", "size"])
        pr = pr[pr["size"] >= 30]
        raw = float(pr["mean"].var())
        noise = float((pr["var"] / pr["size"]).mean())
        r["pair_effect_sd_raw"] = round(raw ** 0.5, 3)
        r["pair_effect_sd_noise_corrected"] = round(max(raw - noise, 0) ** 0.5, 3)
        r["pairs_ge_30_single_episodes"] = int(len(pr))
        r["pair_effect_quantiles"] = q(pr["mean"], (0.5, 0.9, 0.95, 0.99))

        # устойчивость слабостей: первая половина периода против второй
        mid = c["seg_start"].min() + (c["seg_start"].max() - c["seg_start"].min()) / 2
        single["half"] = np.where(single["seg_start"] < mid, 1, 2)
        h = (single.dropna(subset=["op_eff"]).groupby(["login", "topic", "half"])["pair_res"]
             .agg(["mean", "size"]).query("size >= 15")["mean"].unstack("half").dropna())
        r["pair_effect_corr_half1_half2"] = round(float(h[1].corr(h[2])), 3) if len(h) > 30 else None
        r["pairs_in_both_halves"] = int(len(h))

        # --- стаж, сезонность, дрейф тем
        if "tenure_days" in single:
            b = pd.cut(single["tenure_days"], [-1, 30, 90, 180, 365, 730, 1e6],
                       labels=["<30", "30-90", "90-180", "180-365", "1-2y", "2y+"])
            r["residual_by_tenure"] = single.groupby(b, observed=True)["res"].median().round(3).to_dict()
            r["tenure_missing_share"] = round(float(c["tenure_days"].isna().mean()), 3)
            r["share_ops_hired_in_period"] = round(float(
                (c.groupby("login")["employment_date"].first() >= c["seg_start"].min()).mean()), 3)
        r["residual_by_month"] = {str(k): round(float(v), 3)
                                  for k, v in single.groupby("month")["res"].median().items()}
        tm = single.groupby(["topic", "half"])["logd"].agg(["median", "size"]).query("size >= 200")["median"].unstack().dropna()
        r["topic_drift_sd_half2_minus_half1"] = round(float((tm[2] - tm[1]).std()), 3) if len(tm) > 10 else None

        # --- двухтемные: неаддитивность
        if len(two):
            tmed = single.groupby(["topic", "med_name"])["handle_duration"].median()
            t1 = pd.MultiIndex.from_arrays([two["topics"].str[0], two["med_name"]])
            t2 = pd.MultiIndex.from_arrays([two["topics"].str[1], two["med_name"]])
            ratio = two["handle_duration"].to_numpy() / (tmed.reindex(t1).to_numpy() + tmed.reindex(t2).to_numpy())
            r["two_topic_duration_div_sum_of_single_medians"] = q(ratio)

        # --- норматив
        if "aht_target" in c:
            plan = pd.to_numeric(c["aht_target"], errors="coerce")
            r["plan_coverage"] = round(float(plan.gt(0).mean()), 3)
            r["plan_div_fact_median_by_project"] = q(
                (plan / c["handle_duration"]).groupby(c["med_name"]).median())
            r["plan_unique_values"] = int(plan.nunique())
        out[ch] = r
    return out


print(json.dumps(profile(df_model), ensure_ascii=False, indent=1, default=str))
