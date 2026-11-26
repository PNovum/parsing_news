"""Проверки конвейера. Для интеграционных тестов используются данные текущего ядра Jupyter."""
import numpy as np
import pytest


def run_checks(stage, **data):
    from pathlib import Path
    test_file = Path(__file__).resolve()

    class Inputs:
        def pytest_configure(self, config):
            config.operator_topics_ctx = data

    result = pytest.main([str(test_file), "-q", "-k", stage], plugins=[Inputs()])
    if result != pytest.ExitCode.OK:
        raise RuntimeError(f"Проверка {stage} завершилась с кодом {result}")


@pytest.fixture
def ctx(pytestconfig):
    data = getattr(pytestconfig, "operator_topics_ctx", None)
    if data is None:
        pytest.skip("Интеграционный тест запускается из ячейки Jupyter")
    return data


def test_clean_sequence_offline():
    import ast
    import json
    from pathlib import Path
    import pandas as pd

    notebook = json.loads(Path(__file__).with_name("operator_topics.ipynb").read_text(encoding="utf-8"))
    cell = next("".join(c["source"]) for c in notebook["cells"]
                if c["cell_type"] == "code" and "def clean_sequence(" in "".join(c["source"]))
    nodes = [n for n in ast.parse(cell).body if
             isinstance(n, ast.FunctionDef) and n.name == "clean_sequence" or
             isinstance(n, ast.Assign) and ast.unparse(n.targets[0]) == "skip_topics"]
    scope = {"pd": pd}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<clean_sequence>", "exec"), scope)
    test_publish_cleaning({"clean": scope["clean_sequence"], "skip": scope["skip_topics"]})


def test_incoming_unique(ctx):
    frame, keys = ctx["frame"], ctx["keys"]
    assert not frame.groupBy(*keys).count().where("count > 1").take(1), "Дубли после SQL JOIN"


def test_history_unique(ctx):
    assert not ctx["frame"].duplicated(ctx["keys"]).any(), "Дубли в сохранённой истории"


def test_publish_month(ctx):
    frame, date = ctx["fact"], ctx["date"]
    assert date.is_month_end
    assert not frame.empty
    assert frame["report_month"].isin([date.to_period("M").start_time.date()]).all()


def test_publish_allocations(ctx):
    frame, keys = ctx["fact"], ctx["keys"]
    assert not frame.duplicated(keys + ["level1", "level2", "level3"]).any()
    grouped = frame.groupby(keys, dropna=False)
    assert len(grouped) == len(ctx["test"])
    assert frame["allocation_weight"].between(0, 1).all()
    assert frame["topic_duration"].notna().all()
    np.testing.assert_allclose(grouped.allocation_weight.sum(), 1)
    np.testing.assert_allclose(grouped.topic_duration.sum(), grouped.handle_duration.first(), rtol=0, atol=0.001)


def test_publish_windows(ctx):
    train, test = ctx["train"], ctx["test"]
    assert train.seg_start.lt(ctx["start"]).all()
    assert test.seg_start.between(ctx["start"], ctx["end"], inclusive="left").all()
    assert not train.session_id.isin(test.session_id).any()
    assert set(test.bl_type) <= set(ctx["models"])


def test_saved_month(ctx):
    saved, expected = ctx["saved"], ctx["expected"]
    saved = saved.select(*expected.columns)
    assert not saved.exceptAll(expected).take(1), "В БД есть лишние или изменённые строки"
    assert not expected.exceptAll(saved).take(1), "В БД отсутствуют рассчитанные строки"


def test_report_unique(ctx):
    assert not ctx["frame"].duplicated(ctx["keys"]).any()


def test_publish_scores(ctx):
    train, models, score_table = ctx["train"], ctx["models"], ctx["scores"]
    pair_key = ctx["pair_key"]
    MIN_TRAIN_EPISODES, MIN_WITHIN_SCORE = ctx["thresholds"]
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

def test_publish_cleaning(ctx):
    clean = ctx["clean"]
    valid = ("Карты", "Доставка", "Срок")
    for topic in ctx["skip"]:
        kept, ignored = (None, *valid), (None, *topic)
        assert clean([ignored, kept]) == [valid]
        assert clean([kept, ignored, kept]) == [valid]
        assert clean([ignored]) == []


def test_pseudopairs_size(ctx):
    assert len(ctx["first"]) == len(ctx["pairs"])
