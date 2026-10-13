import os
from datetime import date, timedelta

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from world import POISON, ny_end, utc  # noqa: E402

from pitlake.gold import WINDOW_DAYS, build_features, macro_features, price_features  # noqa: E402
from pitlake.leakage import (  # noqa: E402
    bar_closed,
    counterfactual,
    feature_after_source,
    filing_visibility,
)
from pitlake.pit import as_of  # noqa: E402
from pitlake.quality import FUNDAMENTALS_DATED_BY_PERIOD  # noqa: E402

FEATURES = (
    "entity STRING, feature STRING, feature_group STRING, event_time TIMESTAMP, "
    "knowable_at TIMESTAMP, value DOUBLE, source_table STRING, source_ref STRING"
)
SPINE = "entity STRING, as_of TIMESTAMP"
JUST_BEFORE = timedelta(microseconds=1)
Q1 = utc(2024, 12, 28)
A1, A3 = utc(2025, 1, 31, 21, 30), utc(2025, 2, 20, 15)
Q2_ACCEPTED = utc(2025, 5, 2, 20, 30)
POISON_ACCEPTED = utc(2025, 3, 1, 12)


def row(entity, name, event, knowable, value, ref):
    return (entity, name, "fundamentals", event, knowable, float(value), "t", ref)


def read(spark, rows, *probes):
    features = spark.createDataFrame(rows, FEATURES)
    spine = spark.createDataFrame(list(probes), SPINE)
    return {(r["entity"], r["as_of"], r["feature"]): r for r in as_of(features, spine).collect()}


def test_a_value_is_invisible_until_its_knowable_at(spark):
    rows = [row("AAPL", "revenue_q", Q1, A1, 124.3, "a1")]
    seen = read(spark, rows, ("AAPL", A1 - JUST_BEFORE), ("AAPL", A1))
    assert ("AAPL", A1 - JUST_BEFORE, "revenue_q") not in seen
    assert seen[("AAPL", A1, "revenue_q")]["value"] == 124.3


def test_the_latest_period_wins_over_a_later_comparative(spark):
    rows = [
        row("AAPL", "revenue_q", Q1, A1, 124.3, "a1"),
        row("AAPL", "revenue_q", utc(2023, 12, 30), A3, 119.575, "a4"),
    ]
    later = A3 + timedelta(days=1)
    (result,) = read(spark, rows, ("AAPL", later)).values()
    assert (result["value"], result["event_time"], result["source_ref"]) == (124.3, Q1, "a1")


def test_a_restatement_replaces_the_value_once_it_is_knowable(spark):
    rows = [
        row("AAPL", "revenue_q", Q1, A1, 124.3, "a1"),
        row("AAPL", "revenue_q", Q1, A3, 125.0, "a3"),
    ]
    seen = read(spark, rows, ("AAPL", A3 - JUST_BEFORE), ("AAPL", A3))
    assert seen[("AAPL", A3 - JUST_BEFORE, "revenue_q")]["value"] == 124.3
    assert seen[("AAPL", A3, "revenue_q")]["source_ref"] == "a3"
    assert seen[("AAPL", A3, "revenue_q")]["knowable_at"] == A3


def test_macro_features_reach_every_entity_once(spark):
    rows = [
        ("US", "unrate", "macro", utc(2024, 12, 1), A1, 4.1, "fred", "2025-01-10"),
        row("AAPL", "revenue_q", Q1, A1, 124.3, "a1"),
    ]
    seen = read(spark, rows, ("AAPL", A3), ("MSFT", A3), ("US", A3))
    assert sorted(k for k in seen if k[2] == "unrate") == [
        ("AAPL", A3, "unrate"),
        ("MSFT", A3, "unrate"),
        ("US", A3, "unrate"),
    ]
    assert ("MSFT", A3, "revenue_q") not in seen


def test_no_instant_no_value(spark):
    rows = [row("AAPL", "revenue_q", Q1, A1, 124.3, "a1")]
    assert read(spark, rows, ("AAPL", None)) == {}
    features = spark.createDataFrame(rows, FEATURES)
    with pytest.raises(TypeError, match="TIMESTAMP"):
        as_of(
            features,
            spark.createDataFrame([("AAPL", date(2025, 3, 1))], "entity STRING, as_of DATE"),
        )
    with pytest.raises(TypeError, match="TIMESTAMP"):
        as_of(
            features, spark.createDataFrame([("AAPL", "2025-03-01")], "entity STRING, as_of STRING")
        )
    with pytest.raises(ValueError, match="spine"):
        as_of(features, spark.createDataFrame([(A3,)], "at TIMESTAMP"))


@pytest.fixture(scope="module")
def features(spark, world):
    return build_features(spark, world["bars_1d"], world["facts"], world["vintages"]).cache()


def instants(world):
    known = [
        r[0]
        for name in ("facts", "vintages")
        for r in world[name].select("knowable_at").distinct().collect()
    ]
    days = world["days"]
    known += [ny_end(days[WINDOW_DAYS]), ny_end(days[-1])]
    return sorted({k - JUST_BEFORE for k in known})


def rebuild(spark, world, fault=None):
    def known(name, instant):
        return world[name].where(world[name]["knowable_at"] <= instant)

    def at(instant):
        args = (known("bars_1d", instant), known("facts", instant), known("vintages", instant))
        return build_features(spark, *args, **({"fault": fault} if fault else {}))

    return at


def minute_bars(spark, rows=()):
    return spark.createDataFrame(
        list(rows), "venue STRING, symbol STRING, bar_time TIMESTAMP, knowable_at TIMESTAMP"
    )


def test_the_poisoned_filing_never_appears_before_it_was_accepted(spark, world, features):
    days = [utc(2024, 11, 1, 12) + timedelta(days=i) for i in range(192)]
    probes = [*days, POISON_ACCEPTED - JUST_BEFORE, POISON_ACCEPTED]
    spine = spark.createDataFrame(
        [(e, t) for e in ("AAPL", "MSFT", "BTCUSDT") for t in probes], SPINE
    )
    poisoned = [r["as_of"] for r in as_of(features, spine).where(f"value = {POISON}").collect()]
    assert poisoned, "the poison must become visible once accepted, or the test proves nothing"
    assert min(poisoned) == POISON_ACCEPTED


def test_a_companys_results_are_invisible_before_their_filing(spark, features):
    spine = spark.createDataFrame(
        [("AAPL", Q2_ACCEPTED - JUST_BEFORE), ("AAPL", Q2_ACCEPTED)], SPINE
    )
    seen = {(r["as_of"], r["feature"]): r for r in as_of(features, spine).collect()}
    before = seen[(Q2_ACCEPTED - JUST_BEFORE, "revenue_q")]
    assert before["event_time"] == Q1 and before["value"] == POISON
    assert seen[(Q2_ACCEPTED - JUST_BEFORE, "assets")]["event_time"] == Q1
    after = seen[(Q2_ACCEPTED, "revenue_q")]
    assert (after["event_time"], after["value"]) == (utc(2025, 3, 29), 95.359)


def test_the_clean_pipeline_has_no_leakage_cases(spark, world, features):
    results = bar_closed(minute_bars(spark), world["bars_1d"])
    results += feature_after_source(features, world["facts"], world["filings"], world["bars_1d"])
    visibility, filed = filing_visibility(features, world["facts"], world["filings"])
    results.append(filed)
    results += counterfactual(features, rebuild(spark, world), instants(world))
    assert [(r.check, r.subject, r.cases) for r in results if r.cases] == []
    assert all(r.examined for r in results if r.subject != "bars_1m")

    rows = {r["accession_number"]: r for r in visibility.collect()}
    assert len(rows) == 6
    assert all(r["features_before"] == 0 and r["features_at"] > 0 for r in rows.values())
    q2 = rows["0000320193-25-000057"]
    assert (q2["knowable_from"], q2["knowable_basis"], q2["lead_days"]) == (
        Q2_ACCEPTED,
        "acceptance_time",
        34,
    )
    assert rows["0000320193-24-000123"]["knowable_basis"] == "end_of_filing_date"


def test_dating_fundamentals_by_period_end_is_caught(spark, world):
    biased = build_features(
        spark, world["bars_1d"], world["facts"], world["vintages"], FUNDAMENTALS_DATED_BY_PERIOD
    )
    sources = feature_after_source(biased, world["facts"], world["filings"], world["bars_1d"])
    assert {r.subject: r.cases > 0 for r in sources} == {
        "prices": False,
        "fundamentals": True,
        "macro": False,
    }
    visibility, filed = filing_visibility(biased, world["facts"], world["filings"])
    assert filed.cases > 0
    leaked = {r["accession_number"] for r in visibility.where("features_before > 0").collect()}
    assert "0000320193-25-000057" in leaked
    (judged,) = counterfactual(
        biased,
        rebuild(spark, world, FUNDAMENTALS_DATED_BY_PERIOD),
        [Q2_ACCEPTED - JUST_BEFORE],
    )
    assert judged.cases > 0 and "95.359" in judged.sample


def test_bars_known_before_they_close_are_caught(spark, world):
    opened = minute_bars(
        spark,
        [("binance", "BTCUSDT", utc(2025, 1, 2, 0, m), utc(2025, 1, 2, 0, m)) for m in range(3)],
    )
    at_the_bell = world["bars_1d"].selectExpr(
        "venue", "symbol", "session_date", "knowable_at - INTERVAL 3 HOURS AS knowable_at"
    )
    minute, day = bar_closed(opened, at_the_bell)
    assert (minute.cases, minute.examined) == (3, 3)
    assert day.cases == day.examined == 2 * len(world["days"])


def test_macro_dated_by_observation_is_caught(spark, world):
    from pyspark.sql import functions as F

    dated = macro_features(world["vintages"]).withColumn("knowable_at", F.col("event_time"))
    (_, _, macro) = feature_after_source(dated, world["facts"], world["filings"], world["bars_1d"])
    assert macro.subject == "macro" and macro.cases == macro.examined > 0


def test_a_lookahead_price_feature_is_caught_by_the_counterfactual(spark, world):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    def peeking(bars):
        following = Window.partitionBy("entity", "feature").orderBy("event_time")
        return (
            price_features(bars)
            .withColumn("value", F.lead("value").over(following))
            .where(F.col("value").isNotNull())
        )

    bars = world["bars_1d"]
    instant = ny_end(world["days"][10]) + timedelta(hours=1)
    (judged,) = counterfactual(
        peeking(bars), lambda t: peeking(bars.where(bars["knowable_at"] <= t)), [instant]
    )
    assert judged.cases > 0
    (honest,) = counterfactual(
        price_features(bars),
        lambda t: price_features(bars.where(bars["knowable_at"] <= t)),
        [instant],
    )
    assert honest.cases == 0
