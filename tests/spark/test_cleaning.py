import os
from datetime import UTC, date, datetime

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.cleaning import (  # noqa: E402
    FAILED_RULE,
    RECORD,
    evaluate,
    prepare,
    quarantine_rows,
    sequence_gaps,
    silver_rows,
)
from pitlake.datasets import DATASETS  # noqa: E402
from pitlake.quality import (  # noqa: E402
    KNOWABLE_AT_BEFORE_EVENT,
    KNOWABLE_AT_MISSING,
    NO_FAULT,
    halt_violations,
)
from pitlake.silver import (  # noqa: E402
    ALPACA_STOCK_TRADES_SILVER,
    BINANCE_SPOT_TRADES_SILVER,
    COINBASE_SPOT_CANDLES_1M_SILVER,
    EDGAR_COMPANY_FACTS_SILVER,
    EDGAR_FILINGS_SILVER,
    FRED_SERIES_VINTAGES_SILVER,
    SILVER_SPECS,
)
from pitlake.tables import (  # noqa: E402
    BRONZE_METADATA_COLUMNS,
    CORRUPT_RECORD_COLUMN,
    raw_schema,
)

INGESTED = datetime(2026, 10, 12, 7, 0, tzinfo=UTC)
DAY = date(2025, 1, 2)
DAY_SECONDS = 1735776000
DAY_MICROS = DAY_SECONDS * 1_000_000
TS = "yyyy-MM-dd HH:mm:ss"


def bronze(spark, dataset, records, symbol, period_start=DAY, source_file=None):
    schema = ", ".join(
        [raw_schema(dataset), *(f"{name} {kind}" for name, kind in BRONZE_METADATA_COLUMNS)]
    )
    metadata = {
        "_source": dataset.source,
        "_dataset": dataset.name,
        "_symbol": symbol,
        "_period_start": period_start,
        "_source_file": source_file or f"{dataset.source}/{dataset.name}/{symbol}/{period_start}",
        "_source_sha256": "0" * 64,
        "_ingested_at": INGESTED,
        "_load_run_id": "1",
    }
    rows = [
        (
            *(r.get(c) for c in dataset.columns),
            r.get(CORRUPT_RECORD_COLUMN),
            *(metadata[name] for name, _ in BRONZE_METADATA_COLUMNS),
        )
        for r in records
    ]
    return spark.createDataFrame(rows, schema=schema)


@pytest.fixture(scope="module")
def registry(spark):
    return spark.createDataFrame(
        [(d.source, d.name) for d in DATASETS.values()], "source STRING, dataset STRING"
    )


def judge(spec, rows, registry, lookups=None, fault=NO_FAULT):
    return evaluate(spec, prepare(spec, rows, registry, lookups, fault))


def rule_counts(evaluated) -> dict:
    return {r[FAILED_RULE]: r["count"] for r in evaluated.groupBy(FAILED_RULE).count().collect()}


def trade(trade_id, price="100.5", qty="0.1", time=None):
    if time is None:
        time = str(DAY_MICROS + int(trade_id) * 1000)
    return {
        "trade_id": trade_id,
        "price": price,
        "qty": qty,
        "quote_qty": "10.05",
        "time": time,
        "is_buyer_maker": "True",
        "is_best_match": "False",
    }


def binance(spark, records):
    return bronze(spark, BINANCE_SPOT_TRADES_SILVER.dataset, records, "BTCUSDT")


def test_every_rule_rejects_what_it_should(spark, registry):
    rows = binance(
        spark,
        [
            trade("100"),
            trade("101"),
            trade("103"),
            trade("104", price="0"),
            trade("105", time=str(DAY_SECONDS * 1000)),
            {CORRUPT_RECORD_COLUMN: "not,a,trade"},
            trade("101"),
            trade("abc", time=str(DAY_MICROS + 5)),
        ],
    )
    evaluated = judge(BINANCE_SPOT_TRADES_SILVER, rows, registry)
    assert rule_counts(evaluated) == {
        None: 3,
        "price_and_quantity_positive": 1,
        "timestamp_plausible": 1,
        "record_parses": 1,
        "duplicate_key": 1,
        "key_present": 1,
    }
    assert halt_violations(BINANCE_SPOT_TRADES_SILVER.rules, rule_counts(evaluated)) == {}

    kept = evaluated.where(f"{FAILED_RULE} IS NULL").orderBy("trade_id")
    assert [r["trade_id"] for r in kept.collect()] == [100, 101, 103]
    first = kept.selectExpr(
        f"date_format(event_time, '{TS}.SSS') AS e", f"date_format(knowable_at, '{TS}.SSS') AS k"
    ).first()
    assert first["e"] == first["k"] == "2025-01-02 00:00:00.100"
    assert kept.where(f"{RECORD} IS NOT NULL").count() == 0

    corrupt = evaluated.where(f"{FAILED_RULE} = 'record_parses'").first()
    assert "not,a,trade" in corrupt[RECORD]

    quarantined = quarantine_rows(BINANCE_SPOT_TRADES_SILVER, evaluated, 7, "1", INGESTED)
    assert quarantined.count() == 5
    assert silver_rows(BINANCE_SPOT_TRADES_SILVER, evaluated, 7, "1", INGESTED).count() == 3


def test_a_trade_id_gap_is_recorded_and_the_trades_kept(spark, registry):
    rows = binance(spark, [trade("100"), trade("101"), trade("103"), trade("107")])
    evaluated = judge(BINANCE_SPOT_TRADES_SILVER, rows, registry)
    gaps = sequence_gaps(BINANCE_SPOT_TRADES_SILVER, evaluated, "1", INGESTED)
    found = sorted((r["gap_start"], r["gap_end"], r["missing"]) for r in gaps.collect())
    assert found == [(102, 102, 1), (104, 106, 3)]
    assert rule_counts(evaluated) == {None: 4}


@pytest.mark.parametrize(
    ("fault", "rule"),
    [
        (KNOWABLE_AT_MISSING, "knowable_at_present"),
        (KNOWABLE_AT_BEFORE_EVENT, "knowable_at_not_before_event"),
    ],
)
def test_an_injected_knowable_at_fault_halts(spark, registry, fault, rule):
    rows = binance(spark, [trade("100"), trade("101"), trade("102")])
    evaluated = judge(BINANCE_SPOT_TRADES_SILVER, rows, registry, fault=fault)
    counts = rule_counts(evaluated)
    assert counts == {None: 2, rule: 1}
    assert halt_violations(BINANCE_SPOT_TRADES_SILVER.rules, counts) == {rule: 1}


def test_an_unregistered_source_halts(spark, registry):
    rows = binance(spark, [trade("100"), trade("101")])
    nothing = registry.where("false")
    counts = rule_counts(judge(BINANCE_SPOT_TRADES_SILVER, rows, nothing))
    assert halt_violations(BINANCE_SPOT_TRADES_SILVER.rules, counts) == {"source_registered": 2}


def test_fred_vintage_is_knowable_at_the_end_of_its_release_day(spark, registry):
    def observation(day, value, vintage="2025-02-07"):
        return {"realtime_start": vintage, "realtime_end": vintage, "date": day, "value": value}

    dataset = FRED_SERIES_VINTAGES_SILVER.dataset
    rows = bronze(
        spark,
        dataset,
        [
            observation("2025-01-01", "4.0"),
            observation("2024-11-01", "."),
            observation("2024-12-01", "4.1", vintage=None),
        ],
        "UNRATE",
        period_start=date(2025, 2, 7),
    )
    evaluated = judge(FRED_SERIES_VINTAGES_SILVER, rows, registry)
    counts = rule_counts(evaluated)
    assert counts == {None: 1, "value_reported": 1, "knowable_at_present": 1}
    assert halt_violations(FRED_SERIES_VINTAGES_SILVER.rules, counts) == {"knowable_at_present": 1}
    kept = evaluated.where(f"{FAILED_RULE} IS NULL")
    assert kept.selectExpr(f"date_format(knowable_at, '{TS}')").first()[0] == (
        "2025-02-08 05:00:00"
    )


def _fact(accn, end, val, filed, start=None):
    return {
        "taxonomy": "us-gaap",
        "concept": "Revenues",
        "unit": "USD",
        "start": start,
        "end": end,
        "val": val,
        "accn": accn,
        "fy": "2025",
        "fp": "Q1",
        "form": "10-Q",
        "filed": filed,
        "frame": None,
    }


def test_facts_are_knowable_at_acceptance_and_repeats_are_dropped(spark, registry):
    dataset = EDGAR_COMPANY_FACTS_SILVER.dataset
    quarter = _fact("X", "2024-12-28", "124300000000", "2025-01-31", start="2024-09-29")
    old = _fact("OLD", "2010-09-25", "1000", "2010-10-27")
    first = bronze(
        spark, dataset, [quarter, old], "AAPL", date(2025, 1, 31), "edgar/company_facts/AAPL/a"
    )
    second = bronze(
        spark,
        dataset,
        [
            quarter,
            _fact("Y", "2025-03-29", "abc", "2025-05-02"),
            _fact("Z", "2025-04-27", "0.26", "2025-01-31", start="2025-01-27"),
        ],
        "AAPL",
        date(2025, 5, 2),
        "edgar/company_facts/AAPL/b",
    )
    filings = spark.createDataFrame(
        [("AAPL", "X", datetime(2025, 1, 31, 11, 1, tzinfo=UTC))],
        "symbol STRING, accession_number STRING, acceptance_time TIMESTAMP",
    )
    evaluated = judge(
        EDGAR_COMPANY_FACTS_SILVER,
        first.unionByName(second),
        registry,
        lookups={"edgar_filings": filings},
    )
    result = {
        (r["accession_number"], r["_source_file"][-1]): (r[FAILED_RULE], r["basis"], r["k"])
        for r in evaluated.selectExpr(
            "accession_number",
            "_source_file",
            FAILED_RULE,
            "knowable_basis AS basis",
            f"date_format(knowable_at, '{TS}') AS k",
        ).collect()
    }
    assert result == {
        ("X", "a"): (None, "acceptance_time", "2025-01-31 11:01:00"),
        ("OLD", "a"): (None, "end_of_filing_date", "2010-10-28 04:00:00"),
        ("X", "b"): ("duplicate_key", "acceptance_time", "2025-01-31 11:01:00"),
        ("Y", "b"): ("value_numeric", "end_of_filing_date", "2025-05-03 04:00:00"),
        ("Z", "b"): ("timestamp_plausible", "end_of_filing_date", "2025-02-01 05:00:00"),
    }


def test_missing_coinbase_minutes_are_recorded_to_the_end_of_the_day(spark, registry):
    def candle(offset):
        return {
            "time": str(DAY_SECONDS + offset),
            "low": "0.5",
            "high": "2",
            "open": "1",
            "close": "1.5",
            "volume": "3",
        }

    dataset = COINBASE_SPOT_CANDLES_1M_SILVER.dataset
    rows = bronze(spark, dataset, [candle(0), candle(60), candle(180)], "BTC-USD")
    evaluated = judge(COINBASE_SPOT_CANDLES_1M_SILVER, rows, registry)
    assert rule_counts(evaluated) == {None: 3}
    gaps = sequence_gaps(COINBASE_SPOT_CANDLES_1M_SILVER, evaluated, "1", INGESTED)
    found = sorted((r["gap_start"], r["gap_end"], r["missing"]) for r in gaps.collect())
    assert found == [
        (DAY_SECONDS + 120, DAY_SECONDS + 120, 1),
        (DAY_SECONDS + 240, DAY_SECONDS + 86340, 1436),
    ]
    knowable = evaluated.selectExpr(f"date_format(min(knowable_at), '{TS}')").first()[0]
    assert knowable == "2025-01-02 00:01:00"


def test_alpaca_nanosecond_times_parse_to_microseconds(spark, registry):
    dataset = ALPACA_STOCK_TRADES_SILVER.dataset
    record = {
        "t": "2025-01-02T14:30:00.123456789Z",
        "x": "V",
        "p": "243.5",
        "s": "100",
        "c": '["@","I"]',
        "i": "52983525029461",
        "z": "C",
    }
    evaluated = judge(
        ALPACA_STOCK_TRADES_SILVER, bronze(spark, dataset, [record], "AAPL"), registry
    )
    row = evaluated.selectExpr(
        FAILED_RULE, f"date_format(event_time, '{TS}.SSSSSS') AS e", "conditions"
    ).first()
    assert row[FAILED_RULE] is None
    assert row["e"] == "2025-01-02 14:30:00.123456"
    assert row["conditions"] == ["@", "I"]


def _types(df) -> dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in df.schema.fields}


def test_every_spec_produces_its_declared_schema(spark, registry):
    lookups = {}
    for spec in SILVER_SPECS.values():
        rows = bronze(spark, spec.dataset, [], "X")
        for fault in (NO_FAULT, KNOWABLE_AT_MISSING):
            evaluated = judge(spec, rows, registry, lookups, fault)
        output = silver_rows(spec, evaluated, 1, "1", INGESTED)
        expected = {c.name: c.type.lower().replace(" ", "") for c in spec.output_columns}
        assert {k: v for k, v in _types(output).items() if k in expected} == expected
        assert quarantine_rows(spec, evaluated, 1, "1", INGESTED).columns[:2] == [
            "silver_table",
            "rule",
        ]
        if spec.sequence is not None:
            assert "missing" in sequence_gaps(spec, evaluated, "1", INGESTED).columns
        if spec is EDGAR_FILINGS_SILVER:
            lookups["edgar_filings"] = output
