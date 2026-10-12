from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from pitlake.datasets import DATASETS
from pitlake.jobs.commit_transform import commit_rows, latest_silver_commits_query, silver_tables
from pitlake.jobs.plan_transform import group_inputs, rule_rows
from pitlake.jobs.transform_silver import (
    log_rows,
    parse_args,
    pending_files_query,
    shared_predicate,
    symbol_predicate,
)
from pitlake.quality import (
    DROP,
    DUPLICATE_KEY,
    HALT,
    RECORD,
    Rule,
    fault_target_sql,
    first_failure_sql,
    halt_violations,
)
from pitlake.silver import (
    BINANCE_SPOT_TRADES_SILVER,
    EDGAR_COMPANY_FACTS_SILVER,
    EDGAR_FILINGS_SILVER,
    SILVER_SPECS,
    Column,
    Lookup,
    from_epoch,
    get_spec,
    ny_midnight_after,
    transform_groups,
)
from pitlake.tables import (
    quarantine_ddl,
    rejection_rates_view,
    sequence_gaps_ddl,
    silver_ddl,
)

NOW = datetime(2026, 10, 12, 7, 0, tzinfo=UTC)
SPECS = list(SILVER_SPECS.values())
SHARED_PREFIX = (
    ("record_parses", DROP),
    ("source_registered", HALT),
    ("timestamp_plausible", DROP),
    ("knowable_at_present", HALT),
    ("knowable_at_not_before_event", HALT),
)


def test_every_bronze_dataset_has_a_silver_table():
    assert set(SILVER_SPECS) == set(DATASETS)
    assert all(s.table == s.dataset.bronze_table for s in SPECS)


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.table)
def test_rules_start_with_the_shared_rules_and_end_with_duplicates(spec):
    rules = spec.rules
    assert tuple((r.name, r.action) for r in rules[: len(SHARED_PREFIX)]) == SHARED_PREFIX
    judged = [r for r in rules if r.action != RECORD]
    assert judged[-1].name == DUPLICATE_KEY
    assert len({r.name for r in rules}) == len(rules)


def test_only_sequences_are_record_only():
    record = {s.table for s in SPECS for r in s.rules if r.action == RECORD}
    assert record == {"binance_spot_trades", "coinbase_spot_candles_1m"}


def test_first_failure_judges_rules_in_order_and_skips_group_rules():
    sql = first_failure_sql(BINANCE_SPOT_TRADES_SILVER.rules)
    names = [r.name for r in BINANCE_SPOT_TRADES_SILVER.rules if r.action != RECORD]
    names.remove(DUPLICATE_KEY)
    positions = [sql.index(f"THEN '{n}'") for n in names]
    assert positions == sorted(positions)
    assert DUPLICATE_KEY not in sql and "sequence_continuous" not in sql
    assert sql.startswith("CASE WHEN NOT coalesce((_corrupt_record IS NULL), false)")


def test_halt_violations_counts_only_halt_rules_that_failed():
    rules = BINANCE_SPOT_TRADES_SILVER.rules
    counts = {None: 10, "price_and_quantity_positive": 3, "knowable_at_present": 1}
    assert halt_violations(rules, counts) == {"knowable_at_present": 1}
    assert halt_violations(rules, {None: 5, "knowable_at_not_before_event": 0}) == {}


def test_a_fault_only_hits_rows_that_reach_the_knowable_at_rules():
    sql = fault_target_sql(BINANCE_SPOT_TRADES_SILVER.rules)
    assert "_corrupt_record IS NULL" in sql and "(_registered)" in sql
    assert "event_time >= CAST(_period_start AS TIMESTAMP)" in sql
    assert "knowable_at" not in sql


def test_edgar_facts_run_after_the_filings_they_look_up():
    groups = transform_groups(SPECS)
    assert [s.table for s in groups[-1]] == ["edgar_filings", "edgar_company_facts"]
    assert all(len(g) == 1 for g in groups[:-1])
    assert group_inputs(SPECS)[-1] == "edgar.filings,edgar.company_facts"


def test_a_run_can_be_limited_to_some_tables():
    assert group_inputs(SPECS, "binance.spot_trades") == ["binance.spot_trades"]
    assert group_inputs(SPECS, "edgar.company_facts, fred.series_vintages") == [
        "fred.series_vintages",
        "edgar.company_facts",
    ]
    assert group_inputs(SPECS, "alpaca.stock_trades  edgar.filings") == [
        "alpaca.stock_trades",
        "edgar.filings",
    ]
    with pytest.raises(ValueError, match="no Silver spec"):
        group_inputs(SPECS, "binance.nope")


def test_a_lookup_must_come_first_and_exist():
    with pytest.raises(ValueError, match="must be listed before"):
        transform_groups([EDGAR_COMPANY_FACTS_SILVER, EDGAR_FILINGS_SILVER])
    with pytest.raises(ValueError, match="has no spec"):
        transform_groups([EDGAR_COMPANY_FACTS_SILVER])


def test_spec_validation():
    base = BINANCE_SPOT_TRADES_SILVER
    with pytest.raises(ValueError, match="not a Silver column"):
        replace(base, key=("symbol", "nope"))
    with pytest.raises(ValueError, match="set by event_time"):
        replace(base, columns=(*base.columns, Column("knowable_at", "TIMESTAMP", "now()")))
    with pytest.raises(ValueError, match="scope"):
        replace(base, scope="day")
    with pytest.raises(ValueError, match="optional_key"):
        replace(base, optional_key=("price",))
    with pytest.raises(ValueError, match="unique"):
        replace(base, checks=(Rule("key_present", "true", DROP, "again"),))
    lookup = Lookup("x", (("symbol", "symbol"),), (Column("price", "STRING", "p"),))
    with pytest.raises(ValueError, match="repeated columns"):
        replace(base, lookups=(lookup,))


def test_rule_validation():
    with pytest.raises(ValueError, match="action"):
        Rule("r", "true", "warn", "d")
    with pytest.raises(ValueError, match="explain"):
        Rule("r", "true", DROP, " ")
    with pytest.raises(ValueError, match="invalid rule name"):
        Rule("Bad Name", "true", DROP, "d")


def test_instant_facts_may_lack_a_period_start():
    key_rule = next(r for r in EDGAR_COMPANY_FACTS_SILVER.rules if r.name == "key_present")
    assert "period_start" not in key_rule.condition
    assert "`period_end` IS NOT NULL" in key_rule.condition


def test_time_helpers():
    assert from_epoch("t", "micros") == "timestamp_micros(t)"
    assert "BETWEEN -9000000000000000 AND 9000000000000000" in from_epoch("t", "millis")
    assert ny_midnight_after("d") == (
        "to_utc_timestamp(CAST(date_add(d, 1) AS TIMESTAMP), 'America/New_York')"
    )


def test_silver_ddl_types_every_column_and_traces_rows_to_files():
    ddl = silver_ddl("pitlake_dev", BINANCE_SPOT_TRADES_SILVER)
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS `pitlake_dev`.`silver`.`binance_spot_trades`")
    assert "`price` DECIMAL(28,10)" in ddl and "`knowable_at` TIMESTAMP" in ddl
    assert "_source_file STRING" in ddl and "CLUSTER BY (symbol, event_time)" in ddl


def test_shared_tables_are_partitioned_by_silver_table():
    for ddl in (quarantine_ddl("pitlake_dev"), sequence_gaps_ddl("pitlake_dev")):
        assert "PARTITIONED BY (silver_table)" in ddl
    view = rejection_rates_view("pitlake_prod")
    assert "CREATE OR REPLACE VIEW `pitlake_prod`.`silver`.`rejection_rates`" in view
    assert "WHERE r.action = 'drop'" in view


def test_pending_files_read_the_committed_bronze_version():
    query, params = pending_files_query(
        "pitlake_dev", BINANCE_SPOT_TRADES_SILVER, 12, date(2025, 1, 1), None, False, 30
    )
    assert "`pitlake_dev`.`bronze`.`binance_spot_trades`\n            VERSION AS OF 12" in query
    assert "LEFT ANTI JOIN `pitlake_dev`.`control`.`silver_load_log`" in query
    assert query.rstrip().endswith("LIMIT 30")
    assert params == {"silver_table": "binance_spot_trades", "period_start": date(2025, 1, 1)}


def test_reprocess_skips_the_log_and_symbol_scope_is_never_cut_short():
    query, params = pending_files_query(
        "pitlake_dev", EDGAR_FILINGS_SILVER, 3, date(2025, 1, 1), date(2025, 3, 31), True, 30
    )
    assert "ANTI JOIN" not in query and "LIMIT" not in query
    assert "silver_table" not in params


def test_parse_args():
    args = parse_args(["--catalog", "pitlake_dev", "--datasets", "edgar.company_facts"])
    assert [s.table for s in args.specs] == ["edgar_company_facts"]
    args = parse_args(
        ["--catalog", "pitlake_dev", "--datasets", "edgar.company_facts, edgar.filings"]
    )
    assert [s.table for s in args.specs] == ["edgar_filings", "edgar_company_facts"]
    assert len(parse_args(["--catalog", "pitlake_dev"]).specs) == len(SPECS)
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_dev", "--reprocess", "true"])
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_dev", "--inject-fault", "knowable_at_missing"])
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_dev", "--inject-fault", "something_else"])
    args = parse_args(
        [
            "--catalog",
            "pitlake_dev",
            "--reprocess",
            "true",
            "--period-start",
            "2025-01-02",
            "--period-end",
            "2025-01-02",
            "--inject-fault",
            "knowable_at_before_event",
        ]
    )
    assert args.reprocess and args.inject_fault == "knowable_at_before_event"
    with pytest.raises(ValueError, match="no Silver spec"):
        get_spec("binance.nope")


def test_predicates_quote_values():
    assert symbol_predicate(["AAPL", "O'X"]) == "_symbol IN ('AAPL', 'O\\'X')"
    assert shared_predicate(BINANCE_SPOT_TRADES_SILVER, "_source_file IN ('a')") == (
        "silver_table = 'binance_spot_trades' AND (_source_file IN ('a'))"
    )


def test_log_rows_add_up_per_file():
    day = date(2025, 1, 2)
    counts = [
        ("f1", "s1", "BTCUSDT", day, None, 90),
        ("f1", "s1", "BTCUSDT", day, "price_and_quantity_positive", 7),
        ("f1", "s1", "BTCUSDT", day, "duplicate_key", 3),
        ("f2", "s2", "BTCUSDT", day, None, 50),
    ]
    rows = log_rows(BINANCE_SPOT_TRADES_SILVER, counts, {"f1": (2, 5)}, 4, "77", NOW)
    assert rows[0] == (
        "binance_spot_trades",
        "f1",
        "s1",
        "BTCUSDT",
        day,
        4,
        100,
        90,
        10,
        {"duplicate_key": 3, "price_and_quantity_positive": 7},
        2,
        5,
        NOW,
        "77",
    )
    assert rows[1][6:12] == (50, 50, 0, {}, 0, 0)


def test_rule_rows_keep_the_order_rows_are_judged():
    rows = rule_rows(SPECS, NOW)
    binance = [r for r in rows if r[0] == "binance_spot_trades"]
    assert [r[1] for r in binance] == list(range(1, len(binance) + 1))
    assert binance[0][2:4] == ("record_parses", DROP)
    assert all(r[5] for r in rows)


def test_silver_commit_covers_every_table_or_none():
    tables = silver_tables()
    assert tables[-2:] == ["quarantine", "sequence_gaps"]
    versions = {t: i for i, t in enumerate(tables)}
    rows = commit_rows(tables, versions, {"binance_spot_trades": (5, 90, 10)}, "9", NOW)
    assert rows[0] == ("9", NOW, "binance_spot_trades", 0, 5, 90, 10)
    assert rows[-1][4:] == (0, 0, 0)
    with pytest.raises(RuntimeError, match="partial commit"):
        commit_rows(tables, {"quarantine": 1}, {}, "9", NOW)
    assert "PARTITION BY silver_table ORDER BY committed_at DESC" in latest_silver_commits_query(
        "pitlake_prod"
    )
