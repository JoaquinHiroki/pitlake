import re
from dataclasses import dataclass

from pitlake.config import validate_identifier
from pitlake.datasets import (
    ALPACA_STOCK_BARS_1D,
    ALPACA_STOCK_TRADES,
    BINANCE_SPOT_TRADES,
    COINBASE_SPOT_CANDLES_1M,
    EDGAR_COMPANY_FACTS,
    EDGAR_FILING_DOCUMENTS,
    EDGAR_FILINGS,
    FRED_SERIES_VINTAGES,
    Dataset,
)
from pitlake.quality import (
    DROP,
    KNOWABLE_AT_NOT_BEFORE_EVENT,
    KNOWABLE_AT_PRESENT,
    RECORD_PARSES,
    SOURCE_REGISTERED,
    Rule,
    duplicate_key,
    key_present,
    sequence_continuous,
    timestamp_plausible,
    validate_rules,
)

SCOPES = ("file", "symbol")
NEW_YORK = "America/New_York"
PRICE = "DECIMAL(28,10)"
TIMING_COLUMNS = ("event_time", "knowable_at")
MONITOR_GRANULARITIES = ("1 day", "1 week")

SILVER_METADATA_COLUMNS = (
    ("_symbol", "STRING"),
    ("_period_start", "DATE"),
    ("_source_file", "STRING"),
    ("_source_sha256", "STRING"),
    ("_bronze_version", "BIGINT"),
    ("_transformed_at", "TIMESTAMP"),
    ("_transform_run_id", "STRING"),
)


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    expr: str

    def __post_init__(self) -> None:
        validate_identifier(self.name)


@dataclass(frozen=True)
class Lookup:
    table: str
    on: tuple[tuple[str, str], ...]
    columns: tuple[Column, ...]


@dataclass(frozen=True)
class Sequence:
    value: str
    step: int
    description: str
    first: str = ""
    last: str = ""


@dataclass(frozen=True)
class SilverSpec:
    dataset: Dataset
    columns: tuple[Column, ...]
    event_time: str
    knowable_at: str
    plausible: str
    plausible_description: str
    key: tuple[str, ...]
    cluster_by: tuple[str, ...]
    description: str
    checks: tuple[Rule, ...] = ()
    optional_key: tuple[str, ...] = ()
    scope: str = "file"
    lookups: tuple[Lookup, ...] = ()
    derived: tuple[Column, ...] = ()
    sequence: Sequence | None = None
    monitor_granularity: str = ""
    monitor_slices: tuple[str, ...] = ()
    monitor_categories: tuple[str, ...] = ()
    monitor_levels: tuple[str, ...] = ()
    monitor_amounts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.scope not in SCOPES:
            raise ValueError(f"{self.scope!r}: scope must be one of {SCOPES}")
        declared = [*self.columns, *self.derived, *(c for k in self.lookups for c in k.columns)]
        for column in declared:
            if column.name in TIMING_COLUMNS:
                raise ValueError(f"{column.name} is set by event_time/knowable_at, not a column")
        names = [c.name for c in self.output_columns]
        repeated = sorted({n for n in names if names.count(n) > 1})
        if repeated:
            raise ValueError(f"{self.key_name}: repeated columns {repeated}")
        for column in (*self.key, *self.cluster_by):
            if column not in names:
                raise ValueError(f"{self.key_name}: {column!r} is not a Silver column")
        if self.monitor_granularity not in ("", *MONITOR_GRANULARITIES):
            raise ValueError(
                f"{self.key_name}: monitor_granularity must be one of {MONITOR_GRANULARITIES}"
            )
        if self.monitored_columns and not self.monitor_granularity:
            raise ValueError(f"{self.key_name}: monitored columns need a monitor_granularity")
        for column in self.monitored_columns:
            if column not in names:
                raise ValueError(f"{self.key_name}: {column!r} is not a Silver column")
        if not set(self.optional_key) <= set(self.key):
            raise ValueError(f"{self.key_name}: optional_key must be part of key")
        for rule in self.checks:
            if rule.action == "record":
                raise ValueError(f"{rule.name}: record-only checks are declared as a sequence")
        validate_rules(self.rules)

    @property
    def key_name(self) -> str:
        return self.dataset.key

    @property
    def table(self) -> str:
        return self.dataset.bronze_table

    @property
    def output_columns(self) -> tuple[Column, ...]:
        return (
            *self.columns,
            *(c for lookup in self.lookups for c in lookup.columns),
            *self.derived,
            Column("event_time", "TIMESTAMP", self.event_time),
            Column("knowable_at", "TIMESTAMP", self.knowable_at),
        )

    @property
    def judged_columns(self) -> tuple[str, ...]:
        return (*self.monitor_categories, *self.monitor_levels, *self.monitor_amounts)

    @property
    def monitored_columns(self) -> tuple[str, ...]:
        return (*self.monitor_slices, *self.judged_columns)

    @property
    def rules(self) -> tuple[Rule, ...]:
        required = tuple(c for c in self.key if c not in self.optional_key)
        rules = [
            RECORD_PARSES,
            SOURCE_REGISTERED,
            timestamp_plausible(self.plausible, self.plausible_description),
            KNOWABLE_AT_PRESENT,
            KNOWABLE_AT_NOT_BEFORE_EVENT,
        ]
        if required:
            rules.append(key_present(required))
        rules += [*self.checks, duplicate_key(self.key, self.scope)]
        if self.sequence is not None:
            rules.append(
                sequence_continuous(
                    self.sequence.value, self.sequence.step, self.sequence.description
                )
            )
        return tuple(rules)

    @property
    def depends_on(self) -> tuple[str, ...]:
        return tuple(lookup.table for lookup in self.lookups)


def ny_midnight(date_sql: str) -> str:
    return f"to_utc_timestamp(CAST({date_sql} AS TIMESTAMP), '{NEW_YORK}')"


def ny_midnight_after(date_sql: str) -> str:
    # Date-only values: the hour is unknown, so assume the latest one rather than risk a leak.
    return ny_midnight(f"date_add({date_sql}, 1)")


def utc_day(timestamp_sql: str, date_sql: str = "_period_start") -> str:
    return (
        f"{timestamp_sql} >= CAST({date_sql} AS TIMESTAMP) "
        f"AND {timestamp_sql} < CAST(date_add({date_sql}, 1) AS TIMESTAMP)"
    )


def ny_day(timestamp_sql: str, date_sql: str = "_period_start") -> str:
    return (
        f"{timestamp_sql} >= {ny_midnight(date_sql)} "
        f"AND {timestamp_sql} < {ny_midnight_after(date_sql)}"
    )


def from_epoch(value_sql: str, unit: str) -> str:
    functions = {"seconds": "timestamp_seconds", "millis": "timestamp_millis"}
    if unit == "micros":
        return f"timestamp_micros({value_sql})"
    bound = {"seconds": "9000000000000", "millis": "9000000000000000"}[unit]
    return (
        f"CASE WHEN {value_sql} BETWEEN -{bound} AND {bound} "
        f"THEN {functions[unit]}({value_sql}) END"
    )


def rfc3339(value_sql: str) -> str:
    return f"try_cast(regexp_replace({value_sql}, '([.][0-9]{{6}})[0-9]+', '$1') AS TIMESTAMP)"


def _cast(column: str, type_: str) -> str:
    return f"try_cast(`{column}` AS {type_})"


def _positive(*columns: str) -> Rule:
    return Rule(
        "price_and_quantity_positive",
        " AND ".join(f"{c} > 0" for c in columns),
        DROP,
        f"A price or quantity ({', '.join(columns)}) is zero, negative or not a number. Such a "
        "trade or bar is impossible.",
    )


_OHLC_CONSISTENT = Rule(
    "ohlc_consistent",
    "low <= least(open, close) AND high >= greatest(open, close)",
    DROP,
    "The bar's low is above its open or close, or its high below them. Such a bar is impossible.",
)

_BINANCE_PAIR = "'^([A-Z0-9]+?)(USDT|USDC|FDUSD|TUSD|BUSD|BTC|ETH|BNB|EUR|TRY)$'"
_BINANCE_MICROS_FROM = "DATE'2025-01-01'"
_BINANCE_TIME = "try_cast(_raw.`time` AS BIGINT)"

BINANCE_SPOT_TRADES_SILVER = SilverSpec(
    dataset=BINANCE_SPOT_TRADES,
    columns=(
        Column("symbol", "STRING", "_symbol"),
        Column("base_asset", "STRING", f"nullif(regexp_extract(_symbol, {_BINANCE_PAIR}, 1), '')"),
        Column("quote_asset", "STRING", f"nullif(regexp_extract(_symbol, {_BINANCE_PAIR}, 2), '')"),
        Column("trade_id", "BIGINT", _cast("trade_id", "BIGINT")),
        Column("price", PRICE, _cast("price", PRICE)),
        Column("quantity", PRICE, _cast("qty", PRICE)),
        Column("quote_quantity", PRICE, _cast("quote_qty", PRICE)),
        Column("is_buyer_maker", "BOOLEAN", _cast("is_buyer_maker", "BOOLEAN")),
        Column("is_best_match", "BOOLEAN", _cast("is_best_match", "BOOLEAN")),
    ),
    event_time=(
        f"CASE WHEN _period_start < {_BINANCE_MICROS_FROM} "
        f"THEN {from_epoch(_BINANCE_TIME, 'millis')} "
        f"ELSE {from_epoch(_BINANCE_TIME, 'micros')} END"
    ),
    knowable_at="event_time",
    plausible=utc_day("event_time"),
    plausible_description="The trade time is not on the UTC day of its daily file. Binance "
    "switched from milliseconds to microseconds on 2025-01-01, so a unit error lands decades away.",
    checks=(_positive("price", "quantity"),),
    key=("symbol", "trade_id"),
    sequence=Sequence(
        value="trade_id",
        step=1,
        description="Binance numbers each pair's trades consecutively, so a jump in trade_id "
        "inside a daily file means trades are missing from it. The gap is recorded, not repaired.",
    ),
    cluster_by=("symbol", "event_time"),
    description="Every Binance spot trade, typed, with its exchange time as knowable_at.",
    monitor_granularity="1 day",
    monitor_categories=("symbol", "base_asset", "quote_asset", "is_buyer_maker", "is_best_match"),
    monitor_levels=("price",),
    monitor_amounts=("quantity", "quote_quantity"),
)

_COINBASE_DAY_START = "unix_seconds(CAST(_period_start AS TIMESTAMP))"

COINBASE_SPOT_CANDLES_1M_SILVER = SilverSpec(
    dataset=COINBASE_SPOT_CANDLES_1M,
    columns=(
        Column("symbol", "STRING", "_symbol"),
        Column("base_asset", "STRING", "nullif(split_part(_symbol, '-', 1), '')"),
        Column("quote_asset", "STRING", "nullif(split_part(_symbol, '-', 2), '')"),
        Column("open", PRICE, _cast("open", PRICE)),
        Column("high", PRICE, _cast("high", PRICE)),
        Column("low", PRICE, _cast("low", PRICE)),
        Column("close", PRICE, _cast("close", PRICE)),
        Column("volume", PRICE, _cast("volume", PRICE)),
    ),
    event_time=from_epoch("try_cast(_raw.`time` AS BIGINT)", "seconds"),
    knowable_at="event_time + INTERVAL 1 MINUTE",
    plausible=utc_day("event_time"),
    plausible_description="The candle's minute is not on the UTC day of its daily file, which "
    "means the epoch seconds were misread.",
    checks=(_positive("open", "high", "low", "close", "volume"), _OHLC_CONSISTENT),
    key=("symbol", "event_time"),
    sequence=Sequence(
        value="unix_seconds(event_time)",
        step=60,
        first=_COINBASE_DAY_START,
        last=f"{_COINBASE_DAY_START} + 86340",
        description="Coinbase omits minutes without trades and has outages, so a day can lack "
        "candles. Missing minutes are recorded, never filled (ADR 0004).",
    ),
    cluster_by=("symbol", "event_time"),
    description="Coinbase one-minute candles, known at the end of their minute.",
    monitor_granularity="1 day",
    monitor_categories=("symbol", "base_asset", "quote_asset"),
    monitor_levels=("open", "high", "low", "close"),
    monitor_amounts=("volume",),
)

ALPACA_STOCK_TRADES_SILVER = SilverSpec(
    dataset=ALPACA_STOCK_TRADES,
    columns=(
        Column("symbol", "STRING", "_symbol"),
        Column("session_date", "DATE", "_period_start"),
        Column("trade_id", "BIGINT", _cast("i", "BIGINT")),
        Column("exchange", "STRING", "x"),
        Column("price", PRICE, _cast("p", PRICE)),
        Column("size", PRICE, _cast("s", PRICE)),
        Column("conditions", "ARRAY<STRING>", "from_json(c, 'ARRAY<STRING>')"),
        Column("tape", "STRING", "z"),
    ),
    event_time=rfc3339("_raw.t"),
    knowable_at="event_time",
    plausible=ny_day("event_time"),
    plausible_description="The trade time is outside its New York trading day (midnight to "
    "midnight, after-hours included), which means a time zone or parsing error.",
    checks=(_positive("price", "size"),),
    key=("symbol", "trade_id"),
    cluster_by=("symbol", "event_time"),
    description="IEX trades per symbol, with the trade time as knowable_at.",
    monitor_granularity="1 day",
    monitor_slices=("symbol",),
    monitor_categories=("symbol", "exchange", "tape"),
    monitor_levels=("price",),
    monitor_amounts=("size",),
)

ALPACA_STOCK_BARS_1D_SILVER = SilverSpec(
    dataset=ALPACA_STOCK_BARS_1D,
    columns=(
        Column("symbol", "STRING", "_symbol"),
        Column("session_date", "DATE", "_period_start"),
        Column("open", PRICE, _cast("o", PRICE)),
        Column("high", PRICE, _cast("h", PRICE)),
        Column("low", PRICE, _cast("l", PRICE)),
        Column("close", PRICE, _cast("c", PRICE)),
        Column("volume", PRICE, _cast("v", PRICE)),
        Column("trade_count", "BIGINT", _cast("n", "BIGINT")),
        Column("vwap", PRICE, _cast("vw", PRICE)),
    ),
    event_time=rfc3339("_raw.t"),
    knowable_at=ny_midnight_after("session_date"),
    plausible=ny_day("event_time"),
    plausible_description="The bar's time is not its New York trading day, which is how Alpaca's "
    "inclusive end once returned the next day's bar (fixed in the collector, 2026-10-04).",
    checks=(_positive("open", "high", "low", "close", "volume"), _OHLC_CONSISTENT),
    key=("symbol", "session_date"),
    cluster_by=("symbol", "session_date"),
    description="Unadjusted consolidated daily bars, known when their New York day ends.",
    monitor_granularity="1 week",
    monitor_categories=("symbol",),
    monitor_levels=("open", "high", "low", "close", "vwap"),
    monitor_amounts=("volume", "trade_count"),
)

FRED_SERIES_VINTAGES_SILVER = SilverSpec(
    dataset=FRED_SERIES_VINTAGES,
    columns=(
        Column("series_id", "STRING", "_symbol"),
        Column("observation_date", "DATE", _cast("date", "DATE")),
        Column("vintage_date", "DATE", _cast("realtime_start", "DATE")),
        Column("value", "DECIMAL(24,6)", _cast("value", "DECIMAL(24,6)")),
    ),
    event_time="CAST(observation_date AS TIMESTAMP)",
    knowable_at=ny_midnight_after("vintage_date"),
    plausible="event_time >= TIMESTAMP'1900-01-01 00:00:00' "
    "AND event_time < CAST(date_add(CAST(_ingested_at AS DATE), 1) AS TIMESTAMP)",
    plausible_description="The observation date is before 1900 or after the day it was loaded.",
    checks=(
        Rule(
            "value_reported",
            "value IS NOT NULL",
            DROP,
            "The vintage has no value for this date: FRED writes '.' for observations that were "
            "not published (yet) when the vintage was released.",
        ),
    ),
    key=("series_id", "vintage_date", "observation_date"),
    cluster_by=("series_id", "observation_date"),
    description="Every observation of every published vintage, known at the end of its release "
    "day, never at the date of the latest revision.",
    monitor_granularity="1 week",
    monitor_slices=("series_id",),
)

EDGAR_FILINGS_SILVER = SilverSpec(
    dataset=EDGAR_FILINGS,
    columns=(
        Column("symbol", "STRING", "_symbol"),
        Column("accession_number", "STRING", "nullif(accession_number, '')"),
        Column("form", "STRING", "form"),
        Column("filing_date", "DATE", _cast("filing_date", "DATE")),
        Column("report_date", "DATE", "try_cast(nullif(report_date, '') AS DATE)"),
        Column("acceptance_time", "TIMESTAMP", rfc3339("acceptance_date_time")),
        Column("act", "STRING", "nullif(act, '')"),
        Column("file_number", "STRING", "nullif(file_number, '')"),
        Column("film_number", "STRING", "nullif(film_number, '')"),
        Column("items", "STRING", "nullif(items, '')"),
        Column("size_bytes", "BIGINT", _cast("size", "BIGINT")),
        Column("is_xbrl", "BOOLEAN", _cast("is_xbrl", "BOOLEAN")),
        Column("is_inline_xbrl", "BOOLEAN", _cast("is_inline_xbrl", "BOOLEAN")),
        Column("primary_document", "STRING", "nullif(primary_document, '')"),
        Column("primary_doc_description", "STRING", "nullif(primary_doc_description, '')"),
    ),
    event_time="acceptance_time",
    knowable_at="acceptance_time",
    plausible=(
        f"event_time >= {ny_midnight('date_sub(filing_date, 7)')} "
        f"AND event_time < {ny_midnight_after('filing_date')} "
        "AND event_time <= _ingested_at "
        f"AND hour(from_utc_timestamp(event_time, '{NEW_YORK}')) BETWEEN 6 AND 22"
    ),
    plausible_description="The acceptance time is after its filing date, more than a week before "
    "it, after the snapshot was loaded, or outside EDGAR's 06:00-22:00 New York hours.",
    key=("symbol", "accession_number"),
    scope="symbol",
    cluster_by=("symbol", "filing_date"),
    description="Each company's filings, known at their SEC acceptance time.",
)

EDGAR_COMPANY_FACTS_SILVER = SilverSpec(
    dataset=EDGAR_COMPANY_FACTS,
    columns=(
        Column("symbol", "STRING", "_symbol"),
        Column("taxonomy", "STRING", "taxonomy"),
        Column("concept", "STRING", "concept"),
        Column("unit", "STRING", "unit"),
        Column("period_start", "DATE", _cast("start", "DATE")),
        Column("period_end", "DATE", _cast("end", "DATE")),
        Column("value", "DECIMAL(38,6)", _cast("val", "DECIMAL(38,6)")),
        Column("accession_number", "STRING", "nullif(accn, '')"),
        Column("fiscal_year", "INT", _cast("fy", "INT")),
        Column("fiscal_period", "STRING", "nullif(fp, '')"),
        Column("form", "STRING", "form"),
        Column("filed_date", "DATE", _cast("filed", "DATE")),
        Column("frame", "STRING", "nullif(frame, '')"),
    ),
    lookups=(
        Lookup(
            table="edgar_filings",
            on=(("symbol", "symbol"), ("accession_number", "accession_number")),
            columns=(Column("acceptance_time", "TIMESTAMP", "acceptance_time"),),
        ),
    ),
    derived=(
        Column(
            "knowable_basis",
            "STRING",
            "CASE WHEN acceptance_time IS NOT NULL THEN 'acceptance_time' "
            "WHEN filed_date IS NOT NULL THEN 'end_of_filing_date' END",
        ),
    ),
    event_time="CAST(period_end AS TIMESTAMP)",
    knowable_at=f"coalesce(acceptance_time, {ny_midnight_after('filed_date')})",
    plausible="event_time >= TIMESTAMP'1990-01-01 00:00:00' AND period_end <= filed_date",
    plausible_description="The period ends before 1990 or after the filing that reported it: a "
    "fact tagged to a period that had not happened yet, such as a dividend declared for the next "
    "quarter or a subsequent event.",
    checks=(
        Rule(
            "value_numeric",
            "value IS NOT NULL",
            DROP,
            "The fact's value is missing or not a number that fits DECIMAL(38,6).",
        ),
        Rule(
            "period_ordered",
            "period_start IS NULL OR period_start <= period_end",
            DROP,
            "The fact's period starts after it ends.",
        ),
    ),
    key=(
        "symbol",
        "taxonomy",
        "concept",
        "unit",
        "period_start",
        "period_end",
        "accession_number",
    ),
    optional_key=("period_start",),
    scope="symbol",
    cluster_by=("symbol", "concept"),
    description="Every XBRL fact once per filing that reported it, known when that filing was "
    "accepted, never at the end of the reporting period.",
)

MIN_DOCUMENT_CHARS = 10_000

EDGAR_FILING_DOCUMENTS_SILVER = SilverSpec(
    dataset=EDGAR_FILING_DOCUMENTS,
    columns=(
        Column("symbol", "STRING", "_symbol"),
        Column(
            "accession_number",
            "STRING",
            "nullif(regexp_extract(_source_file, "
            "'-([0-9]{10}-[0-9]{2}-[0-9]{6})[.]html?$', 1), '')",
        ),
        Column("html", "STRING", "document"),
        Column("html_chars", "BIGINT", "CAST(length(document) AS BIGINT)"),
    ),
    lookups=(
        Lookup(
            table="edgar_filings",
            on=(("symbol", "symbol"), ("accession_number", "accession_number")),
            columns=(
                Column("form", "STRING", "form"),
                Column("filing_date", "DATE", "filing_date"),
                Column("report_date", "DATE", "report_date"),
                Column("acceptance_time", "TIMESTAMP", "acceptance_time"),
            ),
        ),
    ),
    event_time="acceptance_time",
    knowable_at="acceptance_time",
    # A filing missing from edgar_filings has no acceptance time: knowable_at_present halts on it.
    plausible="event_time IS NULL OR event_time <= _ingested_at",
    plausible_description="The filing was accepted after its document was loaded.",
    checks=(
        Rule(
            "document_has_text",
            f"html_chars >= {MIN_DOCUMENT_CHARS}",
            DROP,
            "The document is too short to be a periodic report: an error page or a cover letter.",
        ),
    ),
    key=("symbol", "accession_number"),
    scope="symbol",
    cluster_by=("symbol", "filing_date"),
    description="The text of each 10-K and 10-Q as filed, known at its SEC acceptance time "
    "(ADR 0010).",
)

SILVER_SPECS: dict[str, SilverSpec] = {
    s.key_name: s
    for s in (
        BINANCE_SPOT_TRADES_SILVER,
        COINBASE_SPOT_CANDLES_1M_SILVER,
        FRED_SERIES_VINTAGES_SILVER,
        ALPACA_STOCK_TRADES_SILVER,
        ALPACA_STOCK_BARS_1D_SILVER,
        EDGAR_FILINGS_SILVER,
        EDGAR_COMPANY_FACTS_SILVER,
        EDGAR_FILING_DOCUMENTS_SILVER,
    )
}
_BY_TABLE = {s.table: s for s in SILVER_SPECS.values()}


def parse_keys(value: str) -> set[str]:
    return {get_spec(k).key_name for k in re.split(r"[,\s]+", value) if k}


def get_spec(key: str) -> SilverSpec:
    try:
        return SILVER_SPECS[key]
    except KeyError:
        raise ValueError(f"no Silver spec for {key!r}; known: {sorted(SILVER_SPECS)}") from None


def spec_for_table(table: str) -> SilverSpec:
    try:
        return _BY_TABLE[table]
    except KeyError:
        raise ValueError(f"no Silver table {table!r}") from None


def transform_groups(specs: list[SilverSpec]) -> list[list[SilverSpec]]:
    order = {s.table: i for i, s in enumerate(specs)}
    group_of = {s.table: s.table for s in specs}

    def root(table: str) -> str:
        while group_of[table] != table:
            table = group_of[table]
        return table

    for spec in specs:
        for dependency in spec.depends_on:
            if dependency not in order:
                raise ValueError(f"{spec.table} looks up {dependency}, which has no spec")
            if order[dependency] > order[spec.table]:
                raise ValueError(f"{dependency} must be listed before {spec.table}")
            group_of[root(spec.table)] = root(dependency)

    groups: dict[str, list[SilverSpec]] = {}
    for spec in specs:
        groups.setdefault(root(spec.table), []).append(spec)
    return list(groups.values())
