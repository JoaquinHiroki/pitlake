from dataclasses import dataclass
from datetime import date

from pitlake.bronze import sql_string
from pitlake.quality import FUNDAMENTALS_DATED_BY_PERIOD, GOLD_FAULTS, NO_FAULT
from pitlake.silver import (
    ALPACA_STOCK_BARS_1D_SILVER,
    ALPACA_STOCK_TRADES_SILVER,
    BINANCE_SPOT_TRADES_SILVER,
    COINBASE_SPOT_CANDLES_1M_SILVER,
    EDGAR_COMPANY_FACTS_SILVER,
    EDGAR_FILINGS_SILVER,
    FRED_SERIES_VINTAGES_SILVER,
    SilverSpec,
    ny_midnight_after,
)

PRICE = "decimal(28,10)"
VOLUME = "decimal(38,10)"
TRADES = "trades"
CANDLES = "candles"
GLOBAL_ENTITY = "US"

PRICES = "prices"
FUNDAMENTALS = "fundamentals"
MACRO = "macro"
FEATURE_GROUPS = (PRICES, FUNDAMENTALS, MACRO)

INSTANT = "instant"
QUARTER = "quarter"
ANNUAL = "annual"
DURATIONS = (INSTANT, QUARTER, ANNUAL)
QUARTER_DAYS = (80, 100)
ANNUAL_DAYS = (350, 380)
FEATURE_SUFFIX = {INSTANT: "", QUARTER: "_q", ANNUAL: "_fy"}

BAR_COLUMNS = (
    "venue",
    "symbol",
    "period",
    "bar_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trades",
    "vwap",
    "knowable_at",
    "_source_file",
    "_source_sha256",
)
DAILY_COLUMNS = (
    "venue",
    "symbol",
    "session_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trades",
    "vwap",
    "minutes",
    "knowable_at",
    "_source",
)
FUNDAMENTAL_COLUMNS = (
    "symbol",
    "metric",
    "duration",
    "period_start",
    "period_end",
    "value",
    "unit",
    "concept",
    "accession_number",
    "form",
    "filed_date",
    "knowable_at",
    "knowable_basis",
)
FEATURE_COLUMNS = (
    "entity",
    "feature",
    "feature_group",
    "event_time",
    "knowable_at",
    "value",
    "source_table",
    "source_ref",
)


@dataclass(frozen=True)
class MinuteBarSource:
    spec: SilverSpec
    kind: str
    quantity: str = "quantity"

    def __post_init__(self) -> None:
        if self.kind not in (TRADES, CANDLES):
            raise ValueError(f"{self.kind!r}: kind must be {TRADES} or {CANDLES}")

    @property
    def venue(self) -> str:
        return self.spec.dataset.source


MINUTE_BAR_SOURCES = (
    MinuteBarSource(BINANCE_SPOT_TRADES_SILVER, TRADES),
    MinuteBarSource(COINBASE_SPOT_CANDLES_1M_SILVER, CANDLES),
    MinuteBarSource(ALPACA_STOCK_TRADES_SILVER, TRADES, quantity="size"),
)
DAILY_FROM_MINUTES = ("binance", "coinbase")
DAILY_BARS = ALPACA_STOCK_BARS_1D_SILVER
EQUITY_VENUES = (DAILY_BARS.dataset.source,)
SILVER_INPUTS = (
    *(s.spec for s in MINUTE_BAR_SOURCES),
    DAILY_BARS,
    EDGAR_COMPANY_FACTS_SILVER,
    EDGAR_FILINGS_SILVER,
    FRED_SERIES_VINTAGES_SILVER,
)


@dataclass(frozen=True)
class Metric:
    name: str
    concepts: tuple[str, ...]
    unit: str
    durations: tuple[str, ...]
    description: str
    taxonomy: str = "us-gaap"

    def __post_init__(self) -> None:
        if not set(self.durations) <= set(DURATIONS):
            raise ValueError(f"{self.name}: durations must be among {DURATIONS}")
        if not self.concepts:
            raise ValueError(f"{self.name}: at least one concept")

    @property
    def features(self) -> tuple[str, ...]:
        return tuple(feature_name(self.name, d) for d in self.durations)


FLOWS = (QUARTER, ANNUAL)
METRICS = (
    Metric(
        "revenue",
        ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"),
        "USD",
        FLOWS,
        "Total revenue. Companies tag it differently, and SalesRevenueNet was retired in 2018, so "
        "the first concept the filing reported wins.",
    ),
    Metric("gross_profit", ("GrossProfit",), "USD", FLOWS, "Revenue less cost of revenue."),
    Metric("operating_income", ("OperatingIncomeLoss",), "USD", FLOWS, "Operating income."),
    Metric("net_income", ("NetIncomeLoss",), "USD", FLOWS, "Net income attributable to the firm."),
    Metric("eps_diluted", ("EarningsPerShareDiluted",), "USD/shares", FLOWS, "Diluted EPS."),
    Metric(
        "operating_cash_flow",
        ("NetCashProvidedByUsedInOperatingActivities",),
        "USD",
        (ANNUAL,),
        "Cash from operations. 10-Qs report it year to date, so only the fiscal year is a feature.",
    ),
    Metric("assets", ("Assets",), "USD", (INSTANT,), "Total assets at the balance sheet date."),
    Metric("liabilities", ("Liabilities",), "USD", (INSTANT,), "Total liabilities."),
    Metric("equity", ("StockholdersEquity",), "USD", (INSTANT,), "Stockholders' equity."),
    Metric("cash", ("CashAndCashEquivalentsAtCarryingValue",), "USD", (INSTANT,), "Cash."),
    Metric(
        "shares_outstanding",
        ("EntityCommonStockSharesOutstanding",),
        "shares",
        (INSTANT,),
        "Shares outstanding from the cover page, dated shortly before the filing.",
        taxonomy="dei",
    ),
)

MACRO_SERIES = {"UNRATE": "unrate", "CPIAUCSL": "cpi", "FEDFUNDS": "fedfunds"}
YEAR_OVER_YEAR = {"CPIAUCSL": "cpi_yoy"}
PRICE_FEATURES = ("close", "return_1d", "volatility_20d", "dollar_volume_20d")
WINDOW_DAYS = 20


def feature_name(metric: str, duration: str) -> str:
    return f"{metric}{FEATURE_SUFFIX[duration]}"


def duration_of(period_start: date | None, period_end: date) -> str | None:
    if period_start is None:
        return INSTANT
    days = (period_end - period_start).days + 1
    if QUARTER_DAYS[0] <= days <= QUARTER_DAYS[1]:
        return QUARTER
    if ANNUAL_DAYS[0] <= days <= ANNUAL_DAYS[1]:
        return ANNUAL
    return None


def duration_sql(start: str = "period_start", end: str = "period_end") -> str:
    days = f"datediff({end}, {start}) + 1"
    return (
        f"CASE WHEN {start} IS NULL THEN '{INSTANT}' "
        f"WHEN {days} BETWEEN {QUARTER_DAYS[0]} AND {QUARTER_DAYS[1]} THEN '{QUARTER}' "
        f"WHEN {days} BETWEEN {ANNUAL_DAYS[0]} AND {ANNUAL_DAYS[1]} THEN '{ANNUAL}' END"
    )


def metric_rows(metrics: tuple[Metric, ...] = METRICS) -> list[tuple]:
    names = [m.name for m in metrics]
    repeated = sorted({n for n in names if names.count(n) > 1})
    if repeated:
        raise ValueError(f"metric names must be unique, repeated: {repeated}")
    return [
        (m.name, m.taxonomy, concept, m.unit, priority, list(m.durations))
        for m in metrics
        for priority, concept in enumerate(m.concepts, start=1)
    ]


def macro_name_sql(series: str = "series_id", names: dict[str, str] = MACRO_SERIES) -> str:
    branches = " ".join(f"WHEN {sql_string(k)} THEN {sql_string(v)}" for k, v in names.items())
    return f"CASE {series} {branches} ELSE lower({series}) END"


def session_end_sql(venue: str = "venue", day: str = "session_date") -> str:
    equities = ", ".join(sql_string(v) for v in EQUITY_VENUES)
    return (
        f"CASE WHEN {venue} IN ({equities}) THEN {ny_midnight_after(day)} "
        f"ELSE CAST(date_add({day}, 1) AS TIMESTAMP) END"
    )


def gold_fault(fault: str) -> str:
    return fault if fault in GOLD_FAULTS else NO_FAULT


def _bar_end(column: str = "bar_time"):
    from pyspark.sql import functions as F

    return F.col(column) + F.expr("INTERVAL 1 MINUTE")


def trade_minute_bars(trades, venue: str, quantity: str = "quantity"):
    from pyspark.sql import functions as F

    order = F.struct("event_time", "trade_id")
    price, size = F.col("price").cast("double"), F.col(quantity).cast("double")
    return (
        trades.groupBy(
            "symbol",
            F.col("_period_start").alias("period"),
            "_source_file",
            "_source_sha256",
            F.date_trunc("minute", "event_time").alias("bar_time"),
        )
        .agg(
            F.min_by("price", order).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", order).alias("close"),
            F.sum(quantity).alias("volume"),
            F.count("*").alias("trades"),
            F.sum(price * size).alias("_notional"),
            F.sum(size).alias("_size"),
            F.max("knowable_at").alias("_last_knowable"),
        )
        .select(
            F.lit(venue).alias("venue"),
            "symbol",
            "period",
            "bar_time",
            *(F.col(c).cast(PRICE).alias(c) for c in ("open", "high", "low", "close")),
            F.col("volume").cast(VOLUME).alias("volume"),
            "trades",
            F.when(F.col("_size") > 0, F.col("_notional") / F.col("_size"))
            .cast(PRICE)
            .alias("vwap"),
            F.greatest(_bar_end(), F.col("_last_knowable")).alias("knowable_at"),
            "_source_file",
            "_source_sha256",
        )
    )


def candle_minute_bars(candles, venue: str):
    from pyspark.sql import functions as F

    return candles.select(
        F.lit(venue).alias("venue"),
        "symbol",
        F.col("_period_start").alias("period"),
        F.col("event_time").alias("bar_time"),
        *(F.col(c).cast(PRICE).alias(c) for c in ("open", "high", "low", "close")),
        F.col("volume").cast(VOLUME).alias("volume"),
        F.lit(None).cast("bigint").alias("trades"),
        F.lit(None).cast(PRICE).alias("vwap"),
        F.greatest(_bar_end("event_time"), F.col("knowable_at")).alias("knowable_at"),
        "_source_file",
        "_source_sha256",
    )


def minute_bars(source: MinuteBarSource, rows):
    if source.kind == TRADES:
        return trade_minute_bars(rows, source.venue, source.quantity)
    return candle_minute_bars(rows, source.venue)


def daily_bars(bars_1m, equity_bars):
    from pyspark.sql import functions as F

    volume = F.col("volume").cast("double")
    crypto = (
        bars_1m.where(F.col("venue").isin(*DAILY_FROM_MINUTES))
        .groupBy("venue", "symbol", F.col("period").alias("session_date"))
        .agg(
            F.min_by("open", "bar_time").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by("close", "bar_time").alias("close"),
            F.sum("volume").alias("volume"),
            F.sum("trades").alias("trades"),
            F.sum(F.col("vwap").cast("double") * volume).alias("_notional"),
            F.sum(F.when(F.col("vwap").isNotNull(), volume)).alias("_size"),
            F.count("*").cast("int").alias("minutes"),
            F.max("knowable_at").alias("_last_knowable"),
        )
        .select(
            "venue",
            "symbol",
            "session_date",
            *(F.col(c).cast(PRICE).alias(c) for c in ("open", "high", "low", "close")),
            F.col("volume").cast(VOLUME).alias("volume"),
            "trades",
            F.when(F.col("_size") > 0, F.col("_notional") / F.col("_size"))
            .cast(PRICE)
            .alias("vwap"),
            "minutes",
            F.greatest(F.expr(session_end_sql()), F.col("_last_knowable")).alias("knowable_at"),
            F.lit("bars_1m").alias("_source"),
        )
    )
    equities = equity_bars.select(
        F.lit(DAILY_BARS.dataset.source).alias("venue"),
        "symbol",
        "session_date",
        *(F.col(c).cast(PRICE).alias(c) for c in ("open", "high", "low", "close")),
        F.col("volume").cast(VOLUME).alias("volume"),
        F.col("trade_count").alias("trades"),
        F.col("vwap").cast(PRICE).alias("vwap"),
        F.lit(None).cast("int").alias("minutes"),
        "knowable_at",
        F.lit(DAILY_BARS.table).alias("_source"),
    )
    return crypto.unionByName(equities)


def fundamentals(spark, facts):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    mapping = spark.createDataFrame(
        metric_rows(),
        "metric STRING, taxonomy STRING, concept STRING, unit STRING, priority INT, "
        "durations ARRAY<STRING>",
    )
    ranked = Window.partitionBy(
        "symbol", "metric", "duration", "period_end", "accession_number"
    ).orderBy("priority", F.col("period_start").asc_nulls_last())
    return (
        facts.join(F.broadcast(mapping), ["taxonomy", "concept", "unit"])
        .withColumn("duration", F.expr(duration_sql()))
        .where(F.expr("array_contains(durations, duration)"))
        .withColumn("_rank", F.row_number().over(ranked))
        .where(F.col("_rank") == 1)
        .select(*FUNDAMENTAL_COLUMNS)
    )


def _feature_rows(frame, entity, feature, group, event_time, knowable_at, value, table, ref):
    from pyspark.sql import functions as F

    return frame.select(
        entity.alias("entity"),
        feature.alias("feature"),
        F.lit(group).alias("feature_group"),
        event_time.cast("timestamp").alias("event_time"),
        knowable_at.alias("knowable_at"),
        value.cast("double").alias("value"),
        F.lit(table).alias("source_table"),
        ref.cast("string").alias("source_ref"),
    )


def fundamental_features(fundamentals_frame, fault: str = NO_FAULT):
    from pyspark.sql import functions as F

    suffix = " ".join(f"WHEN '{d}' THEN '{s}'" for d, s in FEATURE_SUFFIX.items())
    period_end = F.col("period_end").cast("timestamp")
    knowable = period_end if fault == FUNDAMENTALS_DATED_BY_PERIOD else F.col("knowable_at")
    return _feature_rows(
        fundamentals_frame,
        F.col("symbol"),
        F.concat(F.col("metric"), F.expr(f"CASE duration {suffix} END")),
        FUNDAMENTALS,
        period_end,
        knowable,
        F.col("value"),
        EDGAR_COMPANY_FACTS_SILVER.table,
        F.col("accession_number"),
    )


def macro_features(vintages):
    from pyspark.sql import functions as F

    table = FRED_SERIES_VINTAGES_SILVER.table
    levels = _feature_rows(
        vintages,
        F.lit(GLOBAL_ENTITY),
        F.expr(macro_name_sql()),
        MACRO,
        F.col("observation_date"),
        F.col("knowable_at"),
        F.col("value"),
        table,
        F.col("vintage_date"),
    )
    current = vintages.where(F.col("series_id").isin(*YEAR_OVER_YEAR))
    earlier = vintages.select(
        F.col("series_id").alias("_series_id"),
        F.col("vintage_date").alias("_vintage_date"),
        F.col("observation_date").alias("_observation_date"),
        F.col("value").cast("double").alias("_value"),
        F.col("knowable_at").alias("_knowable_at"),
    )
    joined = current.join(
        earlier,
        (F.col("series_id") == F.col("_series_id"))
        & (F.col("vintage_date") == F.col("_vintage_date"))
        & (F.col("_observation_date") == F.add_months("observation_date", -12)),
    )
    changes = _feature_rows(
        joined,
        F.lit(GLOBAL_ENTITY),
        F.expr(macro_name_sql(names=YEAR_OVER_YEAR)),
        MACRO,
        F.col("observation_date"),
        F.greatest("knowable_at", "_knowable_at"),
        F.when(F.col("_value") != 0, F.col("value").cast("double") / F.col("_value") - 1),
        table,
        F.col("vintage_date"),
    )
    return levels.unionByName(changes)


def price_features(bars_1d):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    by_day = Window.partitionBy("venue", "symbol").orderBy("session_date")
    recent = by_day.rowsBetween(-(WINDOW_DAYS - 1), 0)
    inputs = by_day.rowsBetween(-WINDOW_DAYS, 0)
    full = F.count("return_1d").over(recent) == WINDOW_DAYS
    daily = bars_1d.select(
        "venue",
        "symbol",
        "session_date",
        F.col("close").cast("double").alias("close"),
        (F.col("close").cast("double") * F.col("volume").cast("double")).alias("_dollars"),
        "knowable_at",
    ).withColumn("return_1d", F.ln(F.col("close") / F.lag("close").over(by_day)))
    windowed = daily.select(
        "symbol",
        "session_date",
        "close",
        "return_1d",
        F.when(full, F.stddev_samp("return_1d").over(recent)).alias("volatility_20d"),
        F.when(full, F.avg("_dollars").over(recent)).alias("dollar_volume_20d"),
        F.max("knowable_at").over(inputs).alias("_knowable_at"),
    )
    values = F.explode(
        F.array(*(F.struct(F.lit(n).alias("name"), F.col(n).alias("v")) for n in PRICE_FEATURES))
    ).alias("_f")
    long = windowed.select("symbol", "session_date", "_knowable_at", values).where(
        F.col("_f.v").isNotNull()
    )
    return _feature_rows(
        long,
        F.col("symbol"),
        F.col("_f.name"),
        PRICES,
        F.col("session_date"),
        F.col("_knowable_at"),
        F.col("_f.v"),
        "bars_1d",
        F.col("session_date"),
    )


def build_features(spark, bars_1d, facts, vintages, fault: str = NO_FAULT):
    return (
        price_features(bars_1d)
        .unionByName(fundamental_features(fundamentals(spark, facts), fault))
        .unionByName(macro_features(vintages))
    )
