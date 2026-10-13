from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo


def utc(*parts):
    return datetime(*parts)


def ny_end(day):
    end = datetime.combine(day + timedelta(days=1), time(), ZoneInfo("America/New_York"))
    return end.astimezone(UTC).replace(tzinfo=None)


POISON = 999999
SESSIONS = 25
FACTS_SCHEMA = (
    "symbol STRING, taxonomy STRING, concept STRING, unit STRING, period_start DATE, "
    "period_end DATE, value DECIMAL(38,6), accession_number STRING, form STRING, filed_date DATE, "
    "knowable_at TIMESTAMP, knowable_basis STRING"
)

# (symbol, accession, form, filed, acceptance or None, facts as (concept, unit, start, end, value))
FILINGS = (
    (
        "AAPL",
        "0000320193-24-000123",
        "10-K",
        (2024, 11, 1),
        None,
        (
            ("Revenues", "USD", (2023, 10, 1), (2024, 9, 28), "391.035"),
            ("Assets", "USD", None, (2024, 9, 28), "364.98"),
        ),
    ),
    (
        "MSFT",
        "0000950170-25-010491",
        "10-Q",
        (2025, 1, 29),
        (2025, 1, 29, 21, 0),
        (("Revenues", "USD", (2024, 10, 1), (2024, 12, 31), "69.632"),),
    ),
    (
        "AAPL",
        "0000320193-25-000008",
        "10-Q",
        (2025, 1, 31),
        (2025, 1, 31, 21, 30),
        (
            ("Revenues", "USD", (2024, 9, 29), (2024, 12, 28), "124.3"),
            (
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                "USD",
                (2024, 9, 29),
                (2024, 12, 28),
                "124.0",
            ),
            ("Revenues", "USD", (2023, 10, 1), (2023, 12, 30), "119.575"),
            ("NetIncomeLoss", "USD", (2024, 9, 29), (2024, 12, 28), "36.33"),
            ("Assets", "USD", None, (2024, 12, 28), "344.085"),
        ),
    ),
    (
        "AAPL",
        "0000320193-25-000020",
        "10-Q/A",
        (2025, 2, 20),
        (2025, 2, 20, 15, 0),
        (("Revenues", "USD", (2024, 9, 29), (2024, 12, 28), "125.0"),),
    ),
    (
        "AAPL",
        "0000320193-25-000099",
        "8-K",
        (2025, 3, 1),
        (2025, 3, 1, 12, 0),
        (("Revenues", "USD", (2024, 9, 29), (2024, 12, 28), str(POISON)),),
    ),
    (
        "AAPL",
        "0000320193-25-000057",
        "10-Q",
        (2025, 5, 2),
        (2025, 5, 2, 20, 30),
        (
            ("Revenues", "USD", (2024, 12, 29), (2025, 3, 29), "95.359"),
            ("Revenues", "USD", (2024, 9, 29), (2025, 3, 29), "219.659"),
            ("Assets", "USD", None, (2025, 3, 29), "331.233"),
        ),
    ),
)

# (series, vintage, observations as (date, value))
VINTAGES = (
    ("UNRATE", (2025, 1, 10), (((2024, 11, 1), "4.2"), ((2024, 12, 1), "4.1"))),
    (
        "UNRATE",
        (2025, 2, 7),
        (((2024, 11, 1), "4.2"), ((2024, 12, 1), "4.15"), ((2025, 1, 1), "4.0")),
    ),
    ("CPIAUCSL", (2025, 1, 15), (((2023, 12, 1), "300"), ((2024, 12, 1), "309"))),
    (
        "CPIAUCSL",
        (2025, 2, 12),
        (
            ((2023, 12, 1), "300"),
            ((2024, 1, 1), "301"),
            ((2024, 12, 1), "310.5"),
            ((2025, 1, 1), "310"),
        ),
    ),
)


def build(spark):
    facts, filings = [], []
    for symbol, accession, form, filed, accepted, reported in FILINGS:
        filed = date(*filed)
        knowable = utc(*accepted) if accepted else ny_end(filed)
        basis = "acceptance_time" if accepted else "end_of_filing_date"
        if accepted:
            filings.append((symbol, accession, form, filed, utc(*accepted)))
        for concept, unit, start, end, value in reported:
            facts.append(
                (
                    symbol,
                    "us-gaap",
                    concept,
                    unit,
                    date(*start) if start else None,
                    date(*end),
                    Decimal(value),
                    accession,
                    form,
                    filed,
                    knowable,
                    basis,
                )
            )
    vintages = [
        (
            series,
            date(*observed),
            date(*vintage),
            Decimal(value),
            utc(*observed, 0, 0),
            ny_end(date(*vintage)),
        )
        for series, vintage, observations in VINTAGES
        for observed, value in observations
    ]
    first = date(2025, 1, 2)
    days = [first + timedelta(days=i) for i in range(SESSIONS)]
    bars = [
        ("alpaca", "AAPL", d, Decimal(100 + i + i % 3), Decimal(1000), ny_end(d))
        for i, d in enumerate(days)
    ] + [
        (
            "binance",
            "BTCUSDT",
            d,
            Decimal(90000 + (-1) ** i * 100 * i),
            Decimal(10),
            utc(d.year, d.month, d.day) + timedelta(days=1),
        )
        for i, d in enumerate(days)
    ]
    return {
        "facts": spark.createDataFrame(facts, FACTS_SCHEMA),
        "filings": spark.createDataFrame(
            filings,
            "symbol STRING, accession_number STRING, form STRING, filing_date DATE, "
            "acceptance_time TIMESTAMP",
        ),
        "vintages": spark.createDataFrame(
            vintages,
            "series_id STRING, observation_date DATE, vintage_date DATE, value DECIMAL(24,6), "
            "event_time TIMESTAMP, knowable_at TIMESTAMP",
        ),
        "bars_1d": spark.createDataFrame(
            bars,
            "venue STRING, symbol STRING, session_date DATE, close DECIMAL(28,10), "
            "volume DECIMAL(38,10), knowable_at TIMESTAMP",
        ),
        "days": days,
    }
