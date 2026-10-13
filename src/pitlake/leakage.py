import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from pitlake.gold import FUNDAMENTALS, GLOBAL_ENTITY, MACRO, PRICES, session_end_sql
from pitlake.pit import AS_OF, RESULT_COLUMNS, as_of
from pitlake.silver import NEW_YORK, ny_midnight_after

BAR_CLOSED = "bar_closed"
FEATURE_AFTER_SOURCE = "feature_after_source"
FILING_INVISIBLE = "filing_invisible_before_acceptance"
COUNTERFACTUAL = "counterfactual"
CHECKS = (BAR_CLOSED, FEATURE_AFTER_SOURCE, FILING_INVISIBLE, COUNTERFACTUAL)
SAMPLE_ROWS = 5
JUST_BEFORE = timedelta(microseconds=1)


@dataclass(frozen=True)
class CheckResult:
    check: str
    subject: str
    examined: int
    cases: int
    sample: str = "[]"

    def __post_init__(self) -> None:
        if self.check not in CHECKS:
            raise ValueError(f"{self.check!r}: check must be one of {CHECKS}")


class LeakageFound(Exception):
    def __init__(self, results: list[CheckResult]):
        self.results = [r for r in results if r.cases]
        detail = "; ".join(f"{r.check} {r.subject}: {r.cases} cases" for r in self.results[:20])
        total = sum(r.cases for r in self.results)
        super().__init__(f"leakage audit found {total} cases: {detail}; nothing was written")


def _sample(violations, columns: Iterable[str]) -> str:
    from pyspark.sql import functions as F

    rows = violations.select(F.to_json(F.struct(*columns)).alias("row")).limit(SAMPLE_ROWS)
    return json.dumps([json.loads(r["row"]) for r in rows.collect()])


def _judge(check: str, subject: str, examined, violations, columns) -> CheckResult:
    cases = violations.count()
    sample = _sample(violations, columns) if cases else "[]"
    return CheckResult(check, subject, examined.count(), cases, sample)


def bar_closed(bars_1m, bars_1d) -> list[CheckResult]:
    from pyspark.sql import functions as F

    minute = F.expr("knowable_at IS NULL OR knowable_at < bar_time + INTERVAL 1 MINUTE")
    day = F.expr(f"knowable_at IS NULL OR knowable_at < {session_end_sql()}")
    return [
        _judge(
            BAR_CLOSED,
            "bars_1m",
            bars_1m,
            bars_1m.where(minute),
            ["venue", "symbol", "bar_time", "knowable_at"],
        ),
        _judge(
            BAR_CLOSED,
            "bars_1d",
            bars_1d,
            bars_1d.where(day),
            ["venue", "symbol", "session_date", "knowable_at"],
        ),
    ]


def filings_reported(facts, filings):
    from pyspark.sql import functions as F

    accepted = filings.select("symbol", "accession_number", "acceptance_time")
    return (
        facts.groupBy("symbol", "accession_number")
        .agg(
            F.max("form").alias("form"),
            F.max("filed_date").alias("filed_date"),
            F.max("period_end").alias("period_end"),
        )
        .join(accepted, ["symbol", "accession_number"], "left")
        .select(
            "symbol",
            "accession_number",
            "form",
            "filed_date",
            "period_end",
            F.coalesce("acceptance_time", F.expr(ny_midnight_after("filed_date"))).alias(
                "knowable_from"
            ),
            F.when(F.col("acceptance_time").isNotNull(), "acceptance_time")
            .otherwise("end_of_filing_date")
            .alias("knowable_basis"),
        )
    )


def feature_after_source(features, facts, filings, bars_1d) -> list[CheckResult]:
    from pyspark.sql import functions as F

    sources = {
        FUNDAMENTALS: filings_reported(facts, filings).select(
            F.col("symbol").alias("_entity"),
            F.col("accession_number").alias("_ref"),
            F.col("knowable_from").alias("_source_instant"),
        ),
        PRICES: bars_1d.groupBy(
            F.col("symbol").alias("_entity"), F.col("session_date").cast("string").alias("_ref")
        ).agg(F.max("knowable_at").alias("_source_instant")),
    }
    results = []
    columns = ["entity", "feature", "event_time", "knowable_at", "source_ref", "_source_instant"]
    for group in (PRICES, FUNDAMENTALS, MACRO):
        rows = features.where(F.col("feature_group") == group)
        if group == MACRO:
            instant = F.expr(ny_midnight_after("to_date(source_ref)"))
            judged = rows.withColumn("_source_instant", instant)
        else:
            judged = rows.join(
                sources[group],
                (F.col("entity") == F.col("_entity")) & (F.col("source_ref") == F.col("_ref")),
                "left",
            )
        violations = judged.where(
            F.col("_source_instant").isNull() | (F.col("knowable_at") < F.col("_source_instant"))
        )
        results.append(_judge(FEATURE_AFTER_SOURCE, group, rows, violations, columns))
    return results


def filing_visibility(features, facts, filings):
    from pyspark.sql import functions as F

    reported = features.where(F.col("feature_group") == FUNDAMENTALS)
    filed = filings_reported(facts, filings).join(
        reported.select(
            F.col("entity").alias("symbol"), F.col("source_ref").alias("accession_number")
        ).distinct(),
        ["symbol", "accession_number"],
        "left_semi",
    )
    just_before = F.col("knowable_from") - F.expr("INTERVAL 1 MICROSECOND")
    probes = {"features_before": just_before, "features_at": F.col("knowable_from")}
    spine = None
    for instant in probes.values():
        part = filed.select(F.col("symbol").alias("entity"), instant.alias(AS_OF))
        spine = part if spine is None else spine.unionByName(part)
    seen = as_of(reported, spine).select(
        F.col("entity").alias("symbol"),
        F.col(AS_OF).alias("_seen_at"),
        F.col("source_ref").alias("accession_number"),
    )
    visibility = filed
    for name, instant in probes.items():
        counts = (
            filed.select("symbol", "accession_number", instant.alias("_seen_at"))
            .join(seen, ["symbol", "accession_number", "_seen_at"])
            .groupBy("symbol", "accession_number")
            .agg(F.count("*").alias(name))
        )
        visibility = visibility.join(counts, ["symbol", "accession_number"], "left")
    visibility = visibility.select(
        "symbol",
        "accession_number",
        "form",
        "filed_date",
        "period_end",
        "knowable_from",
        "knowable_basis",
        F.datediff(F.to_date(F.from_utc_timestamp("knowable_from", NEW_YORK)), "period_end")
        .cast("int")
        .alias("lead_days"),
        *(F.coalesce(F.col(n), F.lit(0)).cast("bigint").alias(n) for n in probes),
    )
    leaked = visibility.where(F.col("features_before") > 0)
    totals = visibility.agg(
        F.count("*").alias("filings"), F.coalesce(F.sum("features_before"), F.lit(0)).alias("rows")
    ).collect()[0]
    sample = _sample(leaked, ["symbol", "accession_number", "knowable_from", "features_before"])
    result = CheckResult(
        FILING_INVISIBLE,
        FUNDAMENTALS,
        totals["filings"],
        int(totals["rows"]),
        sample if totals["rows"] else "[]",
    )
    return visibility, result


def choose_instants(candidates: Iterable[datetime], latest: int, spread: int = 0) -> list[datetime]:
    ordered = sorted(set(c for c in candidates if c is not None))
    chosen = ordered[-latest:] if latest else []
    rest = ordered[: len(ordered) - len(chosen)]
    if spread and rest:
        step = max(len(rest) // spread, 1)
        chosen += rest[::step][:spread]
    return sorted(c - JUST_BEFORE for c in set(chosen))


def audit_instants(visibility, vintages, bars_1d) -> list[datetime]:
    from pyspark.sql import functions as F

    filed = [r[0] for r in visibility.select("knowable_from").distinct().collect()]
    released = [r[0] for r in vintages.select("knowable_at").distinct().collect()]
    closed = [r[0] for r in bars_1d.agg(F.max("knowable_at")).collect()]
    return sorted(
        {
            *choose_instants(filed, latest=2, spread=1),
            *choose_instants(released, latest=2),
            *choose_instants(closed, latest=1),
        }
    )


def counterfactual(
    features, rebuild: Callable[[datetime], object], instants: Iterable[datetime]
) -> list[CheckResult]:
    from pyspark.sql import functions as F

    entities = features.where(F.col("entity") != GLOBAL_ENTITY).select("entity").distinct()
    results = []
    for instant in instants:
        spine = entities.select("entity", F.lit(instant).cast("timestamp").alias(AS_OF))
        actual = as_of(features, spine).select(*RESULT_COLUMNS)
        expected = as_of(rebuild(instant), spine).select(*RESULT_COLUMNS)
        violations = actual.exceptAll(expected).unionByName(expected.exceptAll(actual))
        results.append(
            _judge(COUNTERFACTUAL, instant.isoformat(), actual, violations, list(RESULT_COLUMNS))
        )
    return results
