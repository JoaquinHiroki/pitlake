import json
import os
import re
import time
from datetime import UTC, date, datetime

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.backtest import GRID  # noqa: E402
from pitlake.decomposition import (  # noqa: E402
    ACROSS,
    AFTER,
    BEFORE,
    INFERRED,
    MEMORISED,
    PROBE_SYSTEM,
)
from pitlake.gold import build_features  # noqa: E402
from pitlake.harness import AGENT_REACH, SYMBOLS  # noqa: E402
from pitlake.information import feature_rows  # noqa: E402
from pitlake.information import in_reach as spark_in_reach  # noqa: E402
from pitlake.jobs.decompose import DECOMPOSED, decompose_instants, parse_args  # noqa: E402
from pitlake.tables import (  # noqa: E402
    DECOMPOSITION_SCORES,
    DECOMPOSITION_SCORES_SCHEMA,
    DECOMPOSITIONS,
    DECOMPOSITIONS_SCHEMA,
    RECALL_PROBES,
    RECALL_PROBES_SCHEMA,
)

RECORD = {
    "backtest_id": "b",
    "research_run_id": "r",
    "period_start": date(2025, 1, 2),
    "period_end": date(2025, 1, 27),
    "input_hash": "recorded",
    "params": GRID[0].to_json(),
}


def columns(schema: str) -> list[str]:
    return re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", schema)


class Collect:
    def __init__(self):
        self.rows: dict[str, list[tuple]] = {}

    def add(self, table: str, rows) -> None:
        self.rows.setdefault(table, []).extend(rows)

    def flush(self, force: bool = True) -> None:
        pass

    def named(self, table: str, schema: str) -> list[dict]:
        return [dict(zip(columns(schema), r, strict=True)) for r in self.rows.get(table, [])]


class Model:
    def __init__(self):
        self.seen: dict[str, list[str]] = {}

    def __call__(self, messages: list[dict], tools: list[dict] | None) -> dict:
        system = messages[0]["content"]
        if system == PROBE_SYSTEM:
            return {"content": "100"}
        neutral = {s: "neutral" for s in SYMBOLS}
        if tools is None:
            return {"content": json.dumps(neutral)}
        when = re.search(r"\d{4}-\d{2}-\d{2}", system)[0]
        results = [m["content"] for m in messages if m["role"] == "tool"]
        if not results:
            call = {
                "name": "price_history",
                "arguments": json.dumps({"symbol": "AAPL", "days": 30}),
            }
            return {"content": "", "tool_calls": [{"id": "1", "function": call}]}
        self.seen.setdefault(when, []).append(results[-1])
        rising = re.findall(r"from [\d,.]+ to [\d,.]+ \(\+", results[-1])
        return {"content": json.dumps({**neutral, "AAPL": "overweight" if rising else "neutral"})}


def test_the_tooled_agent_reads_the_real_world_at_each_instant_and_is_decomposed(spark, world):
    features = build_features(spark, world["bars_1d"], world["facts"], world["vintages"])
    rows = feature_rows(spark_in_reach(features, AGENT_REACH))
    args = parse_args(
        [
            "--catalog",
            "c",
            "--endpoint",
            "e",
            "--cutoff",
            "2025-01-14",
            "--days",
            "7",
            "--at",
            "2025-01-06 2025-01-14",
            "--samples",
            "2",
            "--run-id",
            "run",
        ]  # fmt: skip
    )
    model, writer = Model(), Collect()
    found = decompose_instants(
        RECORD,
        rows,
        {"gold.features": 1},
        rows,
        model,
        {},
        {},
        args,
        writer,
        time.monotonic() + 600,
        datetime(2026, 11, 13, tzinfo=UTC),
    )
    assert [i.state for i in found.instants] == [DECOMPOSED, DECOMPOSED]
    assert [i.era for i in found.instants] == [BEFORE, AFTER]
    assert set(model.seen) == {"2025-01-06", "2025-01-14"}
    for when, texts in model.seen.items():
        last = max(re.findall(r"(\d{4}-\d{2}-\d{2}) [\d,.]+", texts[0].split("Daily closes:")[1]))
        assert last < when
    assert max(re.findall(r"\d{4}-\d{2}-\d{2}", model.seen["2025-01-06"][0])) == "2025-01-05"
    probes = writer.named(RECALL_PROBES, RECALL_PROBES_SCHEMA)
    assert {p["symbol"] for p in probes} == {"AAPL", "BTCUSDT"}
    scores = writer.named(DECOMPOSITION_SCORES, DECOMPOSITION_SCORES_SCHEMA)
    assert [s["era"] for s in scores] == [BEFORE, AFTER]
    assert set(scores[0]["returns"]) == {"AAPL", "BTCUSDT"}
    assert all(s["blind"] == 0.0 == s["recall"] for s in scores)
    rows = writer.named(DECOMPOSITIONS, DECOMPOSITIONS_SCHEMA)
    decomposed = {(r["component"], r["era"]): r for r in rows}
    assert decomposed[MEMORISED, ACROSS]["estimate"] == 0.0
    assert decomposed[INFERRED, AFTER]["estimate"] == pytest.approx(scores[1]["tooled"])
    assert decomposed[INFERRED, AFTER]["low"] is None
