import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPACES = yaml.safe_load((ROOT / "resources" / "genie.yml").read_text())["resources"]["genie_spaces"]
TABLE = re.compile(r"\$\{var\.catalog\}\.(\w+\.\w+)")


def queries(space: dict) -> list[str]:
    body = space["serialized_space"]
    examples = [e["sql"][0] for e in body["instructions"]["example_question_sqls"]]
    answers = [b["answer"][0]["content"][0] for b in body["benchmarks"]["questions"]]
    return examples + answers


def tables(space: dict) -> list[str]:
    return [
        t["identifier"].removeprefix("${var.catalog}.")
        for t in space["serialized_space"]["data_sources"]["tables"]
    ]


@pytest.mark.parametrize("key", sorted(SPACES))
def test_queries_read_only_the_tables_of_their_space(key):
    space = SPACES[key]
    used = {t for sql in queries(space) for t in TABLE.findall(sql)}
    assert used and used <= set(tables(space))


@pytest.mark.parametrize("key", sorted(SPACES))
def test_tables_and_ids_are_sorted_and_unique(key):
    body = SPACES[key]["serialized_space"]
    assert tables(SPACES[key]) == sorted(set(tables(SPACES[key])))
    lists = (
        body["config"]["sample_questions"],
        body["instructions"]["text_instructions"],
        body["instructions"]["example_question_sqls"],
        body["benchmarks"]["questions"],
    )
    for items in lists:
        ids = [item["id"] for item in items]
        assert all(re.fullmatch(r"[0-9a-f]{32}", i) for i in ids)
        assert ids == sorted(set(ids))


def test_space_ids_are_unique_across_spaces():
    ids = [
        item["id"]
        for space in SPACES.values()
        for section in ("config", "instructions", "benchmarks")
        for items in space["serialized_space"][section].values()
        for item in items
    ]
    assert len(ids) == len(set(ids))


def test_market_answers_never_see_a_value_before_it_was_knowable():
    for sql in queries(SPACES["markets_genie"]):
        if "gold.features" in sql and "TIMESTAMP'" in sql:
            assert "knowable_at <= TIMESTAMP'" in sql
            assert "ORDER BY event_time DESC, knowable_at DESC" in sql


def test_the_console_runs_both_spaces():
    console = (ROOT / "resources" / "console.yml").read_text()
    for key in SPACES:
        assert f"${{resources.genie_spaces.{key}.id}}" in console
