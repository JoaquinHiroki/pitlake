from pathlib import Path

import yaml

import conversation

ROOT = Path(__file__).resolve().parents[1]
SPACES = yaml.safe_load((ROOT / "resources" / "genie.yml").read_text())["resources"]["genie_spaces"]


def test_starters_are_the_sample_questions_each_space_was_deployed_with():
    for key, space in conversation.SPACES.items():
        samples = SPACES[f"{key}_genie"]["serialized_space"]["config"]["sample_questions"]
        assert space.starters == tuple(s["question"][0] for s in samples)


def test_the_console_and_the_bundle_name_the_same_space_variables():
    bundle = (ROOT / "resources" / "console.yml").read_text()
    for space in conversation.SPACES.values():
        assert f"name: {space.env}" in bundle


def test_a_completed_message_shows_text_sql_and_suggestions():
    message = {
        "conversation_id": "c1",
        "message_id": "m1",
        "content": "What was Apple's revenue?",
        "status": "COMPLETED",
        "feedback": {"rating": "POSITIVE"},
        "attachments": [
            {"attachment_id": "a1", "text": {"content": "About $85,777 million."}},
            {
                "attachment_id": "a2",
                "query": {
                    "title": "Apple revenue",
                    "description": "Latest quarter known on 2024-08-02",
                    "query": "SELECT 1",
                    "query_result_metadata": {"row_count": 1},
                },
            },
            {"attachment_id": "a3", "suggested_questions": {"questions": ["And Microsoft?"]}},
        ],
    }
    view = conversation.message_view(message)
    assert view["done"] and view["error"] is None and view["feedback"] == "POSITIVE"
    assert view["attachments"] == [
        {"kind": "text", "id": "a1", "content": "About $85,777 million."},
        {
            "kind": "query",
            "id": "a2",
            "title": "Apple revenue",
            "description": "Latest quarter known on 2024-08-02",
            "sql": "SELECT 1",
            "row_count": 1,
        },
        {"kind": "suggestions", "questions": ["And Microsoft?"]},
    ]


def test_a_message_in_progress_is_not_done_and_a_failure_carries_its_error():
    running = conversation.message_view({"id": "m1", "status": "EXECUTING_QUERY"})
    assert not running["done"] and running["message_id"] == "m1" and running["attachments"] == []
    failed = conversation.message_view(
        {"message_id": "m2", "status": "FAILED", "error": {"error": "warehouse stopped"}}
    )
    assert failed["done"] and failed["error"] == "warehouse stopped"
