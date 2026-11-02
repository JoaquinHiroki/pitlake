from dataclasses import dataclass

TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}


@dataclass(frozen=True)
class Space:
    title: str
    env: str
    caption: str
    starters: tuple[str, ...]


SPACES = {
    "markets": Space(
        "Markets",
        "GENIE_MARKETS_SPACE_ID",
        "Prices, company results and macro data as known at the instant you name; a date means "
        "00:00 UTC. Genie reads the latest Gold tables, so research still goes through "
        "pitlake.pit on committed versions (ADR 0014).",
        (
            "What was Apple's latest quarterly revenue as known on 2024-08-02?",
            "Show NVIDIA's quarterly net income as first reported",
            "On which days did the Binance and Coinbase bitcoin closes differ most?",
        ),
    ),
    "operations": Space(
        "Operations",
        "GENIE_OPERATIONS_SPACE_ID",
        "Costs, query statistics, freshness, drift, rules, the gate and backtests.",
        (
            "What did each job cost over the last 7 days?",
            "Is any Silver table stale?",
            "How many rows did the prod transform read per day?",
        ),
    ),
}


def attachment_views(attachments: list[dict]) -> list[dict]:
    views = []
    for attachment in attachments:
        attachment_id = attachment.get("attachment_id")
        if text := attachment.get("text"):
            views.append({"kind": "text", "id": attachment_id, "content": text.get("content", "")})
        if query := attachment.get("query"):
            views.append(
                {
                    "kind": "query",
                    "id": attachment_id,
                    "title": query.get("title"),
                    "description": query.get("description"),
                    "sql": query.get("query", ""),
                    "row_count": query.get("query_result_metadata", {}).get("row_count"),
                }
            )
        if suggested := attachment.get("suggested_questions"):
            views.append({"kind": "suggestions", "questions": suggested.get("questions", [])})
    return views


def message_view(message: dict) -> dict:
    status = message.get("status", "SUBMITTED")
    error = message.get("error") or {}
    return {
        "conversation_id": message.get("conversation_id"),
        "message_id": message.get("message_id") or message.get("id"),
        "question": message.get("content", ""),
        "status": status,
        "done": status in TERMINAL,
        "error": error.get("error") or error.get("type"),
        "feedback": (message.get("feedback") or {}).get("rating"),
        "created": message.get("created_timestamp"),
        "attachments": attachment_views(message.get("attachments") or []),
    }


def conversation_summary(conversation: dict) -> dict:
    return {
        "conversation_id": conversation["conversation_id"],
        "title": conversation.get("title") or "Untitled",
        "created": conversation.get("created_timestamp"),
    }
