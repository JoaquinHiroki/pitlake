"""JSON-lines logging to stdout, which systemd forwards to the journal."""

import json
import logging
import sys
from datetime import UTC, datetime

from pitlake_collector.http import redact

_STANDARD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {
    "message",
    "asctime",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update({k: v for k, v in vars(record).items() if k not in _STANDARD_ATTRS})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # Last line of defence: urllib3's retry warnings, for one, include the request URL.
        return redact(json.dumps(payload, default=str))


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # The SDK and urllib3 log every request at DEBUG/INFO; keep the journal readable.
    for noisy in ("databricks.sdk", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
