"""HTTP with the manners the sources expect: throttled, retried with backoff, streamed."""

import hashlib
import os
import re
import threading
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_CHUNK = 1024 * 1024


# Query parameters that carry credentials. FRED takes its key in the URL, and requests and urllib3
# put the full URL into exception messages, so anything that may reach a log goes through redact().
_CREDENTIAL = re.compile(r"(?i)\b(api_key|apikey|token|secret|password)=[^&\s'\"]+")


def redact(text: str) -> str:
    return _CREDENTIAL.sub(r"\1=REDACTED", text)


class NotFound(Exception):
    pass


class HttpError(Exception):
    """A request failed. The message is redacted, and the original exception is not chained."""


class HttpClient:
    def __init__(
        self,
        user_agent: str,
        min_interval_seconds: float = 0.5,
        retries: int = 5,
        backoff_factor: float = 2.0,
        timeout: tuple[float, float] = (10.0, 120.0),
    ) -> None:
        retry = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),
            respect_retry_after_header=True,
        )
        self._session = requests.Session()
        self._session.headers["User-Agent"] = user_agent
        self._session.mount("https://", HTTPAdapter(max_retries=retry))
        self._timeout = timeout
        self._min_interval = min_interval_seconds
        self._lock = threading.Lock()
        self._last_request = 0.0

    def _throttle(self) -> None:
        with self._lock:
            wait = self._last_request + self._min_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

    def _get(self, url: str, stream: bool) -> requests.Response:
        self._throttle()
        try:
            response = self._session.get(url, stream=stream, timeout=self._timeout)
            if response.status_code == 404:
                response.close()
                raise NotFound(redact(url))
            response.raise_for_status()
        except requests.RequestException as exc:
            # `from None`: the chained exception would print the unredacted URL in a traceback.
            raise HttpError(redact(str(exc))) from None
        return response

    def get_text(self, url: str) -> str:
        return self._get(url, stream=False).text

    def get_bytes(self, url: str) -> bytes:
        """The response body exactly as served, for sources whose raw form is the response."""
        response = self._get(url, stream=False)
        try:
            return response.content
        except requests.RequestException as exc:
            raise HttpError(redact(str(exc))) from None

    def download(self, url: str, dest: Path) -> tuple[str, int]:
        """Stream url to dest, returning (sha256, size). dest only exists if complete."""
        partial = dest.with_name(dest.name + ".part")
        digest = hashlib.sha256()
        size = 0
        try:
            with self._get(url, stream=True) as response, open(partial, "wb") as fh:
                for chunk in response.iter_content(_CHUNK):
                    fh.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                fh.flush()
                os.fsync(fh.fileno())
            partial.replace(dest)
        except requests.RequestException as exc:
            raise HttpError(redact(str(exc))) from None
        finally:
            partial.unlink(missing_ok=True)
        return digest.hexdigest(), size
