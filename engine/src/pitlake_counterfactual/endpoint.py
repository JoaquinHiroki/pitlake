import importlib
import importlib.util
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from pitlake_counterfactual.agent import MAX_TOKENS, TEMPERATURE, Call

RETRIES = 4
RETRY_STATUS = (408, 409, 429, 500, 502, 503, 504)
TIMEOUT = 180


def chat(base_url: str, model: str, key: str | None, timeout: float = TIMEOUT) -> Call:
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"

    def call(messages: list[dict], tools: list[dict] | None, choice: str | None = None) -> dict:
        body = {
            "model": model,
            "messages": messages,
            "max_tokens": MAX_TOKENS,
            "temperature": TEMPERATURE,
        }
        if tools:
            body["tools"] = tools
        if choice:
            body["tool_choice"] = choice
        data = json.dumps(body).encode()
        for attempt in range(RETRIES + 1):
            request = urllib.request.Request(url, data=data, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    found = json.loads(response.read())
                break
            except urllib.error.HTTPError as error:
                if error.code not in RETRY_STATUS or attempt == RETRIES:
                    detail = error.read().decode(errors="replace")[:300]
                    raise RuntimeError(f"{url} returned {error.code}: {detail}") from error
            except urllib.error.URLError as error:
                if attempt == RETRIES:
                    raise RuntimeError(f"{url} is not reachable: {error.reason}") from error
            time.sleep(2**attempt)
        choices = found.get("choices") or []
        if not choices or not choices[0].get("message"):
            raise RuntimeError(f"{model} returned no answer")
        return choices[0]["message"]

    return call


def load(target: str):
    if ":" not in target:
        raise ValueError(f"{target!r} is not module:function or path.py:function")
    where, name = target.rsplit(":", 1)
    if where.endswith(".py"):
        path = Path(where).resolve()
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None or spec.loader is None:
            raise ValueError(f"cannot load {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[path.stem] = module
        spec.loader.exec_module(module)
    else:
        if os.getcwd() not in sys.path:
            sys.path.insert(0, os.getcwd())
        module = importlib.import_module(where)
    found = getattr(module, name, None)
    if not callable(found):
        raise ValueError(f"{target} is not a function")
    return found
