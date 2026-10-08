"""Free models served on the Databricks workspace (Foundation Model APIs, OpenAI-style chat completions with tools).

Used for evals (owner decision 2026-10-06: free model for eval) and usable as the investigator in place of Claude.
Calls POST <host>/serving-endpoints/<model>/invocations directly: the CLI's `serving-endpoints query` drops `tools`.
The final answer comes through a `submit_rca` tool with the RCA schema, which works the same on every model here.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request

from .investigate import MAX_TOOL_CALLS, RCA_SCHEMA, Investigation, _user_prompt, system_prompt
from .toolbox import ToolBox

DEFAULT_MODEL = "databricks-gpt-oss-120b"
SUBMIT = "submit_rca"


class DatabricksChat:
    """Chat completions as the caller: a CLI profile locally, the DATABRICKS_* variables (ci-dev, OAuth M2M) in CI.
    The SDK's Config resolves either and refreshes the token."""

    def __init__(self, profile: str | None, timeout: int = 300):
        self.profile, self.timeout = profile, timeout
        self._config = None

    def _auth(self) -> tuple[str, dict]:
        if self._config is None:
            from databricks.sdk.core import Config

            self._config = Config(profile=self.profile) if self.profile else Config()
        return self._config.host.rstrip("/"), self._config.authenticate()

    def complete_sync(self, model: str, messages: list[dict], tools: list[dict] | None = None,
                      max_tokens: int = 4000) -> dict:
        host, auth_headers = self._auth()
        body = {"messages": messages, "max_tokens": max_tokens}
        if tools:
            body["tools"] = tools
        req = urllib.request.Request(f"{host}/serving-endpoints/{model}/invocations", data=json.dumps(body).encode(),
                                     headers={**auth_headers, "Content-Type": "application/json"})
        for attempt in range(4):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return json.load(r)
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 502, 503, 504) and attempt < 3:
                    time.sleep(5 * 2 ** attempt)
                    continue
                raise RuntimeError(f"HTTP {e.code}: {e.read()[:300]!r}") from None
        raise RuntimeError("unreachable")

    async def complete(self, *args, **kwargs) -> dict:
        return await asyncio.to_thread(self.complete_sync, *args, **kwargs)


def _text(message: dict) -> str:
    """Assistant text; some models return reasoning as a JSON list of typed parts in `content`."""
    content = message.get("content")
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") == "text")
    if isinstance(content, str) and content.startswith("[{"):
        try:
            parts = json.loads(content)
            return "".join(p.get("text", "") for p in parts if isinstance(p, dict) and p.get("type") == "text")
        except ValueError:
            pass
    return content or ""


def _rca_from_text(text: str) -> dict | None:
    """An RCA given as JSON text (optionally fenced) with every required key, else None."""
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        obj = json.loads(text[start:end + 1])
    except ValueError:
        return None
    return obj if isinstance(obj, dict) and set(RCA_SCHEMA["required"]) <= set(obj) else None


def openai_tools(tools: list[dict]) -> list[dict]:
    defs = [{"type": "function", "function": {"name": t["name"], "description": t["description"],
                                              "parameters": t["input_schema"]}} for t in tools]
    defs.append({"type": "function", "function": {
        "name": SUBMIT, "description": "Submit the final root-cause analysis. Call this exactly once, at the end.",
        "parameters": RCA_SCHEMA}})
    return defs


class DatabricksInvestigator:
    def __init__(self, chat: DatabricksChat, model: str = DEFAULT_MODEL, design_notes=None):
        self.chat, self.model, self.design_notes = chat, model, design_notes

    async def investigate(self, category: str, signals: list[str], notes: list[str], today: str,
                          toolbox: ToolBox) -> Investigation:
        tools = openai_tools(await toolbox.list_tools())
        system = system_prompt(self.design_notes) + f"\n\nWhen you are done, call {SUBMIT} with the analysis (do not answer in plain text)."
        messages = [{"role": "system", "content": system},
                    {"role": "user", "content": _user_prompt(category, signals, notes, today)}]
        calls: list[dict] = []
        usage = {"input_tokens": 0, "output_tokens": 0}
        nudges = 0
        for _ in range(MAX_TOOL_CALLS + 6):
            try:
                resp = await self.chat.complete(self.model, messages, tools)
            except RuntimeError as e:
                return Investigation(None, calls, f"model call failed: {e}", self.model, usage)
            u = resp.get("usage") or {}
            usage["input_tokens"] += u.get("prompt_tokens", 0) or 0
            usage["output_tokens"] += u.get("completion_tokens", 0) or 0
            choice = resp["choices"][0]
            msg = choice["message"]
            tool_calls = msg.get("tool_calls") or []
            messages.append({"role": "assistant", "content": _text(msg), **({"tool_calls": tool_calls} if tool_calls else {})})
            if not tool_calls:
                rca = _rca_from_text(_text(msg))
                if rca is not None:    # some models answer with the JSON as text instead of calling submit_rca
                    return Investigation(rca, calls, model=self.model, usage=usage)
                if choice.get("finish_reason") == "length":
                    return Investigation(None, calls, "answer cut off at max_tokens", self.model, usage)
                if nudges >= 2:
                    return Investigation(None, calls, f"no {SUBMIT} call; last text: {_text(msg)[:200]}", self.model, usage)
                nudges += 1
                if not _text(msg).strip():     # an empty reply: drop it and ask again instead of keeping a blank turn
                    messages.pop()
                messages.append({"role": "user", "content": f"Call {SUBMIT} now with your analysis."})
                continue
            for tc in tool_calls:
                name = tc["function"]["name"]
                try:
                    args = json.loads(tc["function"].get("arguments") or "{}")
                except ValueError:
                    args = None
                if name == SUBMIT:
                    if isinstance(args, dict) and set(RCA_SCHEMA["required"]) <= set(args):
                        return Investigation(args, calls, model=self.model, usage=usage)
                    result = {"status": "error", "error": f"{SUBMIT} arguments must match the schema; required: "
                                                          f"{RCA_SCHEMA['required']}"}
                elif args is None:
                    result = {"status": "error", "error": "arguments are not valid JSON"}
                elif len(calls) >= MAX_TOOL_CALLS:
                    result = {"status": "error", "error": f"tool budget used up: call {SUBMIT} now"}
                else:
                    result = await toolbox.call(name, args)
                    calls.append({"tool": name, "arguments": args, "status": result.get("status")})
                messages.append({"role": "tool", "tool_call_id": tc["id"], "content": json.dumps(result, default=str)})
        return Investigation(None, calls, "too many turns without a final answer", self.model, usage)
