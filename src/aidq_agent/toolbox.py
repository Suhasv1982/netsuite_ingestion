"""Access to the five aidq_mcp tools. The agent sees tools only through a ToolBox: the MCP server over stdio in
production, recorded results in evals, fakes in tests."""

from __future__ import annotations

import contextlib
import json
import os
import pathlib
import sys
from typing import Protocol

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


class ToolBox(Protocol):
    async def list_tools(self) -> list[dict]:
        """Anthropic tool definitions: [{"name", "description", "input_schema"}]."""

    async def call(self, name: str, arguments: dict) -> dict:
        """The tool's JSON result (always a dict with a status)."""


class McpToolBox:
    """Starts `python -m aidq_mcp` and talks MCP over stdio. Use as `async with McpToolBox(profile) as tb:`."""

    def __init__(self, profile: str | None):
        self.profile = profile
        self._stack = contextlib.AsyncExitStack()
        self._session = None
        self._tools: list[dict] | None = None

    async def __aenter__(self) -> "McpToolBox":
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        args = ["-m", "aidq_mcp"] + (["--profile", self.profile] if self.profile else [])
        env = {**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")}
        read, write = await self._stack.enter_async_context(
            stdio_client(StdioServerParameters(command=sys.executable, args=args, env=env)))
        self._session = await self._stack.enter_async_context(ClientSession(read, write))
        await self._session.initialize()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._stack.aclose()

    async def list_tools(self) -> list[dict]:
        if self._tools is None:
            listed = await self._session.list_tools()
            self._tools = [{"name": t.name, "description": t.description or "", "input_schema": t.input_schema}
                           for t in listed.tools]
        return self._tools

    async def call(self, name: str, arguments: dict) -> dict:
        res = await self._session.call_tool(name, arguments)
        text = "".join(getattr(c, "text", "") for c in (getattr(res, "content", None) or []))
        try:
            return json.loads(text)
        except ValueError:
            return {"status": "error", "error": f"non-JSON tool result: {text[:300]}"}


# the aidq_mcp tools' defaults, so {"days": 3} and an omitted days hit the same recording
REPLAY_DEFAULTS = {"get_recent_job_runs": {"days": 3}, "get_pipeline_errors": {"hours": 24},
                   "get_recent_deploys": {"days": 7}}


class ReplayToolBox:
    """Serves tool results recorded by evals/record_cassettes.py, keyed by (tool, canonical arguments). Arguments
    that were not recorded get an error result listing the recorded variants, like a real bad-argument error."""

    def __init__(self, cassette: dict):
        self.tools = cassette["tools"]
        self.calls: dict[str, dict] = {}
        for rec in cassette["calls"]:
            self.calls[self.key(rec["tool"], rec["arguments"])] = rec["result"]

    @staticmethod
    def key(name: str, arguments: dict) -> str:
        args = {**REPLAY_DEFAULTS.get(name, {}), "env": "dev",
                **{k: v for k, v in (arguments or {}).items() if v is not None}}
        return name + json.dumps(args, sort_keys=True)

    async def list_tools(self) -> list[dict]:
        return self.tools

    async def call(self, name: str, arguments: dict) -> dict:
        hit = self.calls.get(self.key(name, arguments))
        if hit is not None:
            return hit
        variants = sorted(k[len(name):] for k in self.calls if k.startswith(name + "{"))
        return {"status": "error", "error": f"replay: arguments {json.dumps(arguments, sort_keys=True)} were not "
                                            f"recorded for {name}; recorded: {variants[:12]}"}
