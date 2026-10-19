"""Access to the monitor's aidq_mcp tools. The agent sees tools only through a ToolBox: the MCP server over stdio in
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

    def __init__(self, profile: str | None, only: tuple[str, ...] | None = None):
        """`only`: expose just these tools to the model (the monitor sees its five, not the DQ tools)."""
        self.profile, self.only = profile, only
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
                           for t in listed.tools if self.only is None or t.name in self.only]
        return self._tools

    async def call(self, name: str, arguments: dict) -> dict:
        if self.only is not None and name not in self.only:
            return {"status": "error", "error": f"tool {name!r} is not available here"}
        res = await self._session.call_tool(name, arguments)
        text = "".join(getattr(c, "text", "") for c in (getattr(res, "content", None) or []))
        try:
            return json.loads(text)
        except ValueError:
            return {"status": "error", "error": f"non-JSON tool result: {text[:300]}"}


class LocalToolBox:
    """The aidq_mcp tools called in-process (a Databricks job: no MCP subprocess, no CLI). Same tool definitions and
    the same result dicts as the MCP server, restricted to `only`."""

    def __init__(self, tools, only: tuple[str, ...]):
        self.tools, self.only = tools, only
        self._defs: list[dict] | None = None

    async def list_tools(self) -> list[dict]:
        if self._defs is None:
            from aidq_mcp.server import build_server

            listed = await build_server(self.tools).list_tools()
            self._defs = [{"name": t.name, "description": t.description or "", "input_schema": t.input_schema}
                          for t in listed if t.name in self.only]
        return self._defs

    async def call(self, name: str, arguments: dict) -> dict:
        if name not in self.only:
            return {"status": "error", "error": f"tool {name!r} is not available here"}
        try:
            return getattr(self.tools, name)(**(arguments or {}))
        except TypeError as e:          # unexpected argument names, as the MCP server would reject them
            return {"status": "error", "error": f"bad arguments for {name}: {e}"}


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
        if not any(k.startswith(name + "{") for k in self.calls):   # a tool added after this cassette was recorded
            return {"status": "unavailable", "reason": f"{name} was not recorded in this cassette"}
        variants = sorted(k[len(name):] for k in self.calls if k.startswith(name + "{"))
        return {"status": "error", "error": f"replay: arguments {json.dumps(arguments, sort_keys=True)} were not "
                                            f"recorded for {name}; recorded: {variants[:12]}"}
