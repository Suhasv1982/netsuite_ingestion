"""The only model step: investigate one group of signals with the five read-only tools and return a structured RCA.

Manual tool-use loop (claude-api skill: manual loop when the runner does not expose what we need; here a hard cap
on tool calls and a record of every call for grading). The final answer is JSON constrained by
output_config.format. Server-side fallbacks are on ("default"): a declined request is re-run on another model.
"""

from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass, field

import anthropic

from .toolbox import ToolBox

MODEL = "claude-opus-5-5"
MAX_TOOL_CALLS = 8
CATEGORIES = ["BAD_RULE_EXPR", "SCHEMA_DRIFT", "REJECT_THRESHOLD", "SOURCE_UNAVAILABLE", "CREDENTIAL_EXPIRED",
              "DATA_VOLUME", "COMPUTE_QUOTA", "ORCHESTRATION", "DATA_COMPLETENESS", "OTHER"]

RCA_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": CATEGORIES},
        "summary": {"type": "string", "description": "one or two sentences: what is wrong"},
        "root_cause": {"type": "string", "description": "the most likely cause; say plainly if it is not verified"},
        "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
        "evidence": {"type": "array", "items": {
            "type": "object",
            "properties": {"tool": {"type": "string"}, "finding": {"type": "string"}},
            "required": ["tool", "finding"], "additionalProperties": False}},
        "not_the_cause": {"type": "array", "items": {"type": "string"},
                          "description": "plausible explanations the evidence rules out, each with the reason"},
        "open_questions": {"type": "array", "items": {"type": "string"},
                           "description": "what the tools could not verify"},
        "suggested_fix": {"type": "string", "description": "what a person should do next; nothing is done automatically"},
    },
    "required": ["category", "summary", "root_cause", "confidence", "evidence", "not_the_cause", "open_questions",
                 "suggested_fix"],
    "additionalProperties": False,
}

SYSTEM = """You investigate problems in a daily data ingestion platform (dev environment) and write a root-cause \
analysis for the people who run it.

The platform: a generator job appends one day of synthetic NetSuite data to a Postgres source at 05:00 UTC; the dev \
ingestion job (a Lakeflow pipeline: bronze -> silver -> rejects -> gold, plus a key ledger) runs at 05:30 UTC. \
Deploys go through GitHub Actions (deploy-dev on every merge to main), which also start one ingestion run.

You can only read, through the tools you are given. You cannot change, rerun or fix anything, so never say you did.

Separate what a tool result shows from what you infer. A cause counts as verified only when tool results show it; \
otherwise give it as the most likely explanation, set confidence accordingly, and list what would confirm it in \
open_questions. Ruling things out is useful: put each explanation the evidence excludes in not_the_cause with the \
reason. Do not repeat the context notes back as findings."""

DESIGN_NOTES = pathlib.Path(__file__).resolve().parents[2] / "docs" / "platform_design_notes.md"


def system_prompt() -> str:
    """SYSTEM plus the maintained platform design notes (mechanisms only, no incident diagnoses)."""
    try:
        notes = DESIGN_NOTES.read_text(encoding="utf-8")
    except OSError:
        return SYSTEM
    return f"{SYSTEM}\n\n<platform_design_notes>\n{notes}\n</platform_design_notes>"


@dataclass
class Investigation:
    rca: dict | None
    tool_calls: list[dict] = field(default_factory=list)
    error: str | None = None
    model: str | None = None
    usage: dict = field(default_factory=dict)


def _user_prompt(category: str, signals: list[str], notes: list[str], today: str) -> str:
    lines = [f"Today (UTC): {today}", f"Signal group: {category}", "Signals found by the deterministic checks:"]
    lines += [f"- {s}" for s in signals]
    if notes:
        lines += ["Context notes (known, not signals):"] + [f"- {n}" for n in notes]
    lines.append(f"Investigate with the tools (at most {MAX_TOOL_CALLS} calls), then answer with the RCA JSON.")
    return "\n".join(lines)


class AnthropicInvestigator:
    def __init__(self, client=None, model: str = MODEL, effort: str = "high"):
        self.client = client or anthropic.AsyncAnthropic()
        self.model, self.effort = model, effort

    async def investigate(self, category: str, signals: list[str], notes: list[str], today: str,
                          toolbox: ToolBox) -> Investigation:
        tools = await toolbox.list_tools()
        messages: list[dict] = [{"role": "user", "content": _user_prompt(category, signals, notes, today)}]
        calls: list[dict] = []
        usage = {"input_tokens": 0, "output_tokens": 0}
        for _ in range(MAX_TOOL_CALLS + 4):          # tool turns + the final answer, with slack for pause_turn
            try:
                resp = await self.client.beta.messages.create(
                    model=self.model, max_tokens=16000, system=system_prompt(), tools=tools, messages=messages,
                    output_config={"effort": self.effort, "format": {"type": "json_schema", "schema": RCA_SCHEMA}},
                    betas=["server-side-fallback-2026-07-01"], fallbacks="default")
            except anthropic.RateLimitError as e:
                return Investigation(None, calls, f"rate limited: {e.message}", usage=usage)
            except anthropic.APIStatusError as e:
                return Investigation(None, calls, f"API error {e.status_code}: {e.message}", usage=usage)
            except anthropic.APIConnectionError as e:
                return Investigation(None, calls, f"connection error: {e}", usage=usage)
            u = getattr(resp, "usage", None)
            for k in usage:
                usage[k] += getattr(u, k, 0) or 0
            messages.append({"role": "assistant", "content": resp.content})   # append-only: keep thinking blocks

            if resp.stop_reason == "refusal":
                return Investigation(None, calls, "model declined (refusal)", resp.model, usage)
            if resp.stop_reason == "max_tokens":
                return Investigation(None, calls, "answer cut off at max_tokens", resp.model, usage)
            if resp.stop_reason == "pause_turn":
                continue
            if resp.stop_reason == "tool_use":
                results = []
                for block in (b for b in resp.content if b.type == "tool_use"):
                    if len(calls) >= MAX_TOOL_CALLS:
                        results.append({"type": "tool_result", "tool_use_id": block.id, "is_error": True,
                                        "content": "tool budget used up: answer with the RCA JSON now"})
                        continue
                    out = await toolbox.call(block.name, dict(block.input or {}))
                    calls.append({"tool": block.name, "arguments": dict(block.input or {}), "status": out.get("status")})
                    results.append({"type": "tool_result", "tool_use_id": block.id,
                                    "content": json.dumps(out, default=str),
                                    "is_error": out.get("status") == "error"})
                messages.append({"role": "user", "content": results})
                continue
            text = next((b.text for b in reversed(resp.content) if b.type == "text"), None)
            try:
                return Investigation(json.loads(text), calls, model=resp.model, usage=usage)
            except (TypeError, ValueError):
                return Investigation(None, calls, f"final answer is not JSON: {str(text)[:200]}", resp.model, usage)
        return Investigation(None, calls, "too many turns without a final answer", usage=usage)
