"""The only model step: read the collected profiles and summaries (and, within a budget, the read-only tools) and
return recommendations through one `submit_recommendations` call (plan #39 step 4).

The model only recommends. It has no write tool: the graph's submit node passes each recommendation through the
write toolbox (argument checks and the no-nagging rule in code), and the validator and a person decide after that.
The final answer is schema-checked here; an invalid one is sent back once per turn with the problems, like the
monitor's submit_rca. Since prompt v2 the profiles carry a candidate checklist (every check with violations > 0)
and the answer must cover every candidate as one of: a proposal, a finding, or a dismissal with a reason. A
submission that leaves a candidate unanswered is sent back; if the model never covers them all, the
recommendation fails with the unanswered ids (nothing is written). Calls go through the router (task `rule_generation`), which escalates once to the fallback
when no valid submission comes back.
"""

from __future__ import annotations

import copy
import json
import pathlib
from dataclasses import dataclass, field

from .write_tools import TOOL_DEFS

PROMPT_VERSION = "dq_recommender_v2"
PROMPT_FILE = pathlib.Path(__file__).with_name("prompts") / f"{PROMPT_VERSION}.md"
MAX_TOOL_CALLS = 6
SUBMIT = "submit_recommendations"
TASK = "rule_generation"

OTHER = "other"     # a candidate_id for a problem the checklist does not list
_candidate_ref = {"type": "string", "description": f"the id of the candidate this answers, or '{OTHER}'"}
_proposal = copy.deepcopy(TOOL_DEFS[0]["input_schema"])
_proposal["required"] += ["defect_type", "candidate_id"]
_proposal["properties"]["defect_type"] = {
    "type": "string", "description": "the kind of problem, e.g. negative_amount, end_before_start, invalid_enum"}
_proposal["properties"]["candidate_id"] = _candidate_ref
_finding = copy.deepcopy(TOOL_DEFS[1]["input_schema"])
_finding["required"].append("candidate_id")
_finding["properties"]["candidate_id"] = _candidate_ref

SUBMIT_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["proposals", "findings", "dismissed"],
    "properties": {
        "proposals": {"type": "array", "items": _proposal,
                      "description": "row-level rules (ADD_DQ_RULE), one per problem"},
        "findings": {"type": "array", "items": _finding,
                     "description": "problems a row rule cannot express (DQ_NOT_EXPRESSIBLE incidents)"},
        "dismissed": {"type": "array", "description": "candidates that are not a problem, each with the reason",
                      "items": {"type": "object", "additionalProperties": False,
                                "required": ["candidate_id", "reason"],
                                "properties": {"candidate_id": {"type": "string"}, "reason": {"type": "string"}}}},
    },
}


def schema_problems(obj, schema: dict, path: str = "submission") -> list[str]:
    """Every way `obj` breaks `schema` (the JSON-schema subset used here: types, enums, required and unexpected
    keys, numeric bounds, array items), as short messages; [] when valid."""
    t = schema.get("type")
    if t == "object":
        if not isinstance(obj, dict):
            return [f"{path} must be an object"]
        out = [f"{path}.{k} is required" for k in schema.get("required", []) if k not in obj]
        props = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            out += [f"{path}.{k} is not allowed" for k in obj if k not in props]
        for k, sub in props.items():
            if k in obj:
                out += schema_problems(obj[k], sub, f"{path}.{k}")
        return out
    if t == "array":
        if not isinstance(obj, list):
            return [f"{path} must be an array"]
        return [m for i, item in enumerate(obj) for m in schema_problems(item, schema.get("items", {}), f"{path}[{i}]")]
    if t == "string":
        if not isinstance(obj, str):
            return [f"{path} must be a string"]
        if "enum" in schema and obj not in schema["enum"]:
            return [f"{path} must be one of {schema['enum']}"]
        return []
    if t in ("integer", "number"):
        ok = isinstance(obj, int) if t == "integer" else isinstance(obj, (int, float))
        if not ok or isinstance(obj, bool):
            return [f"{path} must be {'an integer' if t == 'integer' else 'a number'}"]
        if "minimum" in schema and obj < schema["minimum"]:
            return [f"{path} must be >= {schema['minimum']}"]
        if "maximum" in schema and obj > schema["maximum"]:
            return [f"{path} must be <= {schema['maximum']}"]
    return []


def candidate_ids(context: dict) -> list[str]:
    """Every candidate of every profiled table, in order."""
    return [c["id"] for p in (context.get("profiles") or {}).values() if isinstance(p, dict)
            for c in p.get("candidates") or []]


def checklist_problems(sub: dict, ids: list[str]) -> tuple[list[str], list[str]]:
    """(problems, unanswered). Every candidate must be answered by a proposal, a finding or a dismissal. An id that
    is not a candidate (other than 'other'), a dismissal without a reason, or a candidate both dismissed and acted
    on is a problem."""
    known = set(ids)
    acted = [x.get("candidate_id") for x in (sub.get("proposals") or []) + (sub.get("findings") or [])]
    dismissed = [d.get("candidate_id") for d in sub.get("dismissed") or []]
    problems = [f"unknown candidate_id {c!r}" for c in acted + dismissed if c != OTHER and c not in known]
    problems += [f"{c} is dismissed and also proposed or reported" for c in sorted(set(acted) & set(dismissed))]
    problems += [f"dismissal of {d.get('candidate_id')} needs a reason" for d in sub.get("dismissed") or []
                 if len(str(d.get("reason") or "").strip()) < 10]
    answered = set(acted) | set(dismissed)
    return problems, [c for c in ids if c not in answered]


def system_prompt() -> str:
    return PROMPT_FILE.read_text(encoding="utf-8") + f"\n\nWhen you are done, call {SUBMIT} (do not answer in plain text)."


def user_prompt(context: dict) -> str:
    checklist = [{k: c.get(k) for k in ("id", "count", "rate", "kind")}
                 for p in (context.get("profiles") or {}).values() if isinstance(p, dict) for c in p.get("candidates") or []]
    return ("Today (UTC): " + str(context.get("today")) + "\n"
            f"Tables: {', '.join(context.get('tables', []))}\n"
            "Collected by code (the read-only tools' results):\n"
            + json.dumps({k: v for k, v in context.items() if k not in ("today", "tables")}, default=str)
            + f"\n\nCandidate checklist ({len(checklist)}; answer every one):\n" + json.dumps(checklist)
            + f"\n\nYou may call the read-only tools (at most {MAX_TOOL_CALLS} calls), then call {SUBMIT} once.")


@dataclass
class Recommendation:
    result: dict | None
    tool_calls: list[dict] = field(default_factory=list)
    error: str | None = None
    model: str | None = None
    usage: dict = field(default_factory=dict)
    prompt_version: str = PROMPT_VERSION
    unanswered: list[str] = field(default_factory=list)    # candidates the last schema-valid submission left out


def _text(message: dict) -> str:
    """Assistant text; some models return reasoning as typed parts (same handling as aidq_agent.dbx, kept here so
    the dq-agent job does not import the monitor)."""
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


def _from_text(text: str) -> dict | None:
    """A submission given as JSON text instead of the tool call, if it is complete and valid."""
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        obj = json.loads(text[start:end + 1])
    except ValueError:
        return None
    return obj if not schema_problems(obj, SUBMIT_SCHEMA) else None


def openai_tools(tools: list[dict]) -> list[dict]:
    defs = [{"type": "function", "function": {"name": t["name"], "description": t["description"],
                                              "parameters": t["input_schema"]}} for t in tools]
    defs.append({"type": "function", "function": {
        "name": SUBMIT, "description": "Submit all recommendations. Call this exactly once, at the end.",
        "parameters": SUBMIT_SCHEMA}})
    return defs


class DatabricksRecommender:
    def __init__(self, chat, model: str, max_tokens: int = 8000):
        """`chat`: anything with `complete(model, messages, tools, max_tokens=...)` (the router's LoggingChat)."""
        self.chat, self.model, self.max_tokens = chat, model, max_tokens

    async def recommend(self, context: dict, toolbox) -> Recommendation:
        ids = candidate_ids(context)
        unanswered: list[str] = []
        tools = openai_tools(await toolbox.list_tools())
        messages = [{"role": "system", "content": system_prompt()},
                    {"role": "user", "content": user_prompt(context)}]
        calls: list[dict] = []
        usage = {"input_tokens": 0, "output_tokens": 0}
        nudges = 0
        for _ in range(MAX_TOOL_CALLS + 6):
            try:
                resp = await self.chat.complete(self.model, messages, tools, max_tokens=self.max_tokens)
            except RuntimeError as e:
                return Recommendation(None, calls, f"model call failed: {e}", self.model, usage)
            u = resp.get("usage") or {}
            usage["input_tokens"] += u.get("prompt_tokens", 0) or 0
            usage["output_tokens"] += u.get("completion_tokens", 0) or 0
            choice = resp["choices"][0]
            msg = choice["message"]
            tool_calls = msg.get("tool_calls") or []
            messages.append({"role": "assistant", "content": _text(msg), **({"tool_calls": tool_calls} if tool_calls else {})})
            if not tool_calls:
                sub = _from_text(_text(msg))
                if sub is not None:
                    problems, unanswered = checklist_problems(sub, ids)
                    if not problems and not unanswered:
                        return Recommendation(sub, calls, model=self.model, usage=usage)
                if choice.get("finish_reason") == "length":
                    return Recommendation(None, calls, "answer cut off at max_tokens", self.model, usage,
                                          unanswered=unanswered)
                if nudges >= 2:
                    return Recommendation(None, calls, f"no {SUBMIT} call; last text: {_text(msg)[:200]}", self.model,
                                          usage, unanswered=unanswered)
                nudges += 1
                if not _text(msg).strip():
                    messages.pop()
                messages.append({"role": "user", "content": f"Call {SUBMIT} now, answering every candidate."})
                continue
            for tc in tool_calls:
                name = tc["function"]["name"]
                try:
                    args = json.loads(tc["function"].get("arguments") or "{}")
                except ValueError:
                    args = None
                if name == SUBMIT:
                    problems = schema_problems(args, SUBMIT_SCHEMA) if isinstance(args, dict) else ["arguments must be an object"]
                    if not problems:
                        problems, unanswered = checklist_problems(args, ids)
                        if unanswered:
                            problems.append(f"{len(unanswered)} candidate(s) unanswered: {', '.join(unanswered[:20])}"
                                            " (propose, report or dismiss each)")
                    if not problems:
                        return Recommendation(args, calls, model=self.model, usage=usage)
                    result = {"status": "error", "error": f"{SUBMIT} arguments do not match the schema: "
                                                          + "; ".join(problems[:8]) + ". Call it again, corrected."}
                elif args is None:
                    result = {"status": "error", "error": "arguments are not valid JSON"}
                elif len(calls) >= MAX_TOOL_CALLS:
                    result = {"status": "error", "error": f"tool budget used up: call {SUBMIT} now"}
                else:
                    result = await toolbox.call(name, args)
                    calls.append({"tool": name, "arguments": args, "status": result.get("status")})
                messages.append({"role": "tool", "tool_call_id": tc["id"], "content": json.dumps(result, default=str)})
        return Recommendation(None, calls, "too many turns without a final answer"
                              + (f"; {len(unanswered)} candidate(s) still unanswered" if unanswered else ""),
                              self.model, usage, unanswered=unanswered)


def recommendation_ok(rec: Recommendation) -> tuple[bool, str | None]:
    """For the router: a valid submission is success; a failed model call is an endpoint failure (error/timeout);
    anything else is a schema failure."""
    if rec.result is not None:
        return True, None
    err = rec.error or "no submission"
    if err.startswith("model call failed"):
        return False, ("timeout: " if "timed out" in err.lower() else "error: ") + err
    return False, err


class RoutedRecommender:
    """Task `rule_generation` on the routed primary, restarted once on the fallback (src/aidq_llm/routing.yml)."""

    def __init__(self, router):
        self.router = router

    async def recommend(self, context: dict, toolbox) -> Recommendation:
        max_tokens = self.router.route(TASK).max_tokens

        async def attempt(model, chat):
            return await DatabricksRecommender(chat, model, max_tokens).recommend(context, toolbox)

        return await self.router.run(TASK, attempt, recommendation_ok)
