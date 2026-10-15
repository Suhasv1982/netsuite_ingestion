"""The schema drift agent's graph: detect -> (review by the model, only if there is drift) -> submit -> report.

* detect (code): detect.read_drift, plus a profile of each added column on the source (null rate, distinct count;
  aggregates only) for the evidence.
* review (model, routed task `drift_rationale`): one `submit_drift_review` with a rationale, relevance and confidence
  for every change; schema-checked, and every change must be answered (else sent back; never completed -> the run
  fails and writes nothing). No model call when there is no drift.
* submit (code): added -> ADD_COLUMN (type from detect.TYPE_MAP, next free ordinal); removed -> DEACTIVATE_COLUMN,
  except the business key / watermark column -> SCHEMA_DRIFT incident; type_changed -> SCHEMA_TYPE_CHANGE incident;
  unmappable added type -> SCHEMA_DRIFT incident; unonboarded table -> SCHEMA_DRIFT incident (onboarding hand-off).
  For incremental tables the rationale always states that approval requires a bronze rebuild (code adds the sentence
  if the model left it out); for full-load tables it never does (owner decision 2026-10-15).
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
from dataclasses import dataclass, field
from typing import Any

from aidq_dq_agent.recommend import schema_problems

from .detect import Change, read_drift

PROMPT_VERSION = "drift_rationale_v1"
PROMPT_FILE = pathlib.Path(__file__).with_name("prompts") / f"{PROMPT_VERSION}.md"
TASK = "drift_rationale"
SUBMIT = "submit_drift_review"
REBUILD_SENTENCE = ("Approval requires a bronze rebuild: this is an incremental table, so the next run fails closed "
                    "until the dev rebuild runs (deploy-dev with bronze_rebuild=true).")
EXIT_NO_REVIEW, EXIT_STORE = 1, 2

SUBMIT_SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["reviews"],
    "properties": {"reviews": {"type": "array", "items": {
        "type": "object", "additionalProperties": False,
        "required": ["change_id", "rationale", "relevance", "confidence"],
        "properties": {"change_id": {"type": "string"}, "rationale": {"type": "string"},
                       "relevance": {"type": "string", "enum": ["high", "medium", "low"]},
                       "confidence": {"type": "number", "minimum": 0, "maximum": 1}}}}},
}
PROFILE_SQL = 'SELECT count(*) AS n, count("{c}") AS non_null, count(DISTINCT "{c}") AS distinct_values FROM netsuite."{t}"'


@dataclass
class Review:
    result: dict | None
    error: str | None = None
    model: str | None = None
    usage: dict = field(default_factory=dict)
    unanswered: list[str] = field(default_factory=list)


def review_problems(sub: Any, ids: list[str]) -> tuple[list[str], list[str]]:
    """(problems, unanswered): schema, unknown ids, duplicates, and the changes without a review."""
    problems = schema_problems(sub, SUBMIT_SCHEMA)
    if problems:
        return problems, list(ids)
    seen = [r["change_id"] for r in sub["reviews"]]
    problems = [f"unknown change_id {c!r}" for c in seen if c not in ids]
    problems += [f"change {c} reviewed twice" for c in sorted({c for c in seen if seen.count(c) > 1})]
    problems += [f"rationale of {r['change_id']} is too short" for r in sub["reviews"] if len(r["rationale"].strip()) < 20]
    return problems, [c for c in ids if c not in seen]


def profile_added(pg_query, source_endpoint: str, changes: list[Change]) -> dict[str, dict]:
    """change id -> {rows, null_rate, distinct_values} of each added column, on the source (the caller's SELECT)."""
    out = {}
    for c in changes:
        if c.kind != "added":
            continue
        try:
            [r] = pg_query(source_endpoint, PROFILE_SQL.format(c=c.column.replace('"', '""'), t=c.table.replace('"', '""')))
            n = int(r["n"] or 0)
            out[c.id] = {"rows": n, "null_rate": round(1 - int(r["non_null"] or 0) / n, 4) if n else None,
                         "distinct_values": int(r["distinct_values"] or 0)}
        except Exception as exc:  # noqa: BLE001 -- evidence is best effort
            out[c.id] = {"error": f"{type(exc).__name__}: {str(exc)[:120]}"}
    return out


class ModelReviewer:
    """The model step through the router (task drift_rationale; escalates once like the other agents)."""

    def __init__(self, router):
        self.router = router

    async def review(self, changes: list[Change], profiles: dict) -> Review:
        ids = [c.id for c in changes]
        tool = {"type": "function", "function": {"name": SUBMIT, "description": "Submit one review per change.",
                                                 "parameters": SUBMIT_SCHEMA}}
        user = ("Changes found (answer every one):\n" + json.dumps([c.as_dict() for c in changes])
                + "\n\nProfiles of added columns (source, aggregates):\n" + json.dumps(profiles)
                + f"\n\nCall {SUBMIT} once.")
        max_tokens = self.router.route(TASK).max_tokens

        async def attempt(model, chat):
            messages = [{"role": "system", "content": PROMPT_FILE.read_text(encoding="utf-8")},
                        {"role": "user", "content": user}]
            usage, unanswered, last_error = {"input_tokens": 0, "output_tokens": 0}, list(ids), "no submission"
            for _ in range(4):
                try:
                    resp = await chat.complete(model, messages, [tool], max_tokens=max_tokens)
                except RuntimeError as e:
                    return Review(None, f"model call failed: {e}", model, usage, unanswered)
                u = resp.get("usage") or {}
                usage["input_tokens"] += u.get("prompt_tokens", 0) or 0
                usage["output_tokens"] += u.get("completion_tokens", 0) or 0
                msg = resp["choices"][0]["message"]
                calls = msg.get("tool_calls") or []
                messages.append({"role": "assistant", "content": msg.get("content") or "",
                                 **({"tool_calls": calls} if calls else {})})
                if not calls:
                    messages.append({"role": "user", "content": f"Call {SUBMIT} now with a review of every change."})
                    continue
                for tc in calls:
                    try:
                        args = json.loads(tc["function"].get("arguments") or "{}")
                    except ValueError:
                        args = None
                    problems, unanswered = review_problems(args, ids)
                    if not problems and not unanswered:
                        return Review(args, None, model, usage)
                    last_error = "; ".join(problems[:6] + ([f"unanswered: {', '.join(unanswered)}"] if unanswered else []))
                    messages.append({"role": "tool", "tool_call_id": tc["id"],
                                     "content": json.dumps({"status": "error", "error": last_error + ". Call again."})})
            return Review(None, f"no valid {SUBMIT}: {last_error}", model, usage, unanswered)

        def ok(r: Review):
            if r.result is not None:
                return True, None
            err = r.error or "no review"
            return False, (("timeout: " if "timed out" in err.lower() else "error: ") + err
                           if err.startswith("model call failed") else err)

        return await self.router.run(TASK, attempt, ok)


def rationale_for(c: Change, model_text: str) -> str:
    text = model_text.strip()
    if c.incremental and c.kind in ("added", "removed") and "rebuild" not in text.lower():
        text += " " + REBUILD_SENTENCE
    return text


def impact_of(get_impact, c: Change) -> dict | str:
    """The lineage impact for the evidence (plan #70's get_impact): for a removed or retyped column, the column's
    downstream; for an added column or a table, the table's (what consumes the table today). Best effort."""
    if get_impact is None:
        return "not available (no lineage graph)"
    obj = f"{c.table}.{c.column}" if c.kind in ("removed", "type_changed") else c.table
    try:
        out = get_impact("dev", obj)
    except Exception as exc:  # noqa: BLE001 -- evidence only
        return f"not available ({type(exc).__name__}: {str(exc)[:100]})"
    if out.get("status") != "ok":
        return f"not available ({out.get('error') or out.get('reason')})"
    nodes = (out.get("nodes") or {}).get("items", [])
    return {"object": out["object"], "counts": out.get("counts"),
            "gold_and_rules": [n["node"] for n in nodes if n["node"].startswith(("gold:", "rule:"))],
            "source": "get_impact (lineage graph, workspace.knowledge)"}


def submit(changes: list[Change], reviews: dict[str, dict], profiles: dict, write_tools, meta: dict,
           get_impact=None) -> list[dict]:
    """Every change through the write toolbox; returns [{change, kind, result}]."""
    out = []
    for c in changes:
        r = reviews[c.id]
        evidence = {"change": c.as_dict(), "profile": profiles.get(c.id), "relevance": r["relevance"],
                    "impact": impact_of(get_impact, c), **meta}
        text = rationale_for(c, r["rationale"])
        if c.kind == "added" and c.proposed_type:
            res = write_tools.submit_column_proposal({"proposal_type": "ADD_COLUMN", "table": c.table,
                                                      "column_name": c.column, "data_type": c.proposed_type,
                                                      "ordinal": c.ordinal, "rationale": text, "evidence": evidence,
                                                      "confidence": r["confidence"]})
            out.append({"change": c, "written": "ADD_COLUMN proposal", "result": res, "evidence": evidence})
        elif c.kind == "removed" and not c.key_column:
            res = write_tools.submit_column_proposal({"proposal_type": "DEACTIVATE_COLUMN", "table": c.table,
                                                      "column_name": c.column, "rationale": text,
                                                      "evidence": evidence, "confidence": r["confidence"]})
            out.append({"change": c, "written": "DEACTIVATE_COLUMN proposal", "result": res, "evidence": evidence})
        else:
            category = "SCHEMA_TYPE_CHANGE" if c.kind == "type_changed" else "SCHEMA_DRIFT"
            why = {"type_changed": "type changed", "removed": "the business key or watermark column is gone",
                   "added": f"source type {c.source_type!r} has no mapping", "unonboarded": "not onboarded"}[c.kind]
            res = write_tools.report_incident({
                "category": category, "table": c.table, "fingerprint": f"drift:{c.id}",
                "summary": f"{c.table}{'.' + c.column if c.column else ''}: {why}. {text}",
                "suggested_fix": ("onboard the table (onboarding agent)" if c.kind == "unonboarded"
                                  else "decide by hand; no proposal is made for this change"),
                "evidence": evidence})
            out.append({"change": c, "written": f"{category} incident", "result": res, "evidence": evidence})
    return out


async def run(pg_query, source_endpoint: str, metadata_endpoint: str, reviewer, write_tools, trace_id: str,
              meta: dict | None = None, now=None, tracer=None, get_impact=None) -> dict:
    """One run. Returns {report, exit_code, changes, submitted}."""
    from aidq_dq_agent.tracing import NullTracer

    tracer = tracer or NullTracer()
    today = (now or dt.datetime.now(dt.timezone.utc)).date().isoformat()
    with tracer.span("detect", "TOOL", {}) as span:
        changes = read_drift(pg_query, source_endpoint, metadata_endpoint)
        profiles = profile_added(pg_query, source_endpoint, changes)
        span.set_outputs({"changes": [c.as_dict() for c in changes], "profiles": profiles})
    lines = [f"# aidq schema drift, dev, {today}", "", f"Prompt {PROMPT_VERSION}; trace {trace_id}"]
    if getattr(write_tools, "dry_run", False):
        lines.append("Dry run: nothing written.")
    if not changes:
        lines += ["", "**No drift:** the source matches the metadata. No model call."]
        return {"report": "\n".join(lines), "exit_code": 0, "changes": [], "submitted": []}
    with tracer.span("review", "CHAIN", {"changes": len(changes)}) as span:
        rev = await reviewer.review(changes, profiles)
        span.set_attributes({"model": rev.model, "error": rev.error})
    lines.append(f"Model {rev.model}, {rev.usage.get('input_tokens')}/{rev.usage.get('output_tokens')} tokens")
    if rev.result is None:
        lines += ["", f"**No valid review:** {rev.error}. Nothing written."]
        return {"report": "\n".join(lines), "exit_code": EXIT_NO_REVIEW, "changes": changes, "submitted": []}
    reviews = {r["change_id"]: r for r in rev.result["reviews"]}
    submitted = []
    store_error = None
    with tracer.span("submit", "TOOL", {"changes": len(changes)}) as span:
        try:
            submitted = submit(changes, reviews, profiles, write_tools, {"prompt_version": PROMPT_VERSION, **(meta or {})},
                               get_impact)
        except Exception as exc:  # noqa: BLE001 -- keep the review in the report
            store_error = f"{type(exc).__name__}: {str(exc)[:300]}"
        span.set_outputs([{"change": s["change"].id, "result": s["result"]} for s in submitted])
    lines += ["", "## Changes"]
    for c in changes:
        s = next((x for x in submitted if x["change"].id == c.id), None)
        res = s["result"] if s else {}
        status = res.get("status")
        done = (f"{s['written']} " + (str(res.get("proposal_id") or res.get("incident_id") or "(dry run)")
                                      if status == "ok" else f"{status}: {res.get('reason') or res.get('problems')}")
                if s else "NOT WRITTEN")
        lines.append(f"- **{c.id}** ({'incremental' if c.incremental else 'full load'}, relevance "
                     f"{reviews[c.id]['relevance']}): {done}\n  {rationale_for(c, reviews[c.id]['rationale'])}")
    if store_error:
        lines += ["", f"**METADATA UNAVAILABLE:** {store_error}"]
    return {"report": "\n".join(lines), "exit_code": EXIT_STORE if store_error else 0, "changes": changes,
            "submitted": submitted}
