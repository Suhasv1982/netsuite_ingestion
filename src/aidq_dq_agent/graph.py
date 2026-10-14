"""LangGraph wiring: collect -> recommend -> submit -> report (plan #39 step 4).

* collect (code): the four read-only DQ tools: rules, open and recently rejected proposals, the rejects summary,
  and one bronze profile per table.
* recommend (model, read tools only): one submit_recommendations, schema-checked (recommend.py).
* submit (code): each proposal and finding through the write toolbox, which checks the arguments and applies the
  no-nagging rule before it writes anything. In a dry run nothing is written.
* report: what was submitted, skipped or refused, and what the model decided against.

A store failure never loses the recommendations: they stay in the report, marked "not written", and the run ends
with EXIT_METADATA_UNAVAILABLE. A run whose answer leaves a profile candidate unanswered (also after escalation)
writes nothing and ends with EXIT_UNANSWERED.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any, Callable, TypedDict

from langgraph.graph import END, START, StateGraph

from .recommend import PROMPT_VERSION, Recommendation

EXIT_NO_RECOMMENDATION = 1      # the model gave no valid submission, also after escalation
EXIT_METADATA_UNAVAILABLE = 2   # inputs or writes failed: the report has what could not be done
EXIT_UNANSWERED = 4             # the model did not answer every candidate of the checklist: nothing written


class State(TypedDict, total=False):
    today: str
    trace_id: str
    tables: list[str]
    collected: dict[str, Any]
    collect_errors: list[str]
    recommendation: Recommendation
    submitted: list[dict]
    store_error: str
    report: str
    exit_code: int


def build_graph(toolbox, recommender, write_tools, tables: list[str],
                now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.timezone.utc)):

    async def collect(state: State) -> State:
        errors, collected = [], {}
        for name in ("get_dq_rules", "get_open_and_rejected_proposals", "get_rejects_summary"):
            r = await toolbox.call(name, {"env": "dev"})
            collected[name] = r
            if r.get("status") != "ok":
                errors.append(f"{name}: {r.get('status')} {r.get('error') or ''}".strip())
        profiles = {}
        for t in tables:
            r = await toolbox.call("profile_table", {"env": "dev", "table": t})
            profiles[t] = r
            if r.get("status") != "ok":
                errors.append(f"profile_table {t}: {r.get('status')} {r.get('error') or ''}".strip())
        collected["profiles"] = profiles
        return {"collected": collected, "collect_errors": errors, "tables": tables,
                "today": now().date().isoformat(), "trace_id": state.get("trace_id") or uuid.uuid4().hex}

    async def recommend(state: State) -> State:
        context = {"today": state["today"], "tables": [t for t in state["tables"]
                                                       if state["collected"]["profiles"][t].get("status") == "ok"],
                   **state["collected"]}
        return {"recommendation": await recommender.recommend(context, toolbox)}

    async def submit(state: State) -> State:
        out, store_error = [], None
        result = state["recommendation"].result or {}
        for p in result.get("proposals", []):
            args = {k: v for k, v in p.items() if k not in ("defect_type", "candidate_id")}
            args["evidence"] = {**(p.get("evidence") or {}), "defect_type": p.get("defect_type"),
                                "candidate_id": p.get("candidate_id"), "prompt_version": PROMPT_VERSION}
            out.append(_try("proposal", p, lambda a=args: write_tools.submit_proposal(a)))
        for f in result.get("findings", []):
            args = {k: v for k, v in f.items() if k != "candidate_id"}
            args["evidence"] = {**(f.get("evidence") or {}), "candidate_id": f.get("candidate_id")}
            out.append(_try("finding", f, lambda a=args: write_tools.report_finding(a)))
        failed = [o for o in out if o["result"].get("status") == "store_error"]
        if failed:
            store_error = failed[0]["result"]["error"]
        return {"submitted": out, **({"store_error": store_error} if store_error else {})}

    async def report(state: State) -> State:
        rec: Recommendation | None = state.get("recommendation")
        lines = [f"# aidq DQ recommender, dev, {state['today']}", "",
                 f"Prompt {PROMPT_VERSION}; trace {state['trace_id']}"
                 + (f"; model {rec.model}, {rec.usage.get('input_tokens')}/{rec.usage.get('output_tokens')} tokens, "
                    f"{len(rec.tool_calls)} extra tool call(s)" if rec else "")]
        if not write_tools_writes(write_tools):
            lines.append("Dry run: nothing written.")
        for e in state.get("collect_errors") or []:
            lines.append(f"- input unavailable: {e}")
        if rec is None:
            lines += ["", "**No profile could be read:** the model was not called."]
        elif rec.result is None:
            lines += ["", f"**No valid recommendation:** {rec.error}"]
            if rec.unanswered:
                lines += [f"**Checklist incomplete:** {len(rec.unanswered)} candidate(s) unanswered; nothing written:"]
                lines += [f"- {c}" for c in rec.unanswered]
        if state.get("store_error"):
            lines += ["", f"**METADATA UNAVAILABLE:** {state['store_error']}. What could not be written is listed below."]
        for kind, title in (("proposal", "Proposals"), ("finding", "Findings (DQ_NOT_EXPRESSIBLE)")):
            items = [s for s in state.get("submitted") or [] if s["kind"] == kind]
            if items:
                lines += ["", f"## {title}"]
            for s in items:
                lines.append(_line(s))
        dismissed = ((rec.result or {}).get("dismissed") if rec else None) or []
        if dismissed:
            lines += ["", "## Dismissed candidates"] + [f"- {d['candidate_id']}: {d['reason']}" for d in dismissed]
        if rec and rec.result is not None and not state.get("submitted"):
            lines += ["", "Nothing to recommend."]
        code = (EXIT_METADATA_UNAVAILABLE if state.get("store_error") or rec is None
                else EXIT_UNANSWERED if rec.result is None and rec.unanswered
                else EXIT_NO_RECOMMENDATION if rec.result is None else 0)
        return {"report": "\n".join(lines), "exit_code": code}

    g = StateGraph(State)
    for name, fn in (("collect", collect), ("recommend", recommend), ("submit", submit), ("report", report)):
        g.add_node(name, fn)
    g.add_edge(START, "collect")
    # nothing to recommend from when no profile could be read
    g.add_conditional_edges("collect", lambda s: "recommend" if any(
        p.get("status") == "ok" for p in s["collected"]["profiles"].values()) else "report", ["recommend", "report"])
    g.add_edge("recommend", "submit")
    g.add_edge("submit", "report")
    g.add_edge("report", END)
    return g.compile()


def write_tools_writes(write_tools) -> bool:
    return not getattr(write_tools, "dry_run", False)


def _try(kind: str, item: dict, call) -> dict:
    try:
        result = call()
    except Exception as exc:  # noqa: BLE001 -- keep the recommendation in the report
        result = {"status": "store_error", "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    return {"kind": kind, "item": item, "result": result}


def _line(s: dict) -> str:
    it, r = s["item"], s["result"]
    status = r.get("status")
    if status == "ok":
        ref = r.get("proposal_id") or r.get("incident_id")
        done = f"submitted ({'proposal ' if s['kind'] == 'proposal' else 'incident '}{ref})" if ref else "dry run: not written"
    elif status == "skipped":
        done = f"skipped: {r.get('reason')}"
    elif status == "rejected_by_code":
        done = "refused by code: " + "; ".join(r.get("problems") or [])
    else:
        done = f"NOT WRITTEN: {r.get('error')}"
    if s["kind"] == "proposal":
        ev = it.get("evidence") or {}
        return (f"- **{it.get('table')}** {it.get('rule_name')} [{it.get('severity')}, {it.get('defect_type')}, "
                f"{it.get('candidate_id')}, "
                f"confidence {it.get('confidence')}]: `{it.get('rule_expr')}`; {ev.get('violations')} of "
                f"{ev.get('rows')} rows. {done}\n  {it.get('rationale')}")
    return f"- **{it.get('table')}** {it.get('finding_type')}: {it.get('summary')} {done}"


async def run(toolbox, recommender, write_tools, tables: list[str], now=None, trace_id: str | None = None) -> dict:
    graph = build_graph(toolbox, recommender, write_tools, tables, **({"now": now} if now else {}))
    return await graph.ainvoke({"trace_id": trace_id} if trace_id else {})
