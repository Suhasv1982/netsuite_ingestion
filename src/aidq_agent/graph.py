"""LangGraph wiring: collect -> triage -> dedupe -> investigate -> write -> report (healthy days stop after triage).

Only `investigate` calls the model; writing is a fixed node, never a model tool. Dedupe runs before the model so
a problem that already has an OPEN incident costs nothing to see again.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any, Callable, TypedDict

from langgraph.graph import END, START, StateGraph

from . import triage
from .investigate import Investigation
from .store import IncidentStore
from .toolbox import ToolBox

MAX_GROUPS = 3
COLLECT_CALLS = {
    "get_recent_job_runs": {"env": "dev", "days": 3},
    "get_table_health": {"env": "dev"},
    "get_pipeline_errors": {"env": "dev", "hours": 24},
    "compare_bronze_to_source": {"env": "dev"},
    "get_recent_deploys": {"env": "dev", "days": 7},
}


class State(TypedDict, total=False):
    today: str
    trace_id: str
    results: dict[str, dict]
    signals: list[triage.Signal]
    groups: list[triage.Group]
    notes: list[str]
    already_open: list[str]
    investigations: list[dict]
    incidents: list[dict]
    report: str
    exit_code: int


def _latest_ingestion_run(results: dict) -> str | None:
    for job in results.get("get_recent_job_runs", {}).get("jobs", []):
        if job.get("job") == "ingestion":
            items = (job.get("runs") or {}).get("items") or []
            return str(items[-1]["run_id"]) if items else None
    return None


def build_graph(toolbox: ToolBox, investigator, store: IncidentStore,
                now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.timezone.utc)):

    async def collect(state: State) -> State:
        results = {name: await toolbox.call(name, args) for name, args in COLLECT_CALLS.items()}
        return {"results": results, "today": now().date().isoformat(), "trace_id": state.get("trace_id") or uuid.uuid4().hex}

    async def triage_node(state: State) -> State:
        sigs = triage.signals_from(state["results"])
        return {"signals": sigs, "groups": triage.group(sigs)[:MAX_GROUPS], "notes": triage.context_notes(state["results"])}

    async def dedupe(state: State) -> State:
        fps = [g.fingerprint for g in state["groups"]]
        already = store.open_fingerprints(fps) if fps else set()
        return {"groups": [g for g in state["groups"] if g.fingerprint not in already], "already_open": sorted(already)}

    async def investigate(state: State) -> State:
        out = []
        for g in state["groups"]:
            inv: Investigation = await investigator.investigate(
                g.category, [s.detail for s in g.signals], state["notes"], state["today"], toolbox)
            out.append({"group": g, "investigation": inv})
        return {"investigations": out}

    async def write(state: State) -> State:
        incidents = []
        run_id = _latest_ingestion_run(state["results"])
        for item in state["investigations"]:
            g, inv = item["group"], item["investigation"]
            rca = inv.rca
            if rca:
                summary = f"{rca['summary']} Root cause: {rca['root_cause']} (confidence {rca['confidence']})"
                category, fix = rca["category"], rca["suggested_fix"]
            else:   # the signal is real even when the model step failed
                summary = f"{len(g.signals)} signal(s): " + "; ".join(s.detail for s in g.signals[:5]) + \
                          f" [RCA failed: {inv.error}]"
                category, fix = g.category, None
            incident = {"run_id": run_id, "category": category, "summary": summary, "suggested_fix": fix,
                        "agent_trace_id": state["trace_id"], "fingerprint": g.fingerprint,
                        "evidence": {"signals": [s.__dict__ for s in g.signals], "rca": rca, "error": inv.error,
                                     "tool_calls": inv.tool_calls, "model": inv.model, "usage": inv.usage}}
            incident["incident_id"] = store.insert(incident)
            incidents.append(incident)
        return {"incidents": incidents}

    async def report(state: State) -> State:
        lines = [f"# aidq monitor, dev, {state['today']}", ""]
        statuses = {k: v.get("status") for k, v in state["results"].items()}
        lines.append("Tools: " + ", ".join(f"{k} {v}" for k, v in statuses.items()))
        for n in state.get("notes") or []:
            lines.append(f"- {n}")
        if state.get("already_open"):
            lines.append(f"Already open (not re-investigated): {', '.join(state['already_open'])}")
        incidents = state.get("incidents") or []
        if not state.get("signals"):
            lines += ["", "**Healthy:** no signals."]
        elif not incidents:
            lines += ["", "Signals found, all already covered by open incidents."]
        for inc in incidents:
            rca = inc["evidence"]["rca"] or {}
            lines += ["", f"## {inc['category']}  `{inc['fingerprint']}`"
                          + (f"  (incident {inc['incident_id']})" if inc.get("incident_id") else "  (not written)"),
                      inc["summary"]]
            for e in rca.get("evidence", []):
                lines.append(f"- [{e['tool']}] {e['finding']}")
            if rca.get("not_the_cause"):
                lines.append("Ruled out: " + "; ".join(rca["not_the_cause"]))
            if rca.get("open_questions"):
                lines.append("Not verified: " + "; ".join(rca["open_questions"]))
            if inc.get("suggested_fix"):
                lines.append(f"Next step: {inc['suggested_fix']}")
        new = [i for i in incidents if i.get("incident_id")]
        return {"report": "\n".join(lines), "exit_code": 1 if new else 0}

    g = StateGraph(State)
    for name, fn in (("collect", collect), ("triage", triage_node), ("dedupe", dedupe), ("investigate", investigate),
                     ("write", write), ("report", report)):
        g.add_node(name, fn)
    g.add_edge(START, "collect")
    g.add_edge("collect", "triage")
    g.add_conditional_edges("triage", lambda s: "dedupe" if s["groups"] else "report", ["dedupe", "report"])
    g.add_conditional_edges("dedupe", lambda s: "investigate" if s["groups"] else "report", ["investigate", "report"])
    g.add_edge("investigate", "write")
    g.add_edge("write", "report")
    g.add_edge("report", END)
    return g.compile()


async def run(toolbox: ToolBox, investigator, store: IncidentStore, now=None, trace_id: str | None = None) -> dict[str, Any]:
    graph = build_graph(toolbox, investigator, store, **({"now": now} if now else {}))
    return await graph.ainvoke({"trace_id": trace_id} if trace_id else {})
