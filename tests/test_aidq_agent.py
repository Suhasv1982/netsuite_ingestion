"""src/aidq_agent: triage rules, the investigate loop (fake Anthropic client), the graph (fake tools and store)."""

import asyncio
import copy
import datetime as dt
import json
from types import SimpleNamespace as NS

import pytest

pytest.importorskip("langgraph")
anthropic = pytest.importorskip("anthropic")

from aidq_agent import triage  # noqa: E402
from aidq_agent.graph import run  # noqa: E402
from aidq_agent.investigate import MAX_TOOL_CALLS, AnthropicInvestigator, Investigation  # noqa: E402
from aidq_agent.store import DryRunStore  # noqa: E402

NOW = dt.datetime(2026, 10, 6, 14, 45, tzinfo=dt.timezone.utc)


def job(alias, missed=(), runs=()):
    return {"job": alias, "name": f"[dev ci_dev] {alias}", "found": True,
            "schedule": {"cron": "0 0 5 * * ?", "timezone": "UTC", "pause_status": "UNPAUSED"},
            "runs": {"items": list(runs), "total": len(runs), "truncated": False},
            "missed_schedules": list(missed), "missed_schedules_unconfirmed": []}


def run_(rid, result="SUCCESS", state="TERMINATED"):
    return {"run_id": rid, "start": "2026-10-05T05:30:10+00:00", "trigger": "PERIODIC",
            "life_cycle_state": state, "result_state": result, "state_message": None}


HEALTHY = {
    "get_recent_job_runs": {"status": "ok", "jobs": [job("generator", runs=[run_(1)]), job("ingestion", runs=[run_(2)])]},
    "get_table_health": {"status": "ok", "tables": [{"source_table": "netsuite_transactions", "layer": "bronze",
                                                     "threshold_breached": None}],
                         "latest_guard": {"status": "WARN", "event_log_reads": 2}},
    "get_pipeline_errors": {"status": "ok", "pipelines": [{"pipeline": "ingestion", "name": "p", "events": {"items": []}}],
                            "failed_run_audit": {"items": []}},
    "compare_bronze_to_source": {"status": "ok", "verdict": "known_defects_only", "gaps": {"items": [
        {"table": "netsuite_transactions", "date": "2026-10-02", "source": 40, "bronze": 37, "missing": 3,
         "same_day_extra": 3, "classification": "known_defect_same_day_duplicates"}]}},
    "get_recent_deploys": {"status": "ok", "deploys": {"items": []}, "config_changes": {"unavailable": "no access"}},
}


def with_(**changes):
    r = copy.deepcopy(HEALTHY)
    r.update(changes)
    return r


MISSED = with_(get_recent_job_runs={"status": "ok", "jobs": [
    job("generator", missed=["2026-10-06T05:00:00+00:00"], runs=[run_(1)]),
    job("ingestion", missed=["2026-10-06T05:30:00+00:00"], runs=[run_(2)])]})


# -- triage --------------------------------------------------------------------

def test_healthy_day_has_no_signals_but_context_notes():
    assert triage.signals_from(HEALTHY) == []
    notes = triage.context_notes(HEALTHY)
    assert any("same-day duplicate versions on 2026-10-02" in n for n in notes)
    assert any("normal baseline" in n for n in notes)


def test_both_10_06_misses_are_one_orchestration_group_with_a_stable_fingerprint():
    groups = triage.group(triage.signals_from(MISSED))
    assert [(g.category, len(g.signals)) for g in groups] == [("ORCHESTRATION", 2)]
    again = triage.group(triage.signals_from(copy.deepcopy(MISSED)))
    assert groups[0].fingerprint == again[0].fingerprint and groups[0].fingerprint.startswith("orchestration:")


def test_unexplained_gap_failed_run_unavailable_tool_and_threshold_are_signals():
    r = with_(
        compare_bronze_to_source={"status": "ok", "gaps": {"items": [
            {"table": "netsuite_memberships", "date": "2026-10-04", "source": 10, "bronze": 8, "missing": 2,
             "same_day_extra": 0, "classification": "unexplained"}]}},
        get_recent_job_runs={"status": "ok", "jobs": [job("ingestion", runs=[run_(9, "FAILED")])]},
        get_pipeline_errors={"status": "unavailable", "reason": "endpoint disabled"},
        get_table_health={"status": "ok", "tables": [{"source_table": "netsuite_certifications", "layer": "silver",
                                                      "run_id": "9", "threshold_breached": True,
                                                      "reject_rate": 0.2, "discard_threshold": 0.05}]})
    kinds = sorted((s.kind, s.category) for s in triage.signals_from(r))
    assert kinds == [("bronze_gap", "DATA_COMPLETENESS"), ("reject_threshold", "REJECT_THRESHOLD"),
                     ("run_failed", "OTHER"), ("tool_unavailable", "SOURCE_UNAVAILABLE")]


def test_running_or_successful_runs_are_not_failures():
    r = with_(get_recent_job_runs={"status": "ok", "jobs": [job("ingestion", runs=[run_(1, None, "RUNNING"), run_(2)])]})
    assert triage.signals_from(r) == []


# -- investigate loop (fake client) ---------------------------------------------------

def block(type_, **kw):
    return NS(type=type_, **kw)


def resp(stop, *content, model="claude-opus-5-5"):
    return NS(stop_reason=stop, content=list(content), model=model, usage=NS(input_tokens=10, output_tokens=5))


RCA = {"category": "ORCHESTRATION", "summary": "No scheduled runs on 10-06.", "root_cause": "Scheduler did not fire.",
       "confidence": "medium", "evidence": [{"tool": "get_recent_job_runs", "finding": "missed 05:00 and 05:30"}],
       "not_the_cause": ["schedule paused: it is UNPAUSED"], "open_questions": ["platform status"],
       "suggested_fix": "Check the next scheduled run."}


class FakeClient:
    def __init__(self, responses):
        self.responses, self.requests = list(responses), []
        self.beta = NS(messages=NS(create=self._create))

    async def _create(self, **kw):
        self.requests.append(copy.deepcopy({k: v for k, v in kw.items() if k != "messages"}) | {"n_messages": len(kw["messages"])})
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


class FakeToolBox:
    def __init__(self, results):
        self.results, self.calls = results, []

    async def list_tools(self):
        return [{"name": n, "description": n, "input_schema": {"type": "object", "properties": {}}} for n in triage.TOOLS]

    async def call(self, name, arguments):
        self.calls.append((name, arguments))
        return self.results[name]


def investigate(client, results=MISSED):
    tb = FakeToolBox(results)
    inv = asyncio.run(AnthropicInvestigator(client=client).investigate("ORCHESTRATION", ["s1"], ["n1"], "2026-10-06", tb))
    return inv, tb


def test_loop_runs_tools_then_returns_the_rca():
    client = FakeClient([
        resp("tool_use", block("tool_use", id="t1", name="get_recent_deploys", input={"env": "dev", "days": 7})),
        resp("end_turn", block("text", text=json.dumps(RCA)))])
    inv, tb = investigate(client)
    assert inv.rca == RCA and inv.error is None
    assert inv.tool_calls == [{"tool": "get_recent_deploys", "arguments": {"env": "dev", "days": 7}, "status": "ok"}]
    req = client.requests[0]
    assert req["model"] == "claude-opus-5-5" and req["fallbacks"] == "default"
    assert req["betas"] == ["server-side-fallback-2026-07-01"]
    assert req["output_config"]["format"]["type"] == "json_schema" and "thinking" not in req
    assert inv.usage == {"input_tokens": 20, "output_tokens": 10}


def test_tool_budget_is_enforced():
    many = [block("tool_use", id=f"t{i}", name="get_table_health", input={"env": "dev"}) for i in range(MAX_TOOL_CALLS + 3)]
    client = FakeClient([resp("tool_use", *many), resp("end_turn", block("text", text=json.dumps(RCA)))])
    inv, tb = investigate(client)
    assert len(tb.calls) == MAX_TOOL_CALLS and inv.rca == RCA


@pytest.mark.parametrize("response, error", [
    (resp("refusal"), "model declined"),
    (resp("max_tokens", block("text", text="{")), "max_tokens"),
    (resp("end_turn", block("text", text="not json")), "not JSON"),
])
def test_failures_are_reported_not_raised(response, error):
    inv, _ = investigate(FakeClient([response]))
    assert inv.rca is None and error in inv.error


def test_api_errors_are_reported():
    import httpx2  # anthropic 1.x's HTTP library

    err = anthropic.APIConnectionError(request=httpx2.Request("POST", "https://api.anthropic.com/v1/messages"))
    inv, _ = investigate(FakeClient([err]))
    assert inv.rca is None and "connection error" in inv.error


# -- graph -------------------------------------------------------------------------------

class FakeInvestigator:
    def __init__(self, rca=RCA, error=None):
        self.rca, self.error, self.calls = rca, error, []

    async def investigate(self, category, signals, notes, today, toolbox):
        self.calls.append((category, signals, notes))
        return Investigation(self.rca, [{"tool": "get_recent_deploys", "arguments": {}, "status": "ok"}], self.error,
                             "claude-opus-5-5")


class IdStore(DryRunStore):
    def insert(self, incident):
        super().insert(incident)
        return 42


def graph_run(results, investigator=None, store=None):
    investigator = investigator or FakeInvestigator()
    store = store if store is not None else DryRunStore()
    state = asyncio.run(run(FakeToolBox(results), investigator, store, now=lambda: NOW, trace_id="trace-1"))
    return state, investigator, store


def test_healthy_day_writes_nothing_and_calls_no_model():
    state, inv, store = graph_run(HEALTHY)
    assert inv.calls == [] and store.inserted == [] and state["exit_code"] == 0
    assert "Healthy" in state["report"]


def test_missed_runs_produce_one_incident():
    state, inv, store = graph_run(MISSED, store=IdStore())
    assert len(inv.calls) == 1 and inv.calls[0][0] == "ORCHESTRATION"
    [inc] = store.inserted
    assert inc["category"] == "ORCHESTRATION" and inc["agent_trace_id"] == "trace-1" and inc["run_id"] == "2"
    assert inc["evidence"]["rca"] == RCA and len(inc["evidence"]["signals"]) == 2
    assert state["exit_code"] == 1 and "(incident 42)" in state["report"] and "Ruled out:" in state["report"]


def test_open_fingerprint_is_not_investigated_again():
    fp = triage.group(triage.signals_from(MISSED))[0].fingerprint
    state, inv, store = graph_run(MISSED, store=DryRunStore(open_fps={fp}))
    assert inv.calls == [] and store.inserted == [] and state["exit_code"] == 0
    assert fp in state["report"]


def test_failed_rca_still_records_the_signal():
    state, _, store = graph_run(MISSED, investigator=FakeInvestigator(rca=None, error="model declined (refusal)"),
                                store=IdStore())
    [inc] = store.inserted
    assert inc["category"] == "ORCHESTRATION" and "RCA failed: model declined" in inc["summary"]
    assert inc["suggested_fix"] is None


def test_dry_run_reports_but_exits_zero():
    state, _, store = graph_run(MISSED)
    assert len(store.inserted) == 1 and state["exit_code"] == 0 and "(not written)" in state["report"]


# -- replay toolbox, free-model helpers, eval grading -------------------------------------

def test_replay_toolbox_normalizes_defaults_and_reports_unrecorded_args():
    from aidq_agent.toolbox import ReplayToolBox

    tb = ReplayToolBox({"tools": [], "calls": [
        {"tool": "get_recent_job_runs", "arguments": {"env": "dev", "days": 3}, "result": {"status": "ok", "n": 1}}]})
    assert asyncio.run(tb.call("get_recent_job_runs", {})) == {"status": "ok", "n": 1}
    assert asyncio.run(tb.call("get_recent_job_runs", {"env": "dev", "days": 3, "job": None}))["n"] == 1
    miss = asyncio.run(tb.call("get_recent_job_runs", {"days": 9}))
    assert miss["status"] == "error" and "not recorded" in miss["error"]


def test_rca_from_text_accepts_only_complete_json():
    from aidq_agent.dbx import _rca_from_text, _text

    assert _rca_from_text("```json\n" + json.dumps(RCA) + "\n```") == RCA
    assert _rca_from_text('{"summary": "x"}') is None and _rca_from_text("no json") is None
    assert _text({"content": [{"type": "reasoning", "summary": []}, {"type": "text", "text": "hi"}]}) == "hi"


def test_free_model_investigator_loop_with_a_fake_chat():
    from aidq_agent.dbx import SUBMIT, DatabricksInvestigator

    def call(name, args, i):
        return {"id": f"c{i}", "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}

    class Chat:
        def __init__(self):
            self.n = 0

        async def complete(self, model, messages, tools=None, max_tokens=4000):
            self.n += 1
            tc = [call("get_recent_deploys", {"env": "dev"}, 1)] if self.n == 1 else [call(SUBMIT, RCA, 2)]
            return {"choices": [{"message": {"content": "", "tool_calls": tc}, "finish_reason": "tool_calls"}],
                    "usage": {"prompt_tokens": 7, "completion_tokens": 3}}

    tb = FakeToolBox(MISSED)
    inv = asyncio.run(DatabricksInvestigator(Chat()).investigate("ORCHESTRATION", ["s"], [], "2026-10-06", tb))
    assert inv.rca == RCA and [c["tool"] for c in inv.tool_calls] == ["get_recent_deploys"]
    assert inv.usage == {"input_tokens": 14, "output_tokens": 6}


def test_eval_verdicts_pass_rules():
    import sys
    import pathlib

    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "evals"))
    from run_agent_eval import apply_verdicts, grade_items

    items = grade_items({"must_identify": ["a"], "must_not_claim": ["b"]})
    got = apply_verdicts(items, {"items": [{"id": "identify_1", "verdict": "YES"}, {"id": "claim_1", "verdict": "no"}]})
    assert [g["pass"] for g in got] == [True, True]
    got = apply_verdicts(items, {"items": [{"id": "claim_1", "verdict": "yes"}]})
    assert [(g["verdict"], g["pass"]) for g in got] == [("missing", False), ("yes", False)]
    assert apply_verdicts(items, None) is None
