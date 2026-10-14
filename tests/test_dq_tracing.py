"""Step 8: MLflow tracing of the DQ recommender (aidq_dq_agent.tracing) with fakes: the span tree a run produces, the
trace id on every write, and that tracing failures never stop the agent."""

import asyncio
import contextlib
import types

from aidq_dq_agent import graph, tracing
from aidq_dq_agent.recommend import Recommendation
from aidq_llm.router import Router

from test_dq_agent import (CANDIDATES, SUBMISSION, FakeRecommender, FakeToolBox, FakeWriteTools, ScriptChat, call,
                           reply)


class RecordingTracer:
    """Records spans as (name, kind, attributes) in order of closing; a root span yields a fixed trace id."""

    enabled, experiment_id, error = True, "1234", None

    def __init__(self):
        self.spans = []

    @contextlib.contextmanager
    def run(self, name, inputs=None):
        span = _Span(name, "AGENT", self)
        yield "trace:/workspace.aidq_traces/abc123", span
        self.spans.append(span)

    @contextlib.contextmanager
    def span(self, name, kind="TOOL", inputs=None):
        span = _Span(name, kind, self)
        yield span
        self.spans.append(span)


class _Span:
    def __init__(self, name, kind, tracer):
        self.name, self.kind, self.attrs, self.outputs = name, kind, {}, None

    def set_inputs(self, v):
        pass

    def set_outputs(self, v):
        self.outputs = v

    def set_attributes(self, v):
        self.attrs.update(v)


def test_a_run_produces_spans_for_tools_the_recommendation_and_every_write():
    tracer, wt = RecordingTracer(), FakeWriteTools()
    state = asyncio.run(graph.run(FakeToolBox(), FakeRecommender(), wt, list(CANDIDATES), trace_id="t",
                                  tracer=tracer, trace_meta={"trace_experiment_id": "1234"}))
    names = [s.name for s in tracer.spans]
    assert names[:5] == ["tool get_dq_rules", "tool get_open_and_rejected_proposals", "tool get_rejects_summary",
                         "tool profile_table", "tool profile_table"]
    assert "recommend" in names and "write submit_proposal" in names and "write report_finding" in names
    rec = next(s for s in tracer.spans if s.name == "recommend")
    assert rec.kind == "CHAIN" and rec.attrs["model"] == "model-under-test" and rec.attrs["input_tokens"] == 1
    # the trace's experiment id travels in the evidence of proposals and incidents (the review CLI links it)
    assert wt.proposals[0]["evidence"]["trace_experiment_id"] == "1234"
    assert wt.findings[0]["evidence"]["trace_experiment_id"] == "1234"
    assert state["exit_code"] == 0


def test_without_a_tracer_nothing_changes():
    wt = FakeWriteTools()
    asyncio.run(graph.run(FakeToolBox(), FakeRecommender(), wt, list(CANDIDATES), trace_id="t"))
    assert "trace_experiment_id" not in wt.proposals[0]["evidence"]


def test_model_calls_get_a_span_with_tokens_and_latency():
    tracer = RecordingTracer()
    routes = {"tasks": {"rule_generation": {"primary": "big", "fallback": None, "max_tokens": 100}}}
    router = Router(ScriptChat([reply([call("submit_recommendations", SUBMISSION, 1)])]), "a", routes=routes,
                    trace_id="t", tracer=tracer)
    asyncio.run(router.chat_for("rule_generation").complete("big", [{"role": "user", "content": "x"}]))
    [span] = tracer.spans
    assert span.name == "llm rule_generation" and span.kind == "CHAT_MODEL"
    assert span.attrs["model"] == "big" and span.attrs["input_tokens"] == 10 and span.attrs["output_tokens"] == 5
    assert "latency_ms" in span.attrs


def test_mlflow_failures_never_stop_the_agent(monkeypatch):
    broken = types.SimpleNamespace(set_tracking_uri=lambda uri: (_ for _ in ()).throw(RuntimeError("no workspace")))
    monkeypatch.setitem(__import__("sys").modules, "mlflow", broken)
    t = tracing.MlflowTracer().configure()
    assert not t.enabled and "tracing off (RuntimeError: no workspace)" in t.error
    with t.run("dq_recommender", {"x": 1}) as (trace_id, span):
        span.set_outputs({"ok": True})
        assert len(trace_id) == 32                          # an untraced uuid
    with t.span("tool x") as s:
        s.set_attributes({"a": 1})


def test_span_payloads_are_bounded():
    assert tracing._short({"a": 1}) == {"a": 1}
    big = tracing._short({"text": "x" * 10000})
    assert isinstance(big, str) and big.endswith("chars)") and len(big) < tracing.MAX_TEXT + 40


def test_trace_links():
    assert tracing.trace_link("https://h/", "12", "trace:/workspace.aidq_traces/abc") == "https://h/ml/experiments/12/traces/abc"
    assert tracing.trace_link("https://h", "12", "tr-abc") == "https://h/ml/experiments/12/traces/tr-abc"
    assert tracing.trace_link("https://h", "12", "0123456789abcdef") is None      # an untraced uuid
    assert tracing.trace_link(None, "12", "tr-abc") is None


def test_a_real_mlflow_trace_on_a_local_store(tmp_path, monkeypatch):
    import pytest

    mlflow = pytest.importorskip("mlflow")
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    monkeypatch.setenv("MLFLOW_DISABLE_AGENT_HINT", "1")
    t = tracing.MlflowTracer("dq_test", tracking_uri=tmp_path.as_uri()).configure()
    assert t.enabled, t.error
    wt = FakeWriteTools()

    async def go():
        with t.run("dq_recommender", {"write": False}) as (trace_id, root):
            state = await graph.run(FakeToolBox(), FakeRecommender(), wt, list(CANDIDATES), trace_id=trace_id,
                                    tracer=t, trace_meta={"trace_experiment_id": t.experiment_id})
            root.set_outputs({"exit_code": state["exit_code"]})
            return trace_id

    trace_id = asyncio.run(go())
    trace = mlflow.get_trace(trace_id)
    spans = {s.name: s for s in trace.data.spans}
    root = spans["dq_recommender"]
    assert root.parent_id is None
    assert {"tool profile_table", "recommend", "write submit_proposal"} <= set(spans)
    assert all(s.parent_id == root.span_id for n, s in spans.items() if n != "dq_recommender")
    assert wt.proposals[0]["evidence"]["trace_experiment_id"] == t.experiment_id
