"""src/aidq_llm: routing, one deterministic escalation, the call log (never crashes an agent), retention."""

import asyncio
import pathlib
import re

import pytest

from aidq_llm.router import CallLog, PgCallLogWriter, Router, load_routes

ROUTES = {"retention_days": 90,
          "tasks": {"root_cause": {"primary": "big", "fallback": "small", "max_tokens": 100},
                    "eval_judge": {"primary": "judge", "fallback": None, "max_tokens": 50}},
          "illustrative_prices_usd_per_1m_tokens": {"big": [1.0, 2.0]}}
RESP = {"choices": [{"message": {"content": "ok"}}], "usage": {"prompt_tokens": 1000, "completion_tokens": 500}}


class Chat:
    def __init__(self, fail=(), timeout=()):
        self.fail, self.timeout, self.calls = set(fail), set(timeout), []

    async def complete(self, model, messages, tools=None, max_tokens=4000):
        self.calls.append(model)
        if model in self.timeout:
            raise TimeoutError("timed out")
        if model in self.fail:
            raise RuntimeError("HTTP 503: down")
        return RESP


def router(chat, **kw):
    return Router(chat, "aidq_monitor", routes=ROUTES, trace_id="t1", **kw)


def go(coro):
    return asyncio.run(coro)


class TestRouting:
    def test_the_real_routes_file(self):
        r = load_routes()
        assert r["retention_days"] == 90
        assert r["tasks"]["root_cause"]["primary"] == "databricks-gpt-oss-120b"
        assert r["tasks"]["eval_judge"]["fallback"] is None          # the judge is never escalated

    def test_unknown_task_is_an_error(self):
        with pytest.raises(KeyError, match="routing.yml"):
            router(Chat()).route("nope")

    def test_pinned_model_never_escalates(self):
        r = router(Chat(fail={"pinned"}), pinned_model="pinned")
        assert r.route("root_cause").primary == "pinned" and r.route("root_cause").fallback is None
        with pytest.raises(RuntimeError):
            go(r.complete("root_cause", [{"role": "user", "content": "x"}]))


class TestEscalation:
    def test_success_on_the_primary_does_not_escalate(self):
        chat = Chat()
        r = router(chat)
        go(r.complete("root_cause", []))
        assert chat.calls == ["big"]
        [row] = r.log.rows
        assert (row.model, row.escalated, row.outcome, row.input_tokens, row.est_cost_usd) == ("big", False, "ok", 1000, 0.002)

    @pytest.mark.parametrize("kind", ["fail", "timeout"])
    def test_an_endpoint_failure_escalates_exactly_once(self, kind):
        chat = Chat(**{kind: {"big"}})
        r = router(chat)
        go(r.complete("root_cause", []))
        assert chat.calls == ["big", "small"]
        first, second = r.log.rows
        assert first.outcome == ("timeout" if kind == "timeout" else "error")
        assert second.escalated and second.escalation_reason.startswith(first.outcome) and second.outcome == "ok"

    def test_no_retry_loop_when_the_fallback_fails_too(self):
        chat = Chat(fail={"big", "small"})
        with pytest.raises(RuntimeError):
            go(router(chat).complete("root_cause", []))
        assert chat.calls == ["big", "small"]

    def test_schema_failure_escalates_the_whole_attempt_once(self):
        chat = Chat()
        r = router(chat)
        seen = []

        async def attempt(model, c):            # e.g. a monitor investigation: several calls per attempt
            await c.complete(model, [])
            await c.complete(model, [])
            seen.append(model)
            return {"model": model, "rca": None if model == "big" else {"x": 1}}

        out = go(r.run("root_cause", attempt, lambda res: (res["rca"] is not None, "no submit_rca")))
        assert seen == ["big", "small"] and out["model"] == "small"
        assert [(row.model, row.outcome, row.escalated) for row in r.log.rows] == [
            ("big", "ok", False), ("big", "schema_invalid", False), ("small", "ok", True), ("small", "ok", True)]
        assert r.log.rows[2].escalation_reason == "schema_invalid: no submit_rca"

    def test_a_judge_without_fallback_returns_the_failed_result(self):
        r = router(Chat())

        async def attempt(model, c):
            return "bad"

        assert go(r.run("eval_judge", attempt, lambda res: (False, "bad json"))) == "bad"


class TestCallLog:
    def rows(self):
        r = router(Chat())
        go(r.complete("root_cause", []))
        return r.log

    def test_flush_writes_and_prunes(self):
        class W:
            def write(self, rows, retention_days):
                self.rows, self.days = rows, retention_days
                return len(rows), 7

        w = W()
        lines = self.rows().flush(w)
        assert lines == ["LLM calls: 1 logged to llm_calls (pruned 7 older than 90 days)"] and w.days == 90
        assert {"agent", "task", "model", "outcome", "trace_id"} <= set(w.rows[0])

    def test_a_failed_write_never_raises_and_reports_the_calls(self):
        class Broken:
            def write(self, rows, retention_days):
                raise RuntimeError("metadata endpoint is disabled")

        lines = self.rows().flush(Broken())
        assert lines[0].startswith("LLM call log NOT written (RuntimeError: metadata endpoint is disabled)")
        assert "root_cause big ok 1000/500 tok" in lines[1]

    def test_dry_run_writes_nothing_but_lists_the_calls(self):
        lines = self.rows().flush(None)
        assert lines[0] == "LLM calls (not logged: dry run): 1" and len(lines) == 2

    def test_nothing_to_flush(self):
        assert CallLog().flush(None) == []


def test_pg_writer_inserts_then_prunes_with_the_retention():
    statements = []

    class Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def executemany(self, sql, rows):
            statements.append((sql, len(rows)))

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            return Cur()

        def execute(self, sql, params):
            statements.append((sql, params))
            return type("R", (), {"fetchone": lambda self: (3,)})()

        def commit(self):
            statements.append("commit")

    r = router(Chat())
    go(r.complete("root_cause", []))
    rows = [dict(vars(x)) for x in r.log.rows]
    assert PgCallLogWriter(lambda: Conn()).write(rows, 90) == (1, 3)
    assert statements[0][0].startswith("INSERT INTO aidq_metadata.llm_calls (called_at, agent, task, model")
    assert statements[1] == ("SELECT aidq_metadata.prune_llm_calls(%s)", (90,)) and statements[2] == "commit"


def test_no_agent_code_names_a_model():
    """Only src/aidq_llm/routing.yml names models: agents route by task."""
    src = pathlib.Path(__file__).resolve().parent.parent / "src"
    pattern = re.compile(r"""["']databricks-[a-z0-9.-]+["']""")
    offenders = [f"{p.relative_to(src)}:{i}" for p in src.rglob("*.py")
                 for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1) if pattern.search(line)]
    assert offenders == []


def test_monitor_flush_never_raises_and_carries_calls_into_the_report():
    import aidq_agent.__main__ as cli
    from aidq_agent.store import DryRunStore

    r = router(Chat())
    go(r.complete("root_cause", []))
    assert cli.flush_calls(r, DryRunStore())[0] == "LLM calls (not logged: dry run): 1"
    assert cli.flush_calls(None, DryRunStore()) == []


# -- paired comparison (src/aidq_llm/compare.py) ----------------------------------------------------------

from aidq_llm import compare  # noqa: E402


def res(checks=True, ident=True, claim=True):
    return {"checks": {"incident_as_expected": checks},
            "judge": [{"id": "identify_1", "kind": "identify", "pass": ident}, {"id": "claim_1", "kind": "claim", "pass": claim}]}


def test_sign_test():
    assert compare.sign_test_p(0, 0) == 1.0
    assert compare.sign_test_p(5, 0) == pytest.approx(0.0625)      # 5 of 5 discordant pairs one way
    assert compare.sign_test_p(3, 3) == 1.0


def test_an_equal_candidate_with_enough_runs_may_route():
    base = {"case": [res() for _ in range(10)]}
    ok, why = compare.decide(compare.pair(base, {"case": [res() for _ in range(10)]}))
    assert ok and why == []


def test_too_few_runs_never_route():
    base = {"case": [res() for _ in range(3)]}
    ok, why = compare.decide(compare.pair(base, {"case": [res() for _ in range(3)]}))
    assert not ok and "at least 10" in why[0]


def test_a_single_claim_failure_blocks():
    cand = [res() for _ in range(9)] + [res(claim=False)]
    ok, why = compare.decide(compare.pair({"case": [res() for _ in range(10)]}, {"case": cand}))
    assert not ok and any("claim:claim_1: candidate 9/10, target 10" in w for w in why)


def test_a_paired_regression_blocks_even_above_target():
    # candidate meets 8/10 >= target 7, but loses 3 pairs the baseline won and wins none back
    base = [res() for _ in range(10)]
    cand = [res(ident=False)] * 3 + [res()] * 7
    stats = compare.pair({"case": base}, {"case": cand})
    s = next(x for x in stats if x.item == "identify:identify_1")
    assert (s.candidate_pass, s.target, s.b, s.c) == (7, 7, 3, 0)
    ok, why = compare.decide(stats)
    assert not ok and any("worse than the baseline in 3 paired runs" in w for w in why)


# -- the monitor's investigation through the router -----------------------------------------------------

def test_routed_investigation_restarts_on_the_fallback_when_no_rca_comes_back():
    from aidq_agent.dbx import RoutedInvestigator
    from aidq_agent.investigate import Investigation

    seen = []

    class Inv:   # stands in for DatabricksInvestigator: the primary never submits an RCA
        def __init__(self, chat, model, design_notes=None):
            self.model = model

        async def investigate(self, *a):
            seen.append(self.model)
            return Investigation({"summary": "x"} if self.model == "small" else None, [], None if self.model == "small"
                                 else "no submit_rca call", self.model, {})

    import aidq_agent.dbx as dbx
    orig, dbx.DatabricksInvestigator = dbx.DatabricksInvestigator, Inv
    try:
        r = router(Chat())
        inv = go(RoutedInvestigator(r).investigate("ORCHESTRATION", [], [], "2026-10-13", None))
    finally:
        dbx.DatabricksInvestigator = orig
    assert seen == ["big", "small"] and inv.model == "small" and inv.rca == {"summary": "x"}


def test_an_endpoint_failure_caught_inside_the_attempt_is_logged_as_an_error_not_a_schema_failure():
    from aidq_agent.dbx import _rca_ok
    from aidq_agent.investigate import Investigation

    assert _rca_ok(Investigation(None, [], "model call failed: HTTP 404: no such endpoint", "m", {}))[1].startswith("error: ")
    assert _rca_ok(Investigation(None, [], "model call failed: timed out", "m", {}))[1].startswith("timeout: ")
    assert _rca_ok(Investigation(None, [], "no submit_rca call", "m", {})) == (False, "no submit_rca call")

    r = router(Chat())

    async def attempt(model, c):
        return {"model": model, "ok": model == "small"}

    go(r.run("root_cause", attempt, lambda res: (res["ok"], None if res["ok"] else "error: HTTP 404")))
    assert r.log.rows == []                         # no calls were made in this fake; just the reason path
