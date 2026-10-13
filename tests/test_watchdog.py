"""aidq_agent.watchdog: trigger today's missed dev runs (docs/watchdog_plan.md; owner checklist 2026-10-13)."""

import asyncio
import datetime as dt

import pytest

from aidq_agent import watchdog as wd

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 11, 13, 0, tzinfo=UTC)          # both jobs missed today (10-11)
GEN, ING = wd.ALLOWLIST["generator"], wd.ALLOWLIST["ingestion"]
IDS = {GEN: 1, ING: 2}
CRON = {1: "0 0 5 * * ?", 2: "0 30 5 * * ?"}


def ms(t: dt.datetime) -> int:
    return int(t.timestamp() * 1000)


class FakeJobs:
    def __init__(self, runs=None, paused=(), results=None, refuse=None, never_finish=()):
        self.runs = {1: [], 2: [], **(runs or {})}
        self.paused, self.results, self.refuse, self.never_finish = set(paused), results or {}, refuse, set(never_finish)
        self.triggered, self.tokens, self.calls = [], [], []

    def job_id(self, name):
        return IDS.get(name)

    def job(self, jid):
        return {"settings": {"schedule": {"quartz_cron_expression": CRON[jid], "timezone_id": "UTC",
                                          "pause_status": "PAUSED" if jid in self.paused else "UNPAUSED"}}}

    def runs_since(self, jid, since):
        return [r for r in self.runs[jid] if r["start_time"] >= ms(since)]

    def run_now(self, jid, idempotency_token):
        if self.refuse:
            raise RuntimeError(self.refuse)
        self.triggered.append(jid)
        self.tokens.append(idempotency_token)
        return 9000 + jid

    def run_state(self, run_id):
        jid = run_id - 9000
        self.calls.append(jid)
        if jid in self.never_finish:
            return {"life_cycle_state": "RUNNING"}
        if self.calls.count(jid) < 2:          # running once, then done
            return {"life_cycle_state": "RUNNING"}
        return {"life_cycle_state": "TERMINATED", "result_state": self.results.get(jid, "SUCCESS")}


class Clock:
    def __init__(self, t=NOW):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += dt.timedelta(seconds=s)


def act(jobs, write=True, enabled=True, now=NOW):
    clock = Clock(now)
    return wd.Watchdog(jobs, now=clock, write=write, enabled=enabled, sleep=clock.sleep, poll_s=60).act(), jobs


def run(start: dt.datetime, result="SUCCESS", state="TERMINATED"):
    return {"run_id": 1, "start_time": ms(start), "state": {"life_cycle_state": state, "result_state": result}}


def test_both_missed_generator_first_then_dev_after_success():
    r, jobs = act(FakeJobs())
    assert jobs.triggered == [1, 2]
    assert [(a.job, a.triggered, a.result) for a in r.actions] == [("generator", True, "SUCCESS"), ("ingestion", True, "SUCCESS")]
    assert jobs.tokens == ["watchdog-generator-2026-10-11", "watchdog-ingestion-2026-10-11"]
    assert r.label() == "[self-healed]"


def test_dev_is_not_triggered_when_the_generator_fails_and_nothing_is_retried():
    r, jobs = act(FakeJobs(results={1: "FAILED"}))
    assert jobs.triggered == [1]                                   # generator once, no retry, no dev
    gen, ing = r.actions
    assert gen.result == "FAILED" and ing.due and not ing.triggered and "did not end SUCCESS" in ing.reason
    assert r.label() == "[self-heal failed]"


def test_a_run_today_or_an_active_run_blocks_the_trigger():
    ran = FakeJobs(runs={1: [run(NOW.replace(hour=9))]})
    r, jobs = act(ran)
    assert jobs.triggered == [2] and "already ran today" in r.actions[0].reason   # generator ran (SUCCESS): only dev
    active = FakeJobs(runs={1: [run(NOW.replace(hour=12), result=None, state="RUNNING")]})
    r, jobs = act(active)
    assert jobs.triggered == [] and r.actions[0].reason == "a run is active"
    assert "did not end SUCCESS" in r.actions[1].reason or not r.actions[1].due


def test_yesterdays_runs_do_not_count_and_no_backfill():
    r, jobs = act(FakeJobs(runs={1: [run(NOW - dt.timedelta(days=1))], 2: [run(NOW - dt.timedelta(days=1))]}))
    assert jobs.triggered == [1, 2]
    assert len(jobs.tokens) == 2 and all(t.endswith("2026-10-11") for t in jobs.tokens)   # today only


def test_not_due_before_the_grace_period_or_when_paused():
    r, jobs = act(FakeJobs(), now=NOW.replace(hour=5, minute=20))     # generator fired 05:00, grace 30 min
    assert jobs.triggered == [] and all(not a.due for a in r.actions)
    r, jobs = act(FakeJobs(paused={1, 2}))
    assert jobs.triggered == [] and all("PAUSED" in a.reason for a in r.actions)


def test_dry_run_and_kill_switch_trigger_nothing_and_say_so():
    r, jobs = act(FakeJobs(), write=False)
    assert jobs.triggered == [] and "would trigger (dry run)" in r.actions[0].reason and r.label() is None
    r, jobs = act(FakeJobs(), enabled=False)
    assert jobs.triggered == [] and "kill switch" in r.actions[0].reason


def test_timeout_is_an_outcome_not_a_retry():
    r, jobs = act(FakeJobs(never_finish={1}))
    assert jobs.triggered == [1] and r.actions[0].result == "TIMEOUT" and not r.actions[1].triggered
    assert r.actions[0].duration_s >= wd.WAIT["generator"].total_seconds()


def test_workspace_inactive_is_reported():
    r, jobs = act(FakeJobs(refuse="Error: DENY_NEW_AND_EXISTING_RESOURCES denyReason: INACTIVE"))
    assert r.actions[0].result == "REFUSED" and "press Start in the console" in r.actions[0].detail
    assert r.label() == "[self-heal failed]"


def test_only_the_two_dev_jobs_are_allowed():
    wd.check_allowed("generator", GEN)
    for alias, name in [("generator", "netsuite_ingestion_daily"), ("ingestion", "netsuite_ingestion_daily"),
                        ("prod", "netsuite_ingestion_daily"), ("generator", "[dev suhasv] netsuite_daily_generator")]:
        with pytest.raises(PermissionError):
            wd.check_allowed(alias, name)
    assert set(wd.ALLOWLIST.values()) == {"[dev ci_dev] netsuite_daily_generator", "[dev ci_dev] netsuite_ingestion_daily"}


def test_the_incident_is_labelled_and_carries_the_outcome():
    from test_aidq_agent import MISSED, FakeInvestigator, FakeToolBox, IdStore

    from aidq_agent.graph import run as graph_run

    healed, _ = act(FakeJobs())
    store = IdStore()
    state = asyncio.run(graph_run(FakeToolBox(MISSED), FakeInvestigator(), store, now=lambda: NOW,
                                  trace_id="t", watchdog_result=healed))
    [inc] = store.inserted                                         # one ORCHESTRATION incident, not two
    assert inc["category"] == "ORCHESTRATION"
    assert inc["summary"].startswith("[self-healed] watchdog: generator SUCCESS; ingestion SUCCESS.")
    assert [a["job"] for a in inc["evidence"]["self_healed"]["actions"]] == ["generator", "ingestion"]
    assert "Watchdog: generator triggered run 9001 -> SUCCESS" in state["report"]
    assert state["exit_code"] == 1


def test_a_dry_run_watchdog_labels_nothing():
    from test_aidq_agent import MISSED, FakeInvestigator, FakeToolBox

    from aidq_agent.graph import run as graph_run
    from aidq_agent.store import DryRunStore

    dry, _ = act(FakeJobs(), write=False)
    state = asyncio.run(graph_run(FakeToolBox(MISSED), FakeInvestigator(), DryRunStore(), now=lambda: NOW,
                                  trace_id="t", watchdog_result=dry))
    assert "[self-healed]" not in state["report"] and "(dry run: nothing triggered)" in state["report"]


def test_facts_describe_each_action():
    r, _ = act(FakeJobs(results={1: "FAILED"}))
    facts = wd.facts(r)
    assert facts[0].startswith("fact (watchdog): triggered generator at 2026-10-11T13:00:00") and "FAILED" in facts[0]
    assert "ingestion not triggered" in facts[1]
