"""The monitor's deterministic facts step (incident 30, 2026-10-16): error markers found anywhere in the full error
text, get_platform_state (endpoints, applied config changes), aidq_agent.facts, and the facts node in the graph."""

import asyncio
import datetime as dt

import pytest

pytest.importorskip("mcp")
pytest.importorskip("langgraph")

from aidq_agent import facts  # noqa: E402
from aidq_agent.graph import run  # noqa: E402
from aidq_agent.toolbox import ReplayToolBox  # noqa: E402
from aidq_mcp import logic  # noqa: E402
from aidq_mcp.server import Tools  # noqa: E402
from netsuite_ingestion.metadata import row_hash_guard_error  # noqa: E402

from test_aidq_agent import HEALTHY, FakeInvestigator, FakeToolBox, IdStore, with_  # noqa: E402
from test_aidq_mcp_server import FakeReads  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 16, 11, 30, tzinfo=UTC)
GUARD = row_hash_guard_error("netsuite_transactions", ["a", "b"], ["a", "b", "custbody_region"], rebuild=False)
# the shape of the real failure: a long traceback whose decisive line is ~14k characters in
TRACE = "Traceback (most recent call last):\nFile \"bronze.py\", cell 1, line 8\n" + "  frame ...\n" * 1200
LONG_ERROR = f"{TRACE}RuntimeError: {GUARD}\n(guard event-log reads: 1)"


# -- markers ---------------------------------------------------------------------

def test_the_guard_line_is_found_deep_in_a_long_error():
    assert len(LONG_ERROR) > 14000 and LONG_ERROR.index("BLOCKED") > 14000
    [m] = logic.error_markers(LONG_ERROR)
    assert m["marker"] == "row_hash_guard" and m["category"] == "SCHEMA_DRIFT"
    assert m["line"].startswith("BLOCKED: the hashed column set of netsuite_transactions changed in source_columns "
                                "(added custbody_region)")
    assert "rollback" in m["meaning"] and len(m["line"]) <= 300


def test_other_markers_and_none():
    cred = "x\nCaused by: org.postgresql.util.PSQLException: ERROR: OAuth: User is not authorized\nmore"
    assert [m["marker"] for m in logic.error_markers(None, cred)] == ["credential_rejected"]
    assert [m["marker"] for m in logic.error_markers("RESOURCE_EXHAUSTED: quota")] == ["compute_quota"]
    assert logic.error_markers("Traceback ... line 8", None) == []
    assert logic.error_markers(row_hash_guard_error("t", None, ["a"], rebuild=False))[0]["marker"] == "row_hash_guard"


def test_the_full_refresh_and_refresh_mode_guards_are_told_apart():
    from netsuite_ingestion.metadata import refresh_guard_error

    full = refresh_guard_error({"full_refresh": True}, ["bronze_x"], rebuild=False, scope="pending_only")
    unread = refresh_guard_error(None, ["bronze_x"], rebuild=False, scope="pending_only")
    assert [m["marker"] for m in logic.error_markers(full)] == ["full_refresh_guard"]
    assert [m["marker"] for m in logic.error_markers(unread)] == ["refresh_mode_unreadable"]


def test_rebuild_rule_for_config_changes():
    assert logic.change_requires_rebuild({"table_name": "source_columns", "load_mode": "Incremental"})
    assert not logic.change_requires_rebuild({"table_name": "source_columns", "load_mode": "FullLoad"})
    assert not logic.change_requires_rebuild({"table_name": "data_quality_rules", "load_mode": "Incremental"})
    assert not logic.change_requires_rebuild({"table_name": "source_columns", "load_mode": "Incremental",
                                              "requires_rebuild": False})   # the validator's decision wins


# -- the tools -------------------------------------------------------------------

CHANGES = [
    {"change_id": 34, "table_name": "data_quality_rules", "operation": "INSERT",
     "changed_at": dt.datetime(2026, 10, 16, 20, 27, tzinfo=UTC), "proposal_id": 12, "proposal_type": "ADD_DQ_RULE",
     "requires_rebuild": None, "source_table": "netsuite_customers", "load_mode": "FullLoad", "column_name": None,
     "rule_name": "Company Name Not Null", "severity": "SOFT"},
    {"change_id": 33, "table_name": "source_columns", "operation": "INSERT",
     "changed_at": dt.datetime(2026, 10, 15, 19, 27, 55, tzinfo=UTC), "proposal_id": 48, "proposal_type": "ADD_COLUMN",
     "proposal_status": "APPLIED", "requires_rebuild": True, "source_table": "netsuite_transactions", "load_mode": "Incremental",
     "column_name": "custbody_region", "rule_name": None, "severity": None},
]


class GuardReads(FakeReads):
    def __init__(self, disabled_source=False, **kw):
        super().__init__(**kw)
        self.disabled_source, self.change_params = disabled_source, None

    def pipeline_events(self, pid, limit):
        if pid != "p-ingest":
            return []
        return [{"timestamp": "2026-10-15T20:03:24.722Z", "level": "ERROR", "message": "Update 5b3f7a is FAILED.",
                 "origin": {"update_id": "5b3f7a29-0000-0000-0000-000000000000"},
                 "error": {"exceptions": [{"message": LONG_ERROR}]}}]

    def pipeline_updates(self, pid, limit=25):
        return []

    def endpoint_status(self, endpoint):
        if "netsuite-sample" in endpoint and self.disabled_source:
            return {"disabled": True, "current_state": "IDLE", "last_active_time": "2026-10-11T15:00:00Z"}
        return {"disabled": False, "current_state": "ACTIVE", "last_active_time": "2026-10-16T05:42:00Z"}

    def pg_query(self, endpoint, statement, params=()):
        if self.disabled:
            return super().pg_query(endpoint, statement, params)    # raises Unavailable
        if "config_change_log" in statement:
            self.change_params = params
            since, until, _ = params
            return [c for c in CHANGES if since <= c["changed_at"] <= until]
        if "status = 'FAILED'" in statement:
            return [{"run_id": "r1", "source_table": "netsuite_transactions", "layer": "bronze", "status": "FAILED",
                     "error": LONG_ERROR, "started_at": dt.datetime(2026, 10, 15, 20, 2, tzinfo=UTC)}]
        return super().pg_query(endpoint, statement, params)


def test_pipeline_errors_carry_markers_from_the_full_text():
    out = Tools(GuardReads(), now=lambda: NOW).get_pipeline_errors("dev", 24)
    [ev] = out["pipelines"][0]["events"]["items"]
    assert len(ev["exception"]) <= 500 and "BLOCKED" not in ev["exception"]      # what the model used to see
    assert [m["marker"] for m in ev["markers"]] == ["row_hash_guard"]
    [audit] = out["failed_run_audit"]["items"]
    assert [m["marker"] for m in audit["markers"]] == ["row_hash_guard"]


def test_platform_state_lists_endpoints_and_changes_up_to_now():
    reads = GuardReads(disabled_source=True)
    out = Tools(reads, now=lambda: NOW).get_platform_state("dev")
    assert out["status"] == "ok"
    assert [(e["endpoint"], e["disabled"]) for e in out["endpoints"]] == [("source", True), ("metadata", False)]
    assert reads.change_params[0] == NOW - dt.timedelta(hours=48) and reads.change_params[1] == NOW
    [c] = out["applied_config_changes"]["items"]          # the 20:27 change on 10-16 is after `now`
    assert c["proposal_id"] == 48 and c["requires_bronze_rebuild"] is True
    assert "@" not in str(out)                            # changed_by (an email) is not selected


def test_platform_state_survives_a_disabled_metadata_endpoint():
    out = Tools(GuardReads(disabled=True), now=lambda: NOW).get_platform_state("dev")
    assert out["status"] == "ok" and "unavailable" in out["applied_config_changes"]
    assert all("disabled" in e for e in out["endpoints"])


# -- facts -----------------------------------------------------------------------

def guard_results() -> dict:
    tools = Tools(GuardReads(), now=lambda: NOW)
    return with_(get_pipeline_errors=tools.get_pipeline_errors("dev", 24),
                 get_platform_state=tools.get_platform_state("dev", 48))


def test_guard_fact_names_the_block_its_meaning_and_the_change_behind_it():
    lines = facts.marker_facts(guard_results())
    assert len(lines) == 1          # the pipeline event and the audit row carry the same line: one fact
    [line] = lines
    assert "BLOCKED: the hashed column set of netsuite_transactions changed" in line
    assert "Category: SCHEMA_DRIFT" in line and "not a code change or rollback" in line
    assert "proposal 48, ADD_COLUMN, approved and applied" in line and "failed closed as designed" in line


def test_guard_without_a_rebuild_change_says_so():
    r = guard_results()
    r["get_platform_state"]["applied_config_changes"]["items"] = []
    assert "No applied change requiring a rebuild" in facts.marker_facts(r)[0]


def test_endpoint_startup_deploy_and_change_facts():
    r = guard_results()
    r["get_recent_deploys"] = {"status": "ok", "deploys": {"items": [
        {"created": "2026-10-15T21:17:14Z", "workflow": "deploy-dev.yml", "conclusion": "success", "sha": "d5c59c7",
         "title": "STATUS 0p"}]}, "config_changes": {"unavailable": "x"}}
    lines = facts.facts_from(r, ["ENABLED Lakebase endpoint x (it was disabled; last active y): aidq monitor",
                                 "something else"])
    assert any(n.startswith("fact (from get_platform_state): Lakebase endpoints now: source enabled (ACTIVE)")
               for n in lines)
    assert any(n.startswith("fact (monitor start-up): ENABLED Lakebase endpoint x") for n in lines)
    assert not any("something else" in n for n in lines)
    assert any("deploy-dev.yml success d5c59c7" in n for n in lines)
    assert any("column netsuite_transactions.custbody_region (proposal 48 ADD_COLUMN, approved and applied); "
               "requires a bronze rebuild" in n
               for n in lines)


def runs(*states):
    return {"status": "ok", "jobs": [{"job": "ingestion", "runs": {"items": [
        {"start": f"2026-10-15T{h}:00:00+00:00", "trigger": t, "result_state": st} for h, t, st in states]}}]}


def test_recovery_facts_say_whether_a_failure_is_ongoing():
    fixed = facts.recovery_facts(with_(get_recent_job_runs=runs(("19", "ONE_TIME", "SUCCESS"),
                                                                 ("20", "ONE_TIME", "SUCCESS_WITH_FAILURES"),
                                                                 ("21", "ONE_TIME", "SUCCESS"),
                                                                 ("22", "PERIODIC", "SUCCESS"))))
    assert fixed == ["fact (from get_recent_job_runs): ingestion: last failed run started 2026-10-15T20:00:00+00:00 "
                     "(SUCCESS_WITH_FAILURES); all 2 finished run(s) since succeeded (2026-10-15T21:00:00+00:00 "
                     "ONE_TIME, 2026-10-15T22:00:00+00:00 PERIODIC): the failure is not ongoing"]
    still = facts.recovery_facts(with_(get_recent_job_runs=runs(("20", "ONE_TIME", "FAILED"), ("21", "PERIODIC", None))))
    assert "runs since: 2026-10-15T21:00:00+00:00 running" in still[0]
    assert "not known whether" in facts.recovery_facts(with_(get_recent_job_runs=runs(("20", "PERIODIC", "FAILED"))))[0]
    assert facts.recovery_facts(HEALTHY) == []


def test_no_platform_state_adds_no_facts():
    assert facts.facts_from(HEALTHY) == []      # old cassettes / fakes without get_platform_state


def test_replay_of_a_tool_added_after_the_cassette_is_unavailable_not_an_error():
    box = ReplayToolBox({"tools": [], "calls": [{"tool": "get_table_health", "arguments": {"env": "dev"},
                                                 "result": {"status": "ok"}}]})
    assert asyncio.run(box.call("get_platform_state", {"env": "dev", "hours": 48}))["status"] == "unavailable"
    assert asyncio.run(box.call("get_table_health", {"env": "dev", "table": "x"}))["status"] == "error"


# -- graph -----------------------------------------------------------------------

class NotesInvestigator(FakeInvestigator):
    async def investigate(self, category, signals, notes, today, toolbox):
        self.notes = notes
        return await super().investigate(category, signals, notes, today, toolbox)


def test_the_facts_reach_the_model_before_it_investigates():
    r = guard_results()
    tb, inv = FakeToolBox(r), NotesInvestigator()
    state = asyncio.run(run(tb, inv, IdStore(), now=lambda: NOW, startup_log=["ENABLED Lakebase endpoint m (...)"]))
    assert ("get_platform_state", {"env": "dev", "hours": 48}) in tb.calls
    assert any("row-hash guard" in n and "proposal 48" in n for n in inv.notes)
    assert any(n.startswith("fact (monitor start-up)") for n in inv.notes)
    assert "Category: SCHEMA_DRIFT" in state["report"]       # the facts are in the report too
