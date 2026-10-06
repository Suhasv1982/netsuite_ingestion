"""src/aidq_mcp/server.py with faked reads: tool registration, both incident fixtures replayed, failure modes."""

import asyncio
import datetime as dt
import json

import pytest

pytest.importorskip("mcp")

from aidq_mcp import logic  # noqa: E402
from aidq_mcp.reads import Unavailable  # noqa: E402
from aidq_mcp.server import Tools, build_server  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 6, 14, 45, tzinfo=UTC)
JOBS = {"[dev ci_dev] netsuite_daily_generator": 1, "[dev ci_dev] netsuite_ingestion_daily": 2,
        "[dev ci_dev] guard_canary_check": 3}
CRON = {1: "0 0 5 * * ?", 2: "0 30 5 * * ?"}


def ms(day, h, m, s=10):
    return int(dt.datetime(2026, 10, day, h, m, s, tzinfo=UTC).timestamp() * 1000)


class FakeReads:
    def __init__(self, disabled=False):
        self.disabled = disabled
        self.sql_calls = []

    def current_user(self):
        return "owner-user"

    def service_principals(self):
        return {"app-ci-dev": "ci-dev", "app-other": "olist-bi-ml"}

    def job_id(self, name):
        return JOBS.get(name)

    def job_settings(self, jid):
        return {"schedule": {"quartz_cron_expression": CRON[jid], "timezone_id": "UTC", "pause_status": "UNPAUSED"}} \
            if jid in CRON else {}

    def job_runs(self, jid, since):
        if jid not in CRON:
            return []
        h, m = (5, 0) if jid == 1 else (5, 30)
        return [{"run_id": 100 * jid + d, "start_time": ms(d, h, m), "end_time": ms(d, h, m + 1), "trigger": "PERIODIC",
                 "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}}
                for d in (3, 4, 5) if ms(d, h, m) >= since.timestamp() * 1000]

    def pipeline_id(self, name):
        return {"[dev ci_dev] netsuite_ingestion_poc": "p-ingest", "[dev ci_dev] guard_canary": "p-canary"}.get(name)

    def pipeline_events(self, pid, limit):
        if pid != "p-ingest":
            return []
        return [{"timestamp": "2026-10-06T05:31:00.000Z", "level": "WARN", "event_type": "flow_progress",
                 "message": "retry, see https://dbc-1.cloud.databricks.com/x", "origin": {"flow_name": "bronze_x"}},
                {"timestamp": "2026-09-01T00:00:00.000Z", "level": "ERROR", "message": "old"}]

    def pg_query(self, endpoint, statement, params=()):
        if self.disabled:
            raise Unavailable("Lakebase endpoint netsuite-sample/production is disabled (this server never enables it)")
        if "v_table_health" in statement:
            return [{"source_table": "netsuite_transactions", "layer": "ledger", "run_id": "r1", "status": "OK",
                     "error": None, "rows_read": 100, "rows_written": 0, "rows_rejected": 0, "reject_rate": None,
                     "discard_threshold": None, "threshold_breached": None, "active_rules": 2, "open_proposals": 0,
                     "started_at": NOW, "ended_at": NOW}]
        if "layer = 'guard'" in statement:
            return [{"run_id": "r1", "status": "WARN", "event_log_reads": 2, "error": None, "started_at": NOW}]
        if "status = 'FAILED'" in statement:
            return []
        if "count(DISTINCT" in statement:       # source per date: 10-02 has 3 same-day duplicate versions
            return [{"d": "2026-07-11", "n": 50, "k": 45}, {"d": "2026-10-02", "n": 40, "k": 37}]
        return [{"n": 10}]                       # full-load count

    def sql(self, statement):
        self.sql_calls.append(statement)
        if "system.access.audit" in statement:
            return [{"t": "2026-10-04 07:16:40", "service": "jobs", "action": "changeJobAcl", "actor": "x-1",
                     "rc": "200", "params": '{"resourceId":"999"}'}]
        if "_snapshot_date" in statement:        # bronze: 07-11 batch duplicates loaded, 10-02 short by 3
            return [{"d": "2026-07-11", "n": "50"}, {"d": "2026-10-02", "n": "37"}]
        return [{"n": "10"}]

    def workflow_runs(self, wf, since):
        return [{"created_at": "2026-10-01T20:56:18Z", "event": "push", "status": "completed",
                 "conclusion": "success", "head_sha": "f14ef2d0000", "display_title": "Merge pull request #26"}]


def tools(**kw):
    return Tools(FakeReads(**kw), now=lambda: NOW)


# -- registration ---------------------------------------------------------------

def test_five_read_only_tools_are_registered():
    listed = asyncio.run(build_server(tools()).list_tools())
    assert sorted(t.name for t in listed) == ["compare_bronze_to_source", "get_pipeline_errors",
                                              "get_recent_deploys", "get_recent_job_runs", "get_table_health"]
    for t in listed:
        assert t.annotations.read_only_hint is True and t.annotations.destructive_hint is False
        assert t.description and "env" in t.input_schema["properties"]


def test_tool_call_over_the_server_returns_json():
    res = asyncio.run(build_server(tools()).call_tool("get_recent_job_runs", {"env": "dev", "job": "generator"}))
    body = json.loads(res.content[0].text)
    assert body["status"] == "ok" and [j["job"] for j in body["jobs"]] == ["generator"]


# -- incident 2026-10-06: the scheduler did not fire ------------------------------

def test_missed_schedules_on_10_06():
    out = tools().get_recent_job_runs("dev", 3)
    by = {j["job"]: j for j in out["jobs"]}
    assert by["generator"]["missed_schedules"] == ["2026-10-06T05:00:00+00:00"]
    assert by["ingestion"]["missed_schedules"] == ["2026-10-06T05:30:00+00:00"]
    assert by["generator"]["schedule"]["pause_status"] == "UNPAUSED"
    assert by["canary"]["schedule"] is None and "missed_schedules" not in by["canary"]


def test_deploys_show_no_deploy_after_10_01_and_no_change_on_our_jobs():
    out = tools().get_recent_deploys("dev", 7)
    assert out["status"] == "ok"
    assert [d["created"] for d in out["deploys"]["items"]] == ["2026-10-01T20:56:18Z"]
    assert out["config_changes"]["items"][0]["resource"] is None       # ACL change on another job id
    assert out["config_changes"]["items"][0]["actor"] == "other"


def test_audit_query_is_scoped_to_this_envs_resources():
    t = tools()
    t.get_recent_deploys("dev", 7)
    [q] = [s for s in t.reads.sql_calls if "system.access.audit" in s]
    assert "RLIKE '1|2|3|p-ingest|p-canary'" in q and "2026-09-29 14:45:00" in q


# -- incident 2026-10-02: same-day duplicate versions ---------------------------------

def test_gap_classified_as_known_defect_and_batch_duplicates_ignored():
    out = tools().compare_bronze_to_source("dev", "netsuite_transactions")
    assert out["verdict"] == "known_defects_only"
    assert out["gaps"]["items"] == [{"table": "netsuite_transactions", "date": "2026-10-02", "source": 40,
                                     "bronze": 37, "missing": 3, "same_day_extra": 3,
                                     "classification": logic.KNOWN_DEFECT}]
    assert out["totals"] == [{"table": "netsuite_transactions", "source": 90, "bronze": 87}]


def test_full_load_table_compares_totals():
    out = tools().compare_bronze_to_source("dev", "netsuite_customers")
    assert out["verdict"] == "identical" and out["gaps"]["total"] == 0


# -- other tools and failure modes -------------------------------------------------------

def test_table_health_and_guard():
    out = tools().get_table_health("dev", "netsuite_transactions")
    assert out["tables"][0]["layer"] == "ledger" and out["latest_guard"]["event_log_reads"] == 2


def test_pipeline_errors_window_and_redaction():
    out = tools().get_pipeline_errors("dev", 24)
    ingest = next(p for p in out["pipelines"] if p["pipeline"] == "ingestion")
    assert ingest["events"]["total"] == 1                       # the September event is outside the window
    assert ingest["events"]["items"][0]["message"] == "retry, see <url>"
    assert out["failed_run_audit"]["total"] == 0


def test_disabled_endpoint_is_unavailable_not_an_error():
    out = tools(disabled=True).compare_bronze_to_source("dev")
    assert out["status"] == "unavailable" and "never enables it" in out["reason"]
    errs = tools(disabled=True).get_pipeline_errors("dev")      # event log still reported
    assert errs["status"] == "ok" and "unavailable" in errs["failed_run_audit"]


@pytest.mark.parametrize("call", [
    lambda t: t.get_table_health("prod"),
    lambda t: t.get_table_health("dev", "netsuite_x"),
    lambda t: t.get_recent_job_runs("dev", 99),
    lambda t: t.get_recent_job_runs("dev", 3, "prod_job"),
    lambda t: t.compare_bronze_to_source("dev", None, "10/02/2026"),
    lambda t: t.get_recent_deploys("dev", 0),
])
def test_bad_arguments_are_errors(call):
    out = call(tools())
    assert out["status"] == "error" and out["error"]
