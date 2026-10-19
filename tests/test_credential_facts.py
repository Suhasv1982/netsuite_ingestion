"""Token age as a deterministic monitor fact (incident 6, 2026-10-14): aidq_mcp.logic.credential_checks and
aidq_agent.facts.credential_facts (also importable from triage)."""

import datetime as dt

from aidq_agent import facts as facts_mod, triage
from aidq_mcp import logic

UTC = dt.timezone.utc
SINCE = dt.datetime(2026, 10, 13, 12, 0, tzinfo=UTC)


def ms(*a):
    return int(dt.datetime(*a, tzinfo=UTC).timestamp() * 1000)


UPDATES = [
    {"update_id": "5a0223d5-x", "state": "FAILED", "cause": "API_CALL", "creation_time": ms(2026, 10, 14, 1, 11, 20)},
    {"update_id": "cfc52e00-x", "state": "COMPLETED", "cause": "API_CALL", "creation_time": ms(2026, 10, 14, 1, 13)},
    {"update_id": "aaaa0000-x", "state": "FAILED", "cause": "JOB_TASK", "creation_time": ms(2026, 10, 14, 5, 31)},
    {"update_id": "old00000-x", "state": "FAILED", "cause": "JOB_TASK", "creation_time": ms(2026, 10, 12, 5, 31)},
]
REFRESHES = [dt.datetime(2026, 10, 13, 20, 44, 10, tzinfo=UTC), dt.datetime(2026, 10, 14, 1, 12, tzinfo=UTC),
             dt.datetime(2026, 10, 14, 5, 30, 40, tzinfo=UTC)]


def test_failed_updates_get_their_starter_and_the_credential_age_then():
    checks = logic.credential_checks(UPDATES, REFRESHES, SINCE)
    assert [c["update_id"] for c in checks] == ["5a0223d5-x", "aaaa0000-x"]     # failed, in the window only
    manual, scheduled = checks
    assert manual == {"update_id": "5a0223d5-x", "created": "2026-10-14T01:11:20+00:00", "cause": "API_CALL",
                      "started_by_job": False, "last_credential_refresh": "2026-10-13T20:44:10+00:00",
                      "credential_age_minutes": 267, "credential_expired": True}
    assert scheduled["started_by_job"] and scheduled["credential_age_minutes"] == 0
    assert scheduled["credential_expired"] is False


def test_no_refresh_found_is_said_not_guessed():
    [c] = logic.credential_checks(UPDATES[:1], [], SINCE)
    assert c["last_credential_refresh"] is None and c["credential_age_minutes"] is None and c["credential_expired"] is None


def test_facts_name_the_starter_and_the_age():
    checks = logic.credential_checks(UPDATES, REFRESHES, SINCE)
    results = {"get_pipeline_errors": {"pipelines": [{"pipeline": "ingestion", "failed_updates": checks},
                                                     {"pipeline": "canary", "events": []}]}}
    manual, scheduled = triage.credential_facts(results)
    assert manual == ("fact (from get_pipeline_errors): pipeline update 5a0223d5 failed at 2026-10-14T01:11:20+00:00; "
                      "started by API_CALL, not through the job, so refresh_credentials did not run first; the stored "
                      "database credential was last refreshed 2026-10-13T20:44:10+00:00, 4 h 27 min before (EXPIRED: "
                      "credentials last about 60 min)")
    assert "through the job (refresh_credentials ran first)" in scheduled and "still valid" in scheduled
    assert manual in facts_mod.facts_from(results)


def test_unavailable_checks_add_no_fact():
    results = {"get_pipeline_errors": {"pipelines": [{"pipeline": "ingestion", "failed_updates": {"unavailable": "x"}}]}}
    assert triage.credential_facts(results) == []
