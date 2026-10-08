"""Integration test: migration 006 on the dev metadata branch, inside ONE transaction that is always rolled back.

Skipped unless NETSUITE_INTEGRATION=1 (needs the owner's Databricks CLI profile and the dev metadata endpoint).
Applies migrations/006 as aidq_owner like tools/migrate.py, then exercises the proposal lifecycle; nothing is
committed, so the dev database is unchanged afterwards.

    NETSUITE_INTEGRATION=1 pytest tests/test_migration_006_integration.py
"""

import json
import os
from pathlib import Path

import pytest

if os.environ.get("NETSUITE_INTEGRATION") != "1":
    pytest.skip("set NETSUITE_INTEGRATION=1 to run migration 006 against dev (rolled back)", allow_module_level=True)

import migrate  # noqa: E402

SQL = (Path(__file__).resolve().parent.parent / "migrations" / "006_dq_recommender_governance.sql").read_text(encoding="utf-8")
RULE = {"rule_name": "Test Amount Not Negative", "rule_expr": "amount IS NULL OR amount >= 0", "severity": "SOFT"}


@pytest.fixture(scope="module")
def conn():
    c = migrate.connect("dev", os.environ.get("NETSUITE_PROFILE", "DEFAULT"))
    try:
        c.execute(migrate.SET_OWNER_ROLE)
        c.execute(SQL)
        yield c
    finally:
        c.rollback()  # never commit: the whole module runs in this one transaction
        c.close()


def _raises(conn, sql, params=(), match=""):
    conn.execute("SAVEPOINT s")
    with pytest.raises(Exception, match=match):
        conn.execute(sql, params)
    conn.execute("ROLLBACK TO SAVEPOINT s")


def _new(conn, change=None) -> int:
    return conn.execute(
        "INSERT INTO aidq_metadata.config_proposals (proposal_type, table_id, proposed_change, rationale, proposed_by) "
        "VALUES ('ADD_DQ_RULE', 5, %s::jsonb, 'test', 'dq_agent@test') RETURNING proposal_id",
        (json.dumps(change or RULE),)).fetchone()[0]


def _upd(conn, pid, **cols):
    sets = ", ".join(f"{k} = %s" for k in cols)
    conn.execute(f"UPDATE aidq_metadata.config_proposals SET {sets} WHERE proposal_id = %s", (*cols.values(), pid))


def test_new_proposal_cannot_carry_validation_or_severity(conn):
    _raises(conn, "INSERT INTO aidq_metadata.config_proposals (proposal_type, table_id, proposed_change, rationale, "
                  "proposed_by, validation_result) VALUES ('ADD_DQ_RULE', 5, '{}', 't', 'a', '{}')", match="unvalidated")
    _raises(conn, "INSERT INTO aidq_metadata.config_proposals (proposal_type, table_id, proposed_change, rationale, "
                  "proposed_by, approved_severity) VALUES ('ADD_DQ_RULE', 5, '{}', 't', 'a', 'HARD')", match="unvalidated")


def test_validation_result_only_with_the_validation_transition(conn):
    pid = _new(conn)
    _raises(conn, "UPDATE aidq_metadata.config_proposals SET validation_result = '{}' WHERE proposal_id = %s", (pid,),
            match="validation_result")
    _upd(conn, pid, status="VALIDATED", validation_result=json.dumps({"ok": True}))
    _raises(conn, "UPDATE aidq_metadata.config_proposals SET validation_result = '{\"ok\": false}' "
                  "WHERE proposal_id = %s", (pid,), match="validation_result")


def test_approved_severity_only_on_approval_and_used_by_apply(conn):
    pid = _new(conn)
    _upd(conn, pid, status="VALIDATED", validation_result="{}")
    _raises(conn, "UPDATE aidq_metadata.config_proposals SET approved_severity = 'HARD' WHERE proposal_id = %s", (pid,),
            match="approved_severity")
    _raises(conn, "UPDATE aidq_metadata.config_proposals SET status = 'APPROVED', reviewed_by = 'r', reviewed_at = now(), "
                  "approved_severity = 'MEDIUM' WHERE proposal_id = %s", (pid,), match="approved_severity")
    conn.execute("UPDATE aidq_metadata.config_proposals SET status = 'APPROVED', reviewed_by = 'owner@test', "
                 "reviewed_at = now(), approved_severity = 'HARD' WHERE proposal_id = %s", (pid,))
    conn.execute("SELECT aidq_metadata.apply_proposal(%s, 'owner@test')", (pid,))
    sev, created_by = conn.execute("SELECT severity, created_by FROM aidq_metadata.data_quality_rules "
                                   "WHERE proposal_id = %s", (pid,)).fetchone()
    assert (sev, created_by) == ("HARD", "dq_agent@test")  # the reviewer's severity, the agent's authorship
    change = conn.execute("SELECT proposed_change FROM aidq_metadata.config_proposals WHERE proposal_id = %s",
                          (pid,)).fetchone()[0]
    assert change["severity"] == "SOFT"  # the agent's proposal is unchanged in the audit trail


def test_without_override_the_proposed_severity_applies(conn):
    pid = _new(conn, {**RULE, "rule_name": "Test Two"})
    _upd(conn, pid, status="VALIDATED", validation_result="{}")
    conn.execute("UPDATE aidq_metadata.config_proposals SET status = 'APPROVED', reviewed_by = 'owner@test', "
                 "reviewed_at = now() WHERE proposal_id = %s", (pid,))
    conn.execute("SELECT aidq_metadata.apply_proposal(%s, 'owner@test')", (pid,))
    assert conn.execute("SELECT severity FROM aidq_metadata.data_quality_rules WHERE proposal_id = %s",
                        (pid,)).fetchone()[0] == "SOFT"


def test_dq_not_expressible_incident_category(conn):
    conn.execute("INSERT INTO aidq_metadata.incidents (category, summary) VALUES ('DQ_NOT_EXPRESSIBLE', 'test')")
    _raises(conn, "INSERT INTO aidq_metadata.incidents (category, summary) VALUES ('NOT_A_CATEGORY', 'x')",
            match="incidents_category_check")
