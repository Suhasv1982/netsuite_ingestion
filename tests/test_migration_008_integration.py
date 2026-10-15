"""Integration test: migration 008 (shared foundation) on the dev metadata branch, in ONE transaction that is always
rolled back. Skipped unless NETSUITE_INTEGRATION=1.

    NETSUITE_INTEGRATION=1 pytest tests/test_migration_008_integration.py
"""

import json
import os
from pathlib import Path

import psycopg
import pytest

if os.environ.get("NETSUITE_INTEGRATION") != "1":
    pytest.skip("set NETSUITE_INTEGRATION=1 to run migration 008 against dev (rolled back)", allow_module_level=True)

import migrate  # noqa: E402

SQL = (Path(__file__).resolve().parent.parent / "migrations" / "008_agent_foundation.sql").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def conn():
    c = migrate.connect("dev", os.environ.get("NETSUITE_PROFILE", "DEFAULT"))
    try:
        c.execute(migrate.SET_OWNER_ROLE)
        c.execute(SQL)
        yield c
    finally:
        c.rollback()   # never commit
        c.close()


import contextlib  # noqa: E402


@contextlib.contextmanager
def isolated(conn, name):
    """Each test's changes are rolled back to a savepoint, also when it fails (the transaction is rolled back at the
    end anyway)."""
    conn.execute(f"SAVEPOINT {name}")
    try:
        yield
    finally:
        conn.execute(f"ROLLBACK TO SAVEPOINT {name}")


def expect_error(conn, exc, match, fn):
    """fn must raise exc; the failed statement is contained in its own savepoint."""
    conn.execute("SAVEPOINT expect")
    try:
        with pytest.raises(exc, match=match):
            fn()
    finally:
        conn.execute("ROLLBACK TO SAVEPOINT expect")


def propose(conn, ptype, table_id, change):
    return conn.execute(
        "INSERT INTO aidq_metadata.config_proposals (proposal_type, table_id, proposed_change, rationale, proposed_by) "
        "VALUES (%s, %s, %s::jsonb, 'integration test of migration 008', 'test_agent@0') RETURNING proposal_id",
        (ptype, table_id, json.dumps(change))).fetchone()[0]


def approve_and_apply(conn, pid):
    conn.execute("UPDATE aidq_metadata.config_proposals SET status = 'VALIDATED', validation_result = '{}' "
                 "WHERE proposal_id = %s", (pid,))
    conn.execute("UPDATE aidq_metadata.config_proposals SET status = 'APPROVED', reviewed_by = 'reviewer@test', "
                 "reviewed_at = now() WHERE proposal_id = %s", (pid,))
    return conn.execute("SELECT aidq_metadata.apply_proposal(%s, 'reviewer@test')", (pid,)).fetchone()[0]


def table_id(conn, name):
    return conn.execute("SELECT table_id FROM aidq_metadata.source_table_def WHERE source_table = %s",
                        (name,)).fetchone()[0]


def test_deactivate_column_keeps_the_row_and_refuses_keys(conn):
    with isolated(conn, "deact"):
        tid = table_id(conn, "netsuite_transactions")
        col = conn.execute(
            "SELECT column_name FROM aidq_metadata.source_columns s JOIN aidq_metadata.source_table_def d USING "
            "(table_id) WHERE s.table_id = %s AND COALESCE(s.is_active, true) AND s.column_name NOT IN "
            "(d.business_key, COALESCE(d.watermark_col, '')) ORDER BY ordinal DESC LIMIT 1", (tid,)).fetchone()[0]
        assert "applied" in approve_and_apply(conn, propose(conn, "DEACTIVATE_COLUMN", tid, {"column_name": col}))
        assert conn.execute("SELECT is_active FROM aidq_metadata.source_columns WHERE table_id = %s AND "
                            "column_name = %s", (tid, col)).fetchone()[0] is False
        key = propose(conn, "DEACTIVATE_COLUMN", tid, {"column_name": "transaction_internal_id"})
        expect_error(conn, psycopg.errors.RaiseException, "business key or watermark",
                     lambda: approve_and_apply(conn, key))


def test_onboard_table_inserts_the_definition_and_columns_in_one_go(conn):
    with isolated(conn, "onboard"):
        change = {"table": {"source_table": "netsuite_items_test", "dest_catalog": "netsuite",
                            "dest_schema": "netsuite", "business_key": "item_id", "watermark_col": "updated_date",
                            "load_mode": "Incremental", "scd_type": "Merge", "business_area_id": 1,
                            "discard_threshold": 0.05},
                  "columns": [{"column_name": "item_id", "data_type": "bigint", "ordinal": 1},
                              {"column_name": "item_name", "data_type": "text", "ordinal": 2},
                              {"column_name": "updated_date", "data_type": "date", "ordinal": 3}]}
        pid = propose(conn, "ONBOARD_TABLE", None, change)      # no table_id yet: allowed only for ONBOARD_TABLE
        assert "applied" in approve_and_apply(conn, pid)
        tid = table_id(conn, "netsuite_items_test")
        others = [r[0] for r in conn.execute("SELECT table_id FROM aidq_metadata.source_table_def WHERE "
                                             "source_table <> 'netsuite_items_test'").fetchall()]
        assert tid > max(others)
        cols = [r[0] for r in conn.execute("SELECT column_name FROM aidq_metadata.source_columns WHERE table_id = %s "
                                           "ORDER BY ordinal", (tid,)).fetchall()]
        assert cols == ["item_id", "item_name", "updated_date"]
        assert conn.execute("SELECT count(*) FROM aidq_metadata.config_change_log WHERE table_name = "
                            "'source_table_def' AND operation = 'INSERT' AND proposal_id = %s",
                            (pid,)).fetchone()[0] == 1
        again = propose(conn, "ONBOARD_TABLE", None, change)
        expect_error(conn, psycopg.errors.RaiseException, "already defined", lambda: approve_and_apply(conn, again))


def test_table_id_may_be_null_only_for_onboard_table(conn):
    with isolated(conn, "nullid"):
        expect_error(conn, psycopg.errors.CheckViolation, "table_id_chk",
                     lambda: propose(conn, "ADD_COLUMN", None, {"column_name": "x"}))


def test_requires_rebuild_only_with_validation(conn):
    with isolated(conn, "rebuild"):
        tid = table_id(conn, "netsuite_transactions")
        pid = propose(conn, "ADD_COLUMN", tid, {"column_name": "custbody_region", "data_type": "text"})
        conn.execute("UPDATE aidq_metadata.config_proposals SET status = 'VALIDATED', validation_result = '{}', "
                     "requires_rebuild = true WHERE proposal_id = %s", (pid,))
        assert conn.execute("SELECT requires_rebuild FROM aidq_metadata.config_proposals WHERE proposal_id = %s",
                            (pid,)).fetchone()[0] is True
        expect_error(conn, psycopg.errors.RaiseException, "requires_rebuild", lambda: conn.execute(
            "UPDATE aidq_metadata.config_proposals SET requires_rebuild = false WHERE proposal_id = %s", (pid,)))
        expect_error(conn, psycopg.errors.RaiseException, "must be PENDING", lambda: conn.execute(
            "INSERT INTO aidq_metadata.config_proposals (proposal_type, table_id, proposed_change, rationale, "
            "proposed_by, requires_rebuild) VALUES ('ADD_COLUMN', %s, '{}', 'x', 'a', true)", (tid,)))


def test_schema_type_change_is_an_incident_category(conn):
    with isolated(conn, "inc"):
        conn.execute("INSERT INTO aidq_metadata.incidents (category, summary) VALUES ('SCHEMA_TYPE_CHANGE', 'test')")
