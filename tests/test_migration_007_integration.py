"""Integration test: migration 007 (llm_calls, prune_llm_calls) on the dev metadata branch, in ONE transaction that
is always rolled back. Skipped unless NETSUITE_INTEGRATION=1.

    NETSUITE_INTEGRATION=1 pytest tests/test_migration_007_integration.py
"""

import os
from pathlib import Path

import pytest

if os.environ.get("NETSUITE_INTEGRATION") != "1":
    pytest.skip("set NETSUITE_INTEGRATION=1 to run migration 007 against dev (rolled back)", allow_module_level=True)

import migrate  # noqa: E402

from aidq_llm.router import PgCallLogWriter  # noqa: E402

SQL = (Path(__file__).resolve().parent.parent / "migrations" / "007_llm_calls.sql").read_text(encoding="utf-8")


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


class NoCommit:
    """The writer commits; inside this test the commit must not happen, so wrap the shared connection."""

    def __init__(self, c):
        self.c = c

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return self.c.cursor()

    def execute(self, *a):
        return self.c.execute(*a)

    def commit(self):
        pass


def row(**kw):
    base = {"called_at": "2026-10-13T20:00:00+00:00", "agent": "aidq_monitor", "task": "root_cause",
            "model": "m", "escalated": False, "escalation_reason": None, "input_tokens": 10, "output_tokens": 5,
            "latency_ms": 100, "est_cost_usd": 0.000013, "trace_id": "t", "outcome": "ok", "error": None}
    return {**base, **kw}


def test_writer_inserts_and_prunes_only_rows_older_than_the_retention(conn):
    old = row(called_at="2026-06-01T00:00:00+00:00", trace_id="old")
    written, pruned = PgCallLogWriter(lambda: NoCommit(conn)).write([row(), old], 90)
    assert written == 2 and pruned >= 1
    left = {r[0] for r in conn.execute("SELECT trace_id FROM aidq_metadata.llm_calls WHERE trace_id IN ('t', 'old')")}
    assert left == {"t"}


def test_outcome_is_checked_and_keep_days_must_be_positive(conn):
    conn.execute("SAVEPOINT s")
    with pytest.raises(Exception, match="llm_calls_outcome_check"):
        conn.execute("INSERT INTO aidq_metadata.llm_calls (agent, task, model, outcome) VALUES ('a', 't', 'm', 'meh')")
    conn.execute("ROLLBACK TO SAVEPOINT s")
    with pytest.raises(Exception, match="keep_days must be at least 1"):
        conn.execute("SELECT aidq_metadata.prune_llm_calls(0)")
    conn.execute("ROLLBACK TO SAVEPOINT s")


def test_prune_is_not_executable_by_public(conn):
    assert conn.execute("SELECT has_function_privilege('public', 'aidq_metadata.prune_llm_calls(integer)', 'EXECUTE')"
                        ).fetchone()[0] is False
