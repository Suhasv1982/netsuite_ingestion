"""Integration test: the DQ write toolbox against dev aidq_metadata, in ONE transaction that is always rolled back.

Skipped unless NETSUITE_INTEGRATION=1. Runs the real INSERTs (lifecycle trigger, constraints, the open-fingerprint
index) as role aidq_agent when the session may SET ROLE to it (proves INSERT works and UPDATE is refused);
otherwise as the caller.

    NETSUITE_INTEGRATION=1 pytest tests/test_dq_write_tools_integration.py -s
"""

import os

import pytest

if os.environ.get("NETSUITE_INTEGRATION") != "1":
    pytest.skip("set NETSUITE_INTEGRATION=1 to run against dev metadata (rolled back)", allow_module_level=True)

import migrate  # noqa: E402

from aidq_dq_agent.write_tools import WriteTools  # noqa: E402

GOOD = {"table": "netsuite_transaction_lines", "rule_name": "Test Amount Not Negative",
        "rule_expr": "amount IS NULL OR amount >= 0", "severity": "SOFT",
        "rationale": "integration test: 300 of 93,828 rows have a negative amount (0.32%).",
        "evidence": {"violations": 300, "rows": 93828}, "confidence": 0.8}


class NoCommit:
    def __init__(self, c):
        self.c = c

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a):
        return self.c.execute(*a)

    def commit(self):
        pass

    def rollback(self):
        pass


@pytest.fixture(scope="module")
def conn():
    c = migrate.connect("dev", os.environ.get("NETSUITE_PROFILE", "DEFAULT"))
    try:
        try:
            c.execute("SAVEPOINT r")
            c.execute("SET LOCAL ROLE aidq_agent")
            c.as_agent = True
        except Exception:  # noqa: BLE001 -- not a member: run as the caller
            c.execute("ROLLBACK TO SAVEPOINT r")
            c.as_agent = False
        print("as aidq_agent:", c.as_agent)
        yield c
    finally:
        c.rollback()
        c.close()


def test_proposal_insert_and_no_nagging_and_finding(conn):
    t = WriteTools(lambda: NoCommit(conn), "dq_agent@test", "trace-it")
    out = t.submit_proposal(GOOD)
    assert out["status"] == "ok" and out["proposal_id"]
    status = conn.execute("SELECT status FROM aidq_metadata.config_proposals WHERE proposal_id = %s",
                          (out["proposal_id"],)).fetchone()[0]
    assert status == "PENDING"
    again = t.submit_proposal({**GOOD, "rule_expr": "AMOUNT IS NULL OR amount >= 0"})
    assert again["status"] == "skipped" and "already PENDING" in again["reason"]
    f = {"table": "netsuite_memberships", "finding_type": "duplicate_business_key",
         "summary": "integration test finding", "evidence": {"keys": 24}}
    first, second = t.report_finding(f), t.report_finding(f)
    assert first["status"] == "ok" and second["status"] == "skipped"


def test_the_agent_role_cannot_update_proposals(conn):
    if not conn.as_agent:
        pytest.skip("session cannot SET ROLE aidq_agent; privilege checked by tools/dq_roles.py --check instead")
    conn.execute("SAVEPOINT u")
    with pytest.raises(Exception, match="permission denied"):
        conn.execute("UPDATE aidq_metadata.config_proposals SET status = 'VALIDATED' WHERE false")
    conn.execute("ROLLBACK TO SAVEPOINT u")
