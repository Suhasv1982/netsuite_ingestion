"""aidq_dq_agent.write_tools: argument checks, no-nagging, and the two inserts (fake connection, no database)."""

import json

from aidq_dq_agent import write_tools as wt

TABLES = [(2, "netsuite_memberships"), (5, "netsuite_transaction_lines")]
GOOD = {"table": "netsuite_transaction_lines", "rule_name": "Amount Not Negative",
        "rule_expr": "amount IS NULL OR amount >= 0", "severity": "SOFT",
        "rationale": "300 of 93,828 bronze rows (0.32%) have a negative amount; min -4500. SOFT: refunds may exist.",
        "evidence": {"violations": 300, "rows": 93828}, "confidence": 0.8}


class Conn:
    def __init__(self, existing=(), incident_conflict=False):
        self.existing, self.conflict = list(existing), incident_conflict
        self.sql, self.committed, self.rolled_back = [], False, False

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        self.sql.append((sql, params))
        if "FROM aidq_metadata.source_table_def" in sql:
            rows = TABLES
        elif "FROM aidq_metadata.config_proposals" in sql:
            rows = self.existing
        elif "INSERT INTO aidq_metadata.config_proposals" in sql:
            rows = [(101,)]
        elif "INSERT INTO aidq_metadata.incidents" in sql:
            rows = [] if self.conflict else [(7,)]
        else:
            rows = []
        return type("R", (), {"fetchall": lambda s: rows, "fetchone": lambda s: rows[0] if rows else None})()

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def tools(conn, dry_run=False):
    return wt.WriteTools(lambda: conn, "dq_agent@0.1", "trace-9", dry_run=dry_run)


def test_a_good_proposal_is_inserted_pending_with_its_provenance():
    conn = Conn()
    out = tools(conn).submit_proposal(GOOD)
    assert out == {"status": "ok", "proposal_id": 101} and conn.committed
    sql, params = next((s, p) for s, p in conn.sql if s.startswith("INSERT INTO aidq_metadata.config_proposals"))
    assert "'ADD_DQ_RULE'" in sql and "status" not in sql.split("VALUES")[0]      # PENDING by default + trigger
    assert params[0] == 5 and json.loads(params[1]) == {"rule_name": "Amount Not Negative",
                                                        "rule_expr": "amount IS NULL OR amount >= 0", "severity": "SOFT"}
    assert params[4:7] == ("dq_agent@0.1", "trace-9", 0.8)


def test_bad_arguments_are_refused_before_any_write():
    for bad in ({**GOOD, "table": "netsuite_unknown"}, {**GOOD, "severity": "CRITICAL"}, {**GOOD, "rule_expr": " "},
                {**GOOD, "rule_expr": "x" * 1001}, {**GOOD, "rationale": "short"}, {**GOOD, "evidence": {"rows": 1}},
                {**GOOD, "confidence": 1.5}):
        conn = Conn()
        out = tools(conn).submit_proposal(bad)
        assert out["status"] == "rejected_by_code" and out["problems"] and not conn.committed
        assert not any(s.startswith("INSERT") for s, _ in conn.sql)


def test_no_nagging_open_or_recently_rejected():
    same = "AMOUNT is null   or amount >= 0"            # same rule up to case and whitespace
    conn = Conn(existing=[(55, "PENDING", same, 300)])
    out = tools(conn).submit_proposal(GOOD)
    assert out["status"] == "skipped" and "already PENDING" in out["reason"]
    conn = Conn(existing=[(56, "REJECTED", same, 280)])           # 300 vs 280: +7%
    assert tools(conn).submit_proposal(GOOD)["status"] == "skipped"
    conn = Conn(existing=[(57, "REJECTED", same, 100)])           # 300 vs 100: +200% -> may propose again
    assert tools(conn).submit_proposal(GOOD)["status"] == "ok"
    conn = Conn(existing=[(58, "REJECTED", "amount > 0", 300)])   # a different rule
    assert tools(conn).submit_proposal(GOOD)["status"] == "ok"


def test_nag_decision_edge_cases():
    assert wt.nag_decision("a > 0", 5, [{"status": "REJECTED", "rule_expr": "a > 0", "violations": 0}]) is None
    assert wt.nag_decision("a > 0", 0, [{"status": "REJECTED", "rule_expr": "a > 0", "violations": 0}]) is not None
    assert wt.nag_decision("a > 0", 5, [{"status": "REJECTED", "rule_expr": "a > 0", "violations": None}]) is not None


def test_dry_run_decides_but_writes_nothing():
    conn = Conn()
    t = tools(conn, dry_run=True)
    assert t.submit_proposal(GOOD)["dry_run"] is True and not conn.committed
    assert t.written[0]["rule_expr"] == "amount IS NULL OR amount >= 0"


def test_finding_is_one_open_incident_per_table_and_type():
    conn = Conn()
    f = {"table": "netsuite_memberships", "finding_type": "duplicate_business_key",
         "summary": "24 keys have two rows on one day with different created_date", "evidence": {"keys": 24}}
    assert tools(conn).report_finding(f) == {"status": "ok", "incident_id": 7}
    sql, params = next((s, p) for s, p in conn.sql if s.startswith("INSERT INTO aidq_metadata.incidents"))
    assert "'DQ_NOT_EXPRESSIBLE'" in sql and "ON CONFLICT (fingerprint)" in sql
    assert params[3] == "dq:netsuite_memberships:duplicate_business_key"
    again = tools(Conn(incident_conflict=True)).report_finding(f)
    assert again["status"] == "skipped" and "already exists" in again["reason"]
    assert tools(Conn()).report_finding({**f, "finding_type": "made_up"})["status"] == "rejected_by_code"


def test_tool_definitions_match_the_checks():
    names = [t["name"] for t in wt.TOOL_DEFS]
    assert names == ["submit_proposal", "report_finding"]
    sp = wt.TOOL_DEFS[0]["input_schema"]
    assert set(sp["required"]) == {"table", "rule_name", "rule_expr", "severity", "rationale", "evidence", "confidence"}
    assert sp["properties"]["severity"]["enum"] == ["SOFT", "HARD"]
