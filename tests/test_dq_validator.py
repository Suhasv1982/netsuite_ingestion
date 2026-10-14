"""aidq_dq_agent.validator and tools/validate_proposals.py (no database, no warehouse)."""

import json

import pytest

pytest.importorskip("sqlglot")

from aidq_dq_agent import validator as v  # noqa: E402

COLS = ["amount", "quantity", "rate", "date", "membership_status", "start_date", "end_date", "type"]


@pytest.mark.parametrize("expr", [
    "amount IS NULL OR amount >= 0",
    "end_date IS NULL OR start_date IS NULL OR end_date >= start_date",
    "membership_status IN ('Active', 'Suspended', 'Expired')",
    "`date` IS NULL OR `date` <= current_date()",
    "coalesce(amount, 0) = quantity * rate",
    "NOT (amount < 0)",
    "lower(type) IN ('invoice') AND amount BETWEEN 0 AND 10",
])
def test_safe_expressions_pass(expr):
    r = v.static_check(expr, COLS)
    assert r.ok, r.problems


@pytest.mark.parametrize("expr,why", [
    ("amount > 0; DROP TABLE t", "';'"),
    ("amount > 0 -- sneaky", "comments"),
    ("amount > 0 /* x */", "comments"),
    ("amount > (SELECT max(amount) FROM t)", "Subquery"),
    ("rand() > 0.5", "rand()"),
    ("uuid() IS NOT NULL", "uuid()"),
    ("reflect('java.lang.Runtime', 'exec', 'x') IS NULL", "reflect()"),
    ("amount + 1", "boolean"),
    ("nope > 1", "not an active column"),
    ("t.amount > 0", "qualified column"),
    ("date <= current_date()", "must be quoted"),
    ("", "empty"),
    ("amount >", "does not parse"),
])
def test_unsafe_or_wrong_expressions_fail(expr, why):
    r = v.static_check(expr, COLS)
    assert not r.ok and any(why in p for p in r.problems), r.problems


def test_judge_null_results_fail():
    ok = v.static_check("amount >= 0", COLS)
    nulls = v.judge(ok, {"rows": 100, "passed": 90, "failed": 5, "nulls": 5}, 0.05, "15.4")
    assert nulls["status"] == "VALIDATION_FAILED" and nulls["checks"]["null_check"] is False
    assert any("NULL on 5 bronze rows" in p for p in nulls["problems"])


def test_judge_fails_a_reject_rate_above_the_discard_threshold():
    ok = v.static_check("amount >= 0", COLS)
    high = v.judge(ok, {"rows": 100, "passed": 80, "failed": 20, "nulls": 0}, 0.05, "15.4")
    assert high["status"] == "VALIDATION_FAILED" and high["checks"]["reject_rate"] is False
    assert high["would_reject_rate"] == 0.2 and high["max_reject_rate"] == 0.05
    assert any("would reject 20.00% of bronze rows (20 of 100), above 5%, the table's discard_threshold" in p
               for p in high["problems"])
    at_limit = v.judge(ok, {"rows": 100, "passed": 95, "failed": 5, "nulls": 0}, 0.05, "15.4")
    assert at_limit["status"] == "VALIDATED"                     # equal to the threshold is allowed
    low = v.judge(ok, {"rows": 93828, "passed": 93528, "failed": 300, "nulls": 0}, 0.05, "15.4")
    assert low["status"] == "VALIDATED" and low["summary"].startswith("VALIDATED")


def test_judge_uses_ten_percent_when_the_table_has_no_threshold():
    ok = v.static_check("amount >= 0", COLS)
    assert v.judge(ok, {"rows": 100, "passed": 91, "failed": 9, "nulls": 0}, None, None)["status"] == "VALIDATED"
    over = v.judge(ok, {"rows": 100, "passed": 89, "failed": 11, "nulls": 0}, None, None)
    assert over["status"] == "VALIDATION_FAILED" and over["max_reject_rate"] == v.DEFAULT_MAX_REJECT_RATE == 0.10
    assert any("the default limit (the table has no discard_threshold)" in p for p in over["problems"])


def test_judge_without_a_dry_run_never_validates():
    bad = v.static_check("rand() > 0", COLS)
    assert v.judge(bad, None, 0.05, None)["status"] == "VALIDATION_FAILED"


class Conn:
    def __init__(self, proposals):
        self.proposals, self.sql = proposals, []

    def execute(self, sql, params=()):
        self.sql.append((sql, params))
        if "FROM aidq_metadata.config_proposals p" in sql:
            rows = self.proposals
        elif "FROM aidq_metadata.source_columns" in sql:
            rows = [(c,) for c in COLS]
        else:
            rows = []
        return type("R", (), {"fetchall": lambda s: rows})()


def test_runner_sets_the_role_and_writes_status_with_the_result():
    import validate_proposals as vp

    conn = Conn([(1, 5, "netsuite_transaction_lines", 0.05, {"rule_expr": "amount IS NULL OR amount >= 0"}),
                 (2, 5, "netsuite_transaction_lines", 0.05, {"rule_expr": "rand() > 0"})])
    calls = []

    def sql(statement):
        calls.append(statement)
        if statement.startswith("SELECT version()"):
            return [{"v": "15.4"}]
        return [{"rows": 100, "passed": 97, "failed": 3, "nulls": 0}]

    out = vp.validate_all(conn, sql, "workspace", log=lambda m: None)
    assert conn.sql[0][0] == "SET LOCAL ROLE aidq_validator"
    assert [o["status"] for o in out] == ["VALIDATED", "VALIDATION_FAILED"]
    updates = [p for s, p in conn.sql if s.startswith("UPDATE aidq_metadata.config_proposals")]
    assert [u[0] for u in updates] == ["VALIDATED", "VALIDATION_FAILED"] and json.loads(updates[0][1])["rows"] == 100
    assert sum(1 for c in calls if "count_if" in c) == 1          # the unsafe rule never reaches the warehouse


def test_a_backslash_is_refused_because_spark_unescapes_it():
    # '\s' reaches Spark as 's': this rule rejected every email containing an 's' (DQ eval 2026-10-14)
    r = v.static_check(r"email IS NULL OR email rlike '^[^@\s]+@[^@\s]+\.[A-Za-z]+$'", ["email"])
    assert not r.ok and any("backslash is not allowed" in p for p in r.problems)
    assert v.static_check("email IS NULL OR email rlike '^[^@ ]+@[^@ ]+[.][A-Za-z]+$'", ["email"]).ok


def test_the_backslash_email_rule_of_2026_10_14_fails_twice():
    """The 2026-10-14 eval case: Spark reads '^[^@\\s]+@' as '^[^@s]+@', so the rule rejected every email containing
    an 's' (~1,560 of ~3,300 customers). It must fail on the backslash and, if one ever got past that, on the rate."""
    expr = r"email IS NULL OR email rlike '^[^@\s]+@[^@\s]+\.[A-Za-z]+$'"
    static = v.static_check(expr, ["email"])
    assert not static.ok and any("backslash is not allowed" in p for p in static.problems)
    # the dry-run counts that rule produced on dev bronze (customers, 2026-10-14)
    as_if_parsed = v.static_check("email IS NULL OR email rlike '^[^@s]+@[^@s]+.[A-Za-z]+$'", ["email"])
    assert as_if_parsed.ok
    result = v.judge(as_if_parsed, {"rows": 3300, "passed": 1740, "failed": 1560, "nulls": 0}, 0.05, None)
    assert result["status"] == "VALIDATION_FAILED" and result["checks"]["reject_rate"] is False
