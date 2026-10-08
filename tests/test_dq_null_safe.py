"""HARD rules are NULL-safe: every row is either valid or rejected, never neither (owner decision 2026-10-08).

Runs the predicate and reason expressions that dq.py uses (WHERE pred for valid rows, WHERE NOT (pred) for
rejects) on DuckDB, whose NULL logic matches Spark SQL for these operators. Before the change, a rule that
evaluated to NULL dropped the row from both sides.
"""

import pytest

from metadata import build_dq_predicate, build_dq_reason_expr

duckdb = pytest.importorskip("duckdb")

RULES = [
    {"severity": "HARD", "rule_name": "Not in List", "rule_expr": "status in ('Active','Expired')"},
    {"severity": "HARD", "rule_name": "Positive", "rule_expr": "amount > 0"},
]
ROWS = [(1, "Active", 5), (2, "Bogus", 5), (3, None, 5), (4, "Active", None), (5, None, None)]


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE b (id INT, status VARCHAR, amount INT)")
    c.executemany("INSERT INTO b VALUES (?, ?, ?)", ROWS)
    return c


def _ids(con, where):
    return [r[0] for r in con.execute(f"SELECT id FROM b WHERE {where} ORDER BY id").fetchall()]


def test_every_row_is_valid_or_rejected_exactly_once(con):
    pred = build_dq_predicate(RULES)
    valid, rejected = _ids(con, pred), _ids(con, f"NOT ({pred})")
    assert valid == [1]
    assert rejected == [2, 3, 4, 5]
    assert sorted(valid + rejected) == [r[0] for r in ROWS]


def test_the_old_predicate_lost_null_rows(con):
    old = " AND ".join(f"({r['rule_expr']})" for r in RULES)
    lost = {r[0] for r in ROWS} - set(_ids(con, old)) - set(_ids(con, f"NOT ({old})"))
    assert lost == {3, 4, 5}  # what the NULL-safe predicate now routes to rejects


def test_a_null_result_is_named_in_the_reason(con):
    # DuckDB has no array_compact; evaluate the CASE parts the reason expression is built from
    reason = build_dq_reason_expr(RULES)
    cases = reason[len("array_join(array_compact(array("):-len(")), ', ')")]
    got = con.execute(f"SELECT id, [{cases}] FROM b ORDER BY id").fetchall()
    named = {i: [x for x in parts if x is not None] for i, parts in got}
    assert named == {1: [], 2: ["Not in List"], 3: ["Not in List"], 4: ["Positive"], 5: ["Not in List", "Positive"]}
