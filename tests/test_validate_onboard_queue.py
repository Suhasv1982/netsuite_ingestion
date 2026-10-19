"""tools/validate_proposals.py, onboarding plan #77 step 1: PENDING ONBOARD_TABLE proposals (table_id NULL) are
listed and left PENDING; nothing is written for them."""

import sqlglot

import validate_proposals as vp


class Conn:
    def __init__(self, rows):
        self.rows, self.statements = rows, []

    def execute(self, sql, params=()):
        self.statements.append(sql)
        return self

    def fetchall(self):
        return self.rows


def test_onboard_proposals_are_listed_not_written():
    conn, logged = Conn([(60, "netsuite_items")]), []
    out = vp.pending_onboard(conn, log=logged.append)
    assert out == [{"proposal_id": 60, "source_table": "netsuite_items", "status": "PENDING"}]
    assert "left PENDING" in logged[0] and "step 6" in logged[0]
    assert all(s.lstrip().upper().startswith("SELECT") for s in conn.statements)


def test_onboard_queue_sql_keeps_null_table_ids():
    tree = sqlglot.parse_one(vp.ONBOARD_PENDING_SQL, read="postgres")
    [join] = list(tree.find_all(sqlglot.exp.Join))
    assert join.side == "LEFT" and "proposal_type = 'ONBOARD_TABLE'" in vp.ONBOARD_PENDING_SQL
    # the existing queues still take only their own types
    assert "'ADD_DQ_RULE'" in vp.PENDING_SQL and "ONBOARD_TABLE" not in vp.COLUMN_PENDING_SQL
