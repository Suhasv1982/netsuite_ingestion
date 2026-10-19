"""tools/review.py: rendering, the rebuild flag, and the approve/reject guards (no database)."""

import os

import pytest

import review

ROW = {"proposal_id": 12, "source_table": "netsuite_transaction_lines", "load_mode": "Incremental",
       "proposal_type": "ADD_DQ_RULE", "status": "VALIDATED",
       "proposed_change": {"rule_name": "Amount Not Negative", "rule_expr": "amount IS NULL OR amount >= 0", "severity": "SOFT"},
       "rationale": "300 of 93,828 rows have a negative amount.", "evidence": {"violations": 300, "rows": 93828},
       "confidence": 0.8, "proposed_by": "dq_agent@0.1", "agent_trace_id": "t-1",
       "validation_result": {"summary": "VALIDATED | rows 93828, fail 300, null 0", "problems": [],
                             "flags": ["would-reject rate 0.0032 exceeds discard_threshold 0.001"]},
       "reviewed_by": None, "reviewed_at": None, "review_comment": None, "approved_severity": None, "created_at": "2026-10-14"}


def test_render_shows_what_a_reviewer_needs():
    text = review.render(ROW)
    for part in ("Proposal 12  ADD_DQ_RULE  on netsuite_transaction_lines  [VALIDATED]", "rule_expr: amount IS NULL",
                 "Rationale:", "Evidence:", "Validation: VALIDATED", "flag: would-reject rate", "trace: t-1"):
        assert part in text
    assert "requires bronze rebuild" not in text


def test_column_changes_on_incremental_tables_require_a_rebuild():
    assert review.requires_rebuild("ADD_COLUMN", "Incremental")
    assert not review.requires_rebuild("ADD_COLUMN", "FullLoad")
    assert not review.requires_rebuild("ADD_DQ_RULE", "Incremental")
    assert "requires bronze rebuild" in review.render({**ROW, "proposal_type": "ADD_COLUMN"})


class Conn:
    def __init__(self, row):
        self.row, self.sql = row, []

    def execute(self, sql, params=()):
        self.sql.append((sql, params))
        row, cols = self.row, list(self.row or {})

        class R:
            description = [type("C", (), {"name": c}) for c in cols]
            rowcount = 1

            def fetchone(self):
                if "apply_proposal" in sql:
                    return ("Proposal 12 (ADD_DQ_RULE) applied by me",)
                return None if row is None else tuple(row.values())
        return R()


def test_approve_sets_the_override_then_applies_in_the_same_transaction():
    conn = Conn(ROW)
    out = review.approve(conn, 12, "owner@example.com", "HARD", "real defect")
    upd = next(p for s, p in conn.sql if s.startswith("UPDATE aidq_metadata.config_proposals SET status = 'APPROVED'"))
    assert upd == ("owner@example.com", "real defect", "HARD", 12)
    assert any("apply_proposal" in s for s, _ in conn.sql) and "applied" in out


def test_only_validated_proposals_can_be_approved():
    with pytest.raises(SystemExit, match="only VALIDATED"):
        review.approve(Conn({**ROW, "status": "PENDING"}), 12, "me", None, None)
    with pytest.raises(SystemExit, match="not found"):
        review.approve(Conn(None), 12, "me", None, None)


def test_reject_needs_a_real_reason():
    for placeholder in ("  ", "...", ".", "no", "     x    "):
        with pytest.raises(SystemExit, match="at least 10 characters"):
            review.reject(Conn(ROW), 12, "me", placeholder)
    conn = Conn(ROW)
    assert "rejected" in review.reject(conn, 12, "me", "  refunds are legitimate  ")
    assert conn.sql[-1][1] == ("me", "refunds are legitimate", 12)          # stripped


REJECTED = {**ROW, "status": "REJECTED", "reviewed_by": "owner@example.com", "review_comment": "..."}


def test_correct_reason_keeps_the_old_reason_and_only_the_reviewer_may_do_it():
    conn = Conn(REJECTED)
    out = review.correct_reason(conn, 12, "owner@example.com", "future start dates are planned memberships",
                                "2026-10-14T20:30:00+00:00")
    assert out == "Proposal 12: reason corrected by owner@example.com"
    sql, params = conn.sql[-1]
    assert sql == review.CORRECT_REASON_SQL
    assert params == ("future start dates are planned memberships\n[corrected 2026-10-14T20:30:00+00:00 by "
                      "owner@example.com; was: '...']", 12, "owner@example.com")
    with pytest.raises(SystemExit, match="only that reviewer can correct"):
        review.correct_reason(Conn(REJECTED), 12, "someone@example.com", "a long enough reason", "t")
    with pytest.raises(SystemExit, match="only a REJECTED proposal"):
        review.correct_reason(Conn(ROW), 12, "owner@example.com", "a long enough reason", "t")
    with pytest.raises(SystemExit, match="at least 10 characters"):
        review.correct_reason(Conn(REJECTED), 12, "owner@example.com", "...", "t")


def test_a_second_correction_keeps_the_original_reason():
    once = {**REJECTED, "review_comment": "first real reason\n[corrected t1 by owner@example.com; was: '...']"}
    conn = Conn(once)
    review.correct_reason(conn, 12, "owner@example.com", "second real reason", "t2")
    assert conn.sql[-1][1][0] == "second real reason\n[corrected t2 by owner@example.com; was: 'first real reason']"


@pytest.mark.skipif(os.environ.get("NETSUITE_INTEGRATION") != "1", reason="set NETSUITE_INTEGRATION=1 (dev, rolled back)")
def test_review_on_dev_rolled_back():
    import json

    import migrate

    conn = migrate.connect("dev", os.environ.get("NETSUITE_PROFILE", "DEFAULT"))
    try:
        ids = []
        for name in ("Test approve", "Test reject"):
            pid = conn.execute(
                "INSERT INTO aidq_metadata.config_proposals (proposal_type, table_id, proposed_change, rationale, "
                "proposed_by) SELECT 'ADD_DQ_RULE', table_id, %s::jsonb, 'integration test', 'dq_agent@test' FROM "
                "aidq_metadata.source_table_def WHERE source_table = 'netsuite_transaction_lines' RETURNING proposal_id",
                (json.dumps({"rule_name": name, "rule_expr": "amount IS NULL OR amount >= 0", "severity": "SOFT"}),)
            ).fetchone()[0]
            conn.execute("UPDATE aidq_metadata.config_proposals SET status = 'VALIDATED', validation_result = "
                         "'{\"summary\": \"VALIDATED (test)\"}' WHERE proposal_id = %s", (pid,))
            ids.append(pid)
        reviewer = conn.execute("SELECT session_user").fetchone()[0]
        conn.execute("SET LOCAL ROLE aidq_reviewer")
        out = review.approve(conn, ids[0], reviewer, "HARD", "integration test")
        assert "applied" in out
        sev, by = conn.execute("SELECT severity, created_by FROM aidq_metadata.data_quality_rules WHERE proposal_id = %s",
                               (ids[0],)).fetchone()
        assert (sev, by) == ("HARD", "dq_agent@test")
        assert "rejected" in review.reject(conn, ids[1], reviewer, "integration test: not wanted")
        assert conn.execute("SELECT status, review_comment FROM aidq_metadata.config_proposals WHERE proposal_id = %s",
                            (ids[1],)).fetchone() == ("REJECTED", "integration test: not wanted")
        assert "Reviewed by" in review.render(review.show(conn, ids[0]))
        # the lifecycle trigger and aidq_reviewer allow correcting the reason of a rejection; status stays
        assert "reason corrected" in review.correct_reason(conn, ids[1], reviewer, "integration test: corrected", "t")
        status, comment = conn.execute("SELECT status, review_comment FROM aidq_metadata.config_proposals "
                                       "WHERE proposal_id = %s", (ids[1],)).fetchone()
        assert status == "REJECTED" and comment.startswith("integration test: corrected\n[corrected t by ")
    finally:
        conn.rollback()
        conn.close()


def test_show_prints_the_trace_link_when_the_run_was_traced():
    text = review.render({**ROW, "agent_trace_id": "trace:/workspace.aidq_traces/abc",
                          "trace_url": "https://h/ml/experiments/12/traces/abc"})
    assert "trace link: https://h/ml/experiments/12/traces/abc" in text
    assert "trace link" not in review.render(ROW)
    assert review.trace_url(None, ROW) is None                        # untraced: no experiment id in the evidence


# -- ONBOARD_TABLE (onboarding plan #77, step 1): table_id NULL until applied ----------------------------------------

ONBOARD = {**ROW, "proposal_id": 60, "proposal_type": "ONBOARD_TABLE", "source_table": "netsuite_items",
           "load_mode": "Incremental", "requires_rebuild": None,
           "proposed_change": {"table": {"source_table": "netsuite_items", "load_mode": "Incremental",
                                         "business_key": "item_id", "watermark_col": "updated_date",
                                         "scd_type": "Merge", "business_area_id": 3, "dest_schema": "netsuite"},
                               "columns": [{"column_name": "item_name", "data_type": "TEXT", "ordinal": 2},
                                           {"column_name": "item_id", "data_type": "INTEGER", "ordinal": 1}]},
           "validation_result": None}


def test_onboard_show_prints_the_table_definition_and_ordered_columns():
    text = review.render(ONBOARD)
    assert "Proposal 60  ONBOARD_TABLE  on netsuite_items  [VALIDATED]" in text
    for part in ("Proposed table:", "  business_key: item_id", "  watermark_col: updated_date", "  dest_catalog: -",
                 "  columns (2):"):
        assert part in text
    assert text.index("item_id  INTEGER") < text.index("item_name  TEXT")    # by ordinal, not list order
    assert "Change:" not in text


def test_onboard_never_needs_a_rebuild():
    assert not review.needs_rebuild(ONBOARD)
    assert not review.needs_rebuild({**ONBOARD, "requires_rebuild": True})   # a new table has no hashed column set


def test_list_and_show_keep_proposals_without_a_table_id():
    import sqlglot

    for sql in (review.LIST_SQL, review.SHOW_SQL):
        tree = sqlglot.parse_one(sql.replace("%s", "1"), read="postgres")
        [join] = list(tree.find_all(sqlglot.exp.Join))
        assert join.side == "LEFT", sql
        assert "p.proposed_change->'table'->>'source_table'" in sql
    assert "'new table, '" in review.LIST_SQL and "load_mode" in review.SHOW_SQL


def test_other_proposal_types_render_as_before():
    assert "Change:" in review.render(ROW) and "Proposed table:" not in review.render(ROW)
