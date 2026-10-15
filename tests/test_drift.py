"""The schema drift agent: detection (pure), the review checks, the submit mapping, the column validator."""

import asyncio
import pathlib
import sys

import pytest

from aidq_drift import agent, detect
from aidq_drift.detect import Change

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "tools"))
import validate_proposals as vp  # noqa: E402

META = [  # source_table_def x source_columns rows
    {"table_id": 1, "source_table": "netsuite_customers", "business_key": "customer_internal_id", "watermark_col": None,
     "load_mode": "FullLoad", "column_name": "customer_internal_id", "data_type": "INTEGER", "ordinal": 1, "is_active": True},
    {"table_id": 1, "source_table": "netsuite_customers", "business_key": "customer_internal_id", "watermark_col": None,
     "load_mode": "FullLoad", "column_name": "email", "data_type": "TEXT", "ordinal": 3, "is_active": True},
    {"table_id": 4, "source_table": "netsuite_transactions", "business_key": "transaction_internal_id",
     "watermark_col": "updated_date", "load_mode": "Incremental", "column_name": "transaction_internal_id",
     "data_type": "integer", "ordinal": 1, "is_active": True},
    {"table_id": 4, "source_table": "netsuite_transactions", "business_key": "transaction_internal_id",
     "watermark_col": "updated_date", "load_mode": "Incremental", "column_name": "updated_date",
     "data_type": "DATE", "ordinal": 7, "is_active": True},
    {"table_id": 4, "source_table": "netsuite_transactions", "business_key": "transaction_internal_id",
     "watermark_col": "updated_date", "load_mode": "Incremental", "column_name": "memo",
     "data_type": "TEXT", "ordinal": 6, "is_active": True},
]


def src(table, column, dtype):
    return {"table_name": table, "column_name": column, "data_type": dtype, "ordinal_position": 1}


def test_no_drift_when_source_matches_ignoring_type_case():
    source = [src("netsuite_customers", "customer_internal_id", "integer"), src("netsuite_customers", "email", "text"),
              src("netsuite_transactions", "transaction_internal_id", "integer"),
              src("netsuite_transactions", "updated_date", "date"), src("netsuite_transactions", "memo", "text")]
    assert detect.diff(source, ["netsuite_customers", "netsuite_transactions"], META) == []


def test_every_kind_of_drift():
    source = [src("netsuite_customers", "customer_internal_id", "integer"), src("netsuite_customers", "email", "text"),
              src("netsuite_customers", "created_date", "date"),                      # added, full load
              src("netsuite_transactions", "transaction_internal_id", "bigint"),      # type changed, key column
              src("netsuite_transactions", "updated_date", "date"),
              src("netsuite_transactions", "custbody_region", "text"),               # added, incremental
              src("netsuite_transactions", "geo", "point")]                          # added, unmappable
    changes = {c.id: c for c in detect.diff(source, ["netsuite_customers", "netsuite_transactions", "netsuite_items"],
                                             META)}
    assert set(changes) == {"netsuite_customers:added:created_date", "netsuite_transactions:added:custbody_region",
                            "netsuite_transactions:added:geo", "netsuite_transactions:removed:memo",
                            "netsuite_transactions:type_changed:transaction_internal_id", "netsuite_items:unonboarded"}
    created = changes["netsuite_customers:added:created_date"]
    assert (created.proposed_type, created.ordinal, created.incremental) == ("DATE", 4, False)
    region = changes["netsuite_transactions:added:custbody_region"]
    assert region.incremental and region.proposed_type == "TEXT" and region.ordinal == 8
    assert changes["netsuite_transactions:added:geo"].proposed_type is None
    assert changes["netsuite_transactions:type_changed:transaction_internal_id"].key_column


REVIEW = {"reviews": [{"change_id": "t:added:c", "rationale": "A new column with region codes, mostly filled.",
                       "relevance": "medium", "confidence": 0.8}]}


def test_every_change_needs_one_review():
    assert agent.review_problems(REVIEW, ["t:added:c"]) == ([], [])
    problems, unanswered = agent.review_problems(REVIEW, ["t:added:c", "t:removed:d"])
    assert unanswered == ["t:removed:d"]
    twice = {"reviews": REVIEW["reviews"] * 2}
    assert any("reviewed twice" in p for p in agent.review_problems(twice, ["t:added:c"])[0])
    assert agent.review_problems({"reviews": [{"change_id": "x"}]}, ["x"])[0]          # schema


def test_the_rebuild_sentence_only_for_incremental_tables():
    inc = Change("netsuite_transactions", "added", "custbody_region", incremental=True)
    full = Change("netsuite_customers", "added", "created_date", incremental=False)
    assert agent.REBUILD_SENTENCE in agent.rationale_for(inc, "A region code column.")
    assert agent.rationale_for(inc, "Approval requires a bronze rebuild.") == "Approval requires a bronze rebuild."
    assert agent.rationale_for(full, "A creation date.") == "A creation date."


class FakeWriteTools:
    dry_run = False

    def __init__(self):
        self.proposals, self.incidents = [], []

    def submit_column_proposal(self, args):
        self.proposals.append(args)
        return {"status": "ok", "proposal_id": len(self.proposals)}

    def report_incident(self, args):
        self.incidents.append(args)
        return {"status": "ok", "incident_id": len(self.incidents)}


def test_submit_maps_each_change_to_the_right_write():
    changes = [Change("netsuite_transactions", "added", "custbody_region", "text", None, "TEXT", 8, True),
               Change("netsuite_transactions", "added", "geo", "point", None, None, 9, True),
               Change("netsuite_transactions", "removed", "memo", None, "TEXT", None, 6, True),
               Change("netsuite_transactions", "removed", "updated_date", None, "DATE", None, 7, True, True),
               Change("netsuite_transactions", "type_changed", "amount", "numeric", "INTEGER", "DECIMAL", 5, True),
               Change("netsuite_items", "unonboarded")]
    reviews = {c.id: {"rationale": "Reviewed by the model in a full sentence.", "relevance": "low", "confidence": 0.5}
               for c in changes}
    wt = FakeWriteTools()
    out = agent.submit(changes, reviews, {}, wt, {"prompt_version": "v1"})
    assert [o["written"] for o in out] == ["ADD_COLUMN proposal", "SCHEMA_DRIFT incident", "DEACTIVATE_COLUMN proposal",
                                           "SCHEMA_DRIFT incident", "SCHEMA_TYPE_CHANGE incident", "SCHEMA_DRIFT incident"]
    add, deact = wt.proposals
    assert (add["proposal_type"], add["data_type"], add["ordinal"]) == ("ADD_COLUMN", "TEXT", 8)
    assert agent.REBUILD_SENTENCE in add["rationale"] and agent.REBUILD_SENTENCE in deact["rationale"]
    assert [i["fingerprint"] for i in wt.incidents] == [
        "drift:netsuite_transactions:added:geo", "drift:netsuite_transactions:removed:updated_date",
        "drift:netsuite_transactions:type_changed:amount", "drift:netsuite_items:unonboarded"]


def test_no_drift_means_no_model_call():
    class NoModel:
        async def review(self, changes, profiles):
            raise AssertionError("no model call expected")

    def pg(endpoint, sql, params=()):
        if "information_schema.tables" in sql:
            return [{"table_name": "netsuite_customers"}]
        if "information_schema.columns" in sql:
            return [src("netsuite_customers", "customer_internal_id", "integer"), src("netsuite_customers", "email", "text")]
        return META[:2]

    state = asyncio.run(agent.run(pg, "s", "m", NoModel(), FakeWriteTools(), "t"))
    assert state["exit_code"] == 0 and "No drift" in state["report"]


TABLE_INC = {"load_mode": "Incremental", "business_key": "transaction_internal_id", "watermark_col": "updated_date"}
TABLE_FULL = {"load_mode": "FullLoad", "business_key": "customer_internal_id", "watermark_col": None}
META_COLS = [("transaction_internal_id", 1, True), ("updated_date", 7, True), ("memo", 6, True), ("old", 2, False)]


def test_validator_add_column_and_requires_rebuild_only_for_incremental():
    ok = vp.judge_column("ADD_COLUMN", {"column_name": "custbody_region", "data_type": "TEXT", "ordinal": 8}, TABLE_INC,
                         {"custbody_region": "text"}, META_COLS)
    assert ok["status"] == "VALIDATED" and ok["requires_rebuild"] is True
    full = vp.judge_column("ADD_COLUMN", {"column_name": "created_date", "data_type": "DATE", "ordinal": 4}, TABLE_FULL,
                           {"created_date": "date"}, [("customer_internal_id", 1, True)])
    assert full["status"] == "VALIDATED" and full["requires_rebuild"] is False        # full load: no row hash
    for change, source, why in (
            ({"column_name": "gone", "data_type": "TEXT"}, {}, "not in the source"),
            ({"column_name": "memo", "data_type": "TEXT"}, {"memo": "text"}, "already active"),
            ({"column_name": "c", "data_type": "INTEGER"}, {"c": "text"}, "does not match"),
            ({"column_name": "c", "data_type": "TEXT", "ordinal": 6}, {"c": "text"}, "ordinal 6"),
            ({"column_name": "g", "data_type": "TEXT"}, {"g": "point"}, "no metadata type mapping")):
        r = vp.judge_column("ADD_COLUMN", change, TABLE_INC, source, META_COLS)
        assert r["status"] == "VALIDATION_FAILED" and r["requires_rebuild"] is None and any(why in p for p in r["problems"])


def test_validator_deactivate_column():
    ok = vp.judge_column("DEACTIVATE_COLUMN", {"column_name": "memo"}, TABLE_INC, {}, META_COLS)
    assert ok["status"] == "VALIDATED" and ok["requires_rebuild"] is True
    for col, source, why in (("updated_date", {}, "business key or the watermark"), ("memo", {"memo": "text"}, "still exists"),
                             ("old", {}, "not an active column")):
        r = vp.judge_column("DEACTIVATE_COLUMN", {"column_name": col}, TABLE_INC, source, META_COLS)
        assert r["status"] == "VALIDATION_FAILED" and any(why in p for p in r["problems"]), col


def test_review_uses_the_validators_requires_rebuild():
    import review

    base = {"proposal_type": "ADD_COLUMN", "load_mode": "FullLoad"}
    assert review.needs_rebuild({**base, "requires_rebuild": None}) is False
    assert review.needs_rebuild({**base, "requires_rebuild": True}) is True
    assert review.needs_rebuild({"proposal_type": "ADD_COLUMN", "load_mode": "Incremental", "requires_rebuild": None})


@pytest.mark.parametrize("t", ["date", "integer", "text", "timestamp without time zone", "bigint", "numeric"])
def test_type_map_covers_the_source_types(t):
    assert detect.metadata_type(t)


def test_impact_evidence_uses_the_column_for_removals_and_the_table_for_additions():
    asked = []

    def get_impact(env, obj):
        asked.append(obj)
        return {"status": "ok", "object": f"source:{obj}", "counts": {"gold": 1},
                "nodes": {"items": [{"node": "gold:g", "path": "x"}, {"node": "silver:t.c", "path": "y"}]}}

    removed = Change("t", "removed", "c", incremental=True)
    added = Change("t", "added", "d", incremental=True)
    assert agent.impact_of(get_impact, removed)["gold_and_rules"] == ["gold:g"]
    agent.impact_of(get_impact, added)
    assert asked == ["t.c", "t"]
    assert agent.impact_of(None, removed).startswith("not available")
    assert agent.impact_of(lambda e, o: {"status": "error", "error": "unknown object"}, removed).startswith("not available")
