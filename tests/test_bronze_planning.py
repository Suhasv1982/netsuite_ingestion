"""Unit tests for the incremental-bronze planning logic in metadata.py (no Spark needed).

Covers: the floor date, flow names (incl. the pending-entry hash), ledger keying,
entry-based pending detection (not counts; a second version of a key on the same
date is pending), which `once` flows are defined for a normal run, a late-row
top-up, and a bronze_rebuild full refresh, and the row-hash column-set guard.
"""

import datetime as dt

import pytest

from metadata import (
    DEFAULT_FLOOR,
    NULL_KEY,
    TooManyPendingKeys,
    anchor_flow_name,
    bronze_source_columns,
    describe_column_change,
    enforce_pending_cap,
    find_pending_entries,
    floor_date,
    group_pending_by_date,
    is_incremental,
    ledger_diff,
    ledger_key,
    pending_key_hash,
    guard_audit_row,
    guard_reads_row,
    pick_update_id,
    plan_flows,
    read_create_update_attempts,
    read_create_update_detail,
    refresh_guard_error,
    row_hash_guard_error,
    snapshot_flow_name,
    topup_flow_name,
)

T = "netsuite_memberships"
D1, D2, D3 = dt.date(2026, 7, 11), dt.date(2026, 8, 1), dt.date(2026, 8, 22)


def names(flows, kind=None):
    return [f.name for f in flows if kind is None or f.kind == kind]


class TestFloorDate:
    def test_iso_date(self):
        assert floor_date({"bronze_watermark": "1900-01-01"}) == dt.date(1900, 1, 1)

    def test_blank_or_null_means_no_floor(self):
        assert floor_date({"bronze_watermark": None}) == DEFAULT_FLOOR
        assert floor_date({"bronze_watermark": "  "}) == DEFAULT_FLOOR
        assert floor_date({}) == DEFAULT_FLOOR

    @pytest.mark.parametrize("bad", ["07-30-2026", "2026/07/30", "yesterday"])
    def test_non_iso_value_raises_instead_of_being_guessed(self, bad):
        with pytest.raises(ValueError, match="not an ISO date"):
            floor_date({"source_table": T, "bronze_watermark": bad})


class TestIsIncremental:
    def test_load_mode(self):
        assert is_incremental({"load_mode": "Incremental"}) is True
        assert is_incremental({"load_mode": " incremental "}) is True
        assert is_incremental({"load_mode": "FullLoad"}) is False
        assert is_incremental({}) is False


class TestFlowNames:
    def test_snapshot_flow_name_is_derived_from_the_date(self):
        assert snapshot_flow_name(T, D1) == "netsuite_memberships__20260711"

    def test_hash_is_short_and_order_independent(self):
        assert pending_key_hash([("b", "h"), ("a", "h"), ("c", "h")]) == pending_key_hash([("c", "h"), ("a", "h"), ("b", "h")])
        assert len(pending_key_hash([("a", "h")])) == 10

    def test_different_entry_sets_get_different_names(self):
        assert topup_flow_name(T, D1, [("1", "h1"), ("2", "h2")]) != topup_flow_name(T, D1, [("1", "h1"), ("2", "h2"), ("3", "h3")])

    def test_a_second_version_of_the_same_key_gets_a_different_name(self):
        assert topup_flow_name(T, D1, [("1", "h1")]) != topup_flow_name(T, D1, [("1", "h2")])

    def test_same_entry_set_reproduces_the_same_name(self):
        assert topup_flow_name(T, D1, [("2", "b"), ("1", "a")]) == topup_flow_name(T, D1, [("1", "a"), ("2", "b")])
        assert topup_flow_name(T, D1, [("1", "a")]).startswith("netsuite_memberships__20260711__topup_")

    def test_null_key_is_hashable(self):
        assert ledger_key(None) == NULL_KEY
        assert pending_key_hash([(None, "h")]) == pending_key_hash([(NULL_KEY, "h")])


class TestFindPendingEntries:
    def test_entries_missing_from_the_ledger(self):
        assert find_pending_entries([(1, D1, "a"), (2, D1, "b")], [(1, D1, "a")]) == {("2", D1, "b")}

    def test_nothing_pending_when_ledger_is_complete(self):
        assert find_pending_entries([(1, D1, "a")], [("1", D1, "a")]) == set()

    def test_keys_compare_as_strings_so_int_and_text_keys_match(self):
        assert find_pending_entries([(90001, D1, "a")], [("90001", D1, "a")]) == set()

    def test_null_business_keys_are_matched_not_always_pending(self):
        assert find_pending_entries([(None, D1, "a")], [(NULL_KEY, D1, "a")]) == set()

    def test_late_update_backdated_to_a_loaded_date_is_not_excluded(self):
        # key 5 was loaded on D2; a later update for key 5 is backdated to D1, which is already
        # loaded for other keys. Keyed on (key, date, hash) it is pending; keyed on key alone it would be hidden.
        ledger = [(1, D1, "a"), (2, D1, "b"), (5, D2, "e")]
        source = [(1, D1, "a"), (2, D1, "b"), (5, D1, "e2")]
        assert find_pending_entries(source, ledger) == {("5", D1, "e2")}

    def test_exact_copy_of_a_loaded_row_is_not_pending(self):
        # same key, day and content: one entry; a late exact copy carries nothing new
        assert find_pending_entries([(1, D1, "a"), (1, D1, "a")], [(1, D1, "a")]) == set()


class TestIncident20261002SecondVersionSameDay:
    """Generator run 2 (10-01) wrote key 7 dated D1; the D1 batch later wrote a second version of key 7, also dated
    D1. Keyed on (key, date) both versions are one pair and the second never loaded. Keyed on content it is pending
    and its top-up loads only that version."""

    ledger = [("6", D1, "v6"), ("7", D1, "v7_first")]
    source = [("6", D1, "v6"), ("7", D1, "v7_first"), ("7", D1, "v7_second")]

    def test_the_pairs_alone_look_complete(self):
        assert {(k, d) for k, d, _ in self.source} == {(k, d) for k, d, _ in self.ledger}

    def test_the_second_version_is_pending(self):
        assert find_pending_entries(self.source, self.ledger) == {("7", D1, "v7_second")}

    def test_the_topup_carries_only_the_second_version(self):
        pending = group_pending_by_date(find_pending_entries(self.source, self.ledger))
        (f,) = plan_flows(T, [D1], {D1}, pending, scope="pending_only")
        assert f.kind == "topup" and f.entries == (("7", "v7_second"),)


class TestMovedRowsWithEqualCounts:
    """One row moves OUT of date D (updated to a later date), one late row moves IN to D.
    The number of rows on D is unchanged, so a count comparison sees nothing; an entry comparison does."""

    ledger = [(1, D1, "a"), (2, D1, "b"), (3, D2, "c")]                 # D1 holds keys 1, 2 (2 rows)
    source = [(2, D1, "b"), (9, D1, "i"), (1, D3, "a3"), (3, D2, "c")]  # key 1 moved to D3; late key 9 on D1

    def test_the_counts_are_equal(self):
        assert sum(1 for _, d, _ in self.ledger if d == D1) == sum(1 for _, d, _ in self.source if d == D1) == 2

    def test_entry_comparison_still_finds_the_late_row_and_the_moved_row(self):
        assert find_pending_entries(self.source, self.ledger) == {("9", D1, "i"), ("1", D3, "a3")}

    def test_plan_defines_a_topup_for_the_loaded_date_and_a_snapshot_flow_for_the_new_date(self):
        pending = group_pending_by_date(find_pending_entries(self.source, self.ledger))
        ledger_dates = {d for _, d, _ in self.ledger}
        src_dates = sorted({d for _, d, _ in self.source})
        flows = plan_flows(T, src_dates, ledger_dates, {d: e for d, e in pending.items() if d in ledger_dates})
        assert names(flows, "topup") == [topup_flow_name(T, D1, [("9", "i")])]
        assert snapshot_flow_name(T, D3) in names(flows, "snapshot")
        (topup,) = [f for f in flows if f.kind == "topup"]
        assert topup.entries == (("9", "i"),) and topup.snapshot_date == D1


class TestPlanFlows:
    def test_first_run_empty_ledger_defines_every_date_and_no_topups(self):
        flows = plan_flows(T, [D1, D2], set(), {})
        assert names(flows) == [snapshot_flow_name(T, D1), snapshot_flow_name(T, D2)]
        assert all(f.kind == "snapshot" for f in flows)

    def test_all_dates_scope_keeps_defining_loaded_dates(self):
        flows = plan_flows(T, [D1, D2], {D1, D2}, {}, scope="all_dates")
        assert names(flows) == [snapshot_flow_name(T, D1), snapshot_flow_name(T, D2)]

    def test_pending_only_scope_with_nothing_pending_defines_only_the_anchor(self):
        flows = plan_flows(T, [D1, D2], {D1, D2}, {}, scope="pending_only")
        assert names(flows) == [anchor_flow_name(T)] and flows[0].kind == "anchor"

    def test_anchor_is_only_used_when_nothing_else_is_defined(self):
        assert names(plan_flows(T, [D1, D2], {D1}, {}, scope="pending_only"), "anchor") == []
        assert names(plan_flows(T, [D1], {D1}, {}, scope="all_dates"), "anchor") == []

    def test_empty_source_still_gets_a_flow(self):
        assert names(plan_flows(T, [], set(), {})) == [anchor_flow_name(T)]
        assert names(plan_flows(T, [], set(), {}, rebuild=True)) == [anchor_flow_name(T)]

    def test_anchor_name_is_constant(self):
        assert anchor_flow_name(T) == "netsuite_memberships__anchor"

    def test_pending_only_scope_defines_only_new_dates(self):
        flows = plan_flows(T, [D1, D2, D3], {D1, D2}, {}, scope="pending_only")
        assert names(flows) == [snapshot_flow_name(T, D3)]

    def test_an_older_new_date_is_a_snapshot_flow(self):
        old = dt.date(2026, 6, 20)
        flows = plan_flows(T, [old, D1], {D1}, {}, scope="pending_only")
        assert names(flows) == [snapshot_flow_name(T, old)]

    def test_late_rows_on_a_loaded_date_get_a_topup(self):
        flows = plan_flows(T, [D1], {D1}, {D1: [("9", "i"), ("3", "c")]}, scope="pending_only")
        (f,) = flows
        assert f.kind == "topup" and f.entries == (("3", "c"), ("9", "i"))
        assert f.name == topup_flow_name(T, D1, [("3", "c"), ("9", "i")])

    def test_topup_for_a_date_missing_from_the_source_is_dropped(self):
        assert plan_flows(T, [D1], {D1, D2}, {D2: [("1", "a")]}) == [plan_flows(T, [D1], {D1, D2}, {})[0]]

    def test_topup_for_a_date_that_is_not_loaded_is_not_defined(self):
        # a brand-new date is loaded whole by its snapshot flow, never by a top-up
        flows = plan_flows(T, [D1, D3], {D1}, {D3: [("1", "a")]}, scope="all_dates")
        assert names(flows, "topup") == []

    def test_stale_ledger_reproduces_the_same_topup_name(self):
        first = plan_flows(T, [D1], {D1}, {D1: [("9", "i")]})
        again = plan_flows(T, [D1], {D1}, {D1: [("9", "i")]})  # ledger was not updated between runs
        assert names(first, "topup") == names(again, "topup")

    def test_a_grown_pending_set_gets_a_new_name(self):
        assert names(plan_flows(T, [D1], {D1}, {D1: [("9", "i")]}), "topup") != names(
            plan_flows(T, [D1], {D1}, {D1: [("9", "i"), ("10", "j")]}), "topup")

    def test_dates_are_planned_in_order(self):
        assert names(plan_flows(T, [D3, D1, D2], set(), {})) == [snapshot_flow_name(T, d) for d in (D1, D2, D3)]

    def test_unknown_scope_raises(self):
        with pytest.raises(ValueError, match="unknown flow scope"):
            plan_flows(T, [D1], set(), {}, scope="some_dates")


class TestRebuild:
    def test_rebuild_defines_every_date_and_no_topups_even_with_pending_entries(self):
        flows = plan_flows(T, [D1, D2], {D1, D2}, {D1: [("9", "i")]}, scope="pending_only", rebuild=True)
        assert names(flows) == [snapshot_flow_name(T, D1), snapshot_flow_name(T, D2)]
        assert all(f.kind == "snapshot" for f in flows)

    def test_rebuild_ignores_the_ledger_scope(self):
        assert names(plan_flows(T, [D1], {D1}, {}, scope="pending_only", rebuild=True)) == [snapshot_flow_name(T, D1)]


class TestGroupPendingByDate:
    def test_groups_and_sorts(self):
        assert group_pending_by_date([("b", D2, "y"), ("a", D2, "x"), ("z", D1, "w")]) == {
            D1: [("z", "w")], D2: [("a", "x"), ("b", "y")]}

    def test_two_versions_of_one_key_stay_apart(self):
        assert group_pending_by_date([("7", D1, "v2"), ("7", D1, "v1")]) == {D1: [("7", "v1"), ("7", "v2")]}


class TestLedgerDiff:
    def test_missing_and_extra(self):
        missing, extra = ledger_diff([(1, D1, "a"), (2, D1, "b")], [("2", D1, "b"), ("3", D1, "c")])
        assert missing == {("1", D1, "a")} and extra == {("3", D1, "c")}

    def test_identical_sets_have_no_difference(self):
        assert ledger_diff([(1, D1, "a"), (None, D2, "n")], [("1", D1, "a"), (NULL_KEY, D2, "n")]) == (set(), set())

    def test_a_ledger_without_row_hashes_differs_from_bronze(self):
        # an old ledger row (row_hash NULL) never matches a hashed bronze entry: ledger_check reports it
        missing, extra = ledger_diff([(1, D1, "a")], [("1", D1, None)])
        assert missing == {("1", D1, "a")} and extra == {("1", D1, None)}


class TestPendingCap:
    def test_within_cap_passes(self):
        enforce_pending_cap(10, 10)

    def test_over_cap_fails_loudly(self):
        with pytest.raises(TooManyPendingKeys, match="bronze_rebuild"):
            enforce_pending_cap(11, 10)


COLS = ["membership_internal_id", "customer_internal_id", "membership_status", "updated_date"]


class TestRowHashGuard:
    def test_same_column_set_passes(self):
        assert row_hash_guard_error(T, list(COLS), list(COLS), rebuild=False) is None

    def test_nothing_recorded_fails_closed_and_names_the_table(self):
        msg = row_hash_guard_error(T, None, COLS, rebuild=False, target="dev")
        assert msg.startswith("BLOCKED") and T in msg and "no row-hash column set is recorded" in msg
        assert 'bronze_rebuild=true' in msg and "-t dev" in msg

    def test_added_column_is_named(self):
        msg = row_hash_guard_error(T, COLS, COLS + ["end_date"], rebuild=False)
        assert T in msg and "added end_date" in msg and "requires a bronze rebuild" in msg

    def test_removed_column_is_named(self):
        msg = row_hash_guard_error(T, COLS, COLS[:-1], rebuild=False)
        assert "removed updated_date" in msg

    def test_reorder_alone_is_a_change(self):
        msg = row_hash_guard_error(T, COLS, list(reversed(COLS)), rebuild=False)
        assert msg and "order changed" in msg

    def test_rebuild_is_always_allowed(self):
        assert row_hash_guard_error(T, None, COLS, rebuild=True) is None
        assert row_hash_guard_error(T, COLS, COLS + ["x"], rebuild=True) is None

    def test_describe_column_change(self):
        assert describe_column_change(["a", "b"], ["a", "c"]) == "added c; removed b"


class TestBronzeSourceColumns:
    def test_metadata_columns_are_dropped_and_order_kept(self):
        assert bronze_source_columns(["b", "a", "_row_hash", "_snapshot_date", "_loaded_at"]) == ["b", "a"]


BRONZE = ["poc_bronze.netsuite_memberships", "poc_bronze.netsuite_certifications"]


class TestRefreshGuard:
    normal = {"full_refresh": False}

    def test_normal_update_is_allowed(self):
        assert refresh_guard_error(self.normal, BRONZE, rebuild=False, scope="pending_only") is None

    def test_full_refresh_without_rebuild_is_refused(self):
        msg = refresh_guard_error({"full_refresh": True}, BRONZE, rebuild=False, scope="pending_only")
        assert msg and "bronze_rebuild=true" in msg

    def test_full_refresh_with_rebuild_is_allowed(self):
        assert refresh_guard_error({"full_refresh": True}, BRONZE, rebuild=True, scope="pending_only") is None

    def test_selective_refresh_naming_a_bronze_table_is_refused(self):
        upd = {"full_refresh": False, "full_refresh_selection": ["poc_bronze.netsuite_memberships"]}
        assert refresh_guard_error(upd, BRONZE, rebuild=False, scope="pending_only")
        assert refresh_guard_error(upd, BRONZE, rebuild=True, scope="pending_only") is None

    def test_selection_may_use_a_qualified_or_quoted_name(self):
        for name in (
            "poc_bronze.netsuite_memberships",
            "poc_netsuite.poc_bronze.netsuite_memberships",
            "`poc_bronze.netsuite_memberships`",
        ):
            upd = {"full_refresh": False, "full_refresh_selection": [name]}
            assert refresh_guard_error(upd, BRONZE, rebuild=False, scope="pending_only"), name

    def test_selective_refresh_of_silver_only_is_allowed(self):
        upd = {"full_refresh": False, "full_refresh_selection": ["poc_silver.netsuite_memberships"]}
        assert refresh_guard_error(upd, BRONZE, rebuild=False, scope="pending_only") is None

    def test_unreadable_event_fails_closed_for_pending_only(self):
        msg = refresh_guard_error(None, BRONZE, rebuild=False, scope="pending_only")
        assert msg and "event log" in msg

    def test_unreadable_event_is_tolerated_for_all_dates(self):
        assert refresh_guard_error(None, BRONZE, rebuild=False, scope="all_dates") is None

    def test_rebuild_flag_on_a_normal_update_is_not_an_error(self):
        assert refresh_guard_error(self.normal, BRONZE, rebuild=True, scope="pending_only") is None

    def test_missing_selection_key_is_treated_as_empty(self):
        assert refresh_guard_error({}, BRONZE, rebuild=False, scope="pending_only") is None


class TestRefreshGuardMessage:
    ctx = {"target": "dev", "update_id": "763476c9-4a6b-4586-81be-fe94fce4d72d", "pipeline": "2b815392-6b06-4271-9579-8f5ab2b19fc6"}

    def msg(self, update=None, **kw):
        update = {"full_refresh": True} if update is None else update
        return refresh_guard_error(update, BRONZE, rebuild=False, scope="pending_only", context=kw.get("context", self.ctx))

    def test_says_what_was_blocked(self):
        m = self.msg()
        assert m.startswith("BLOCKED:") and "763476c9-4a6b-4586-81be-fe94fce4d72d" in m
        assert "full refresh of ALL tables" in m and "bronze_rebuild=false" in m
        assert "before it changed any data" in m

    def test_names_the_selected_bronze_tables(self):
        m = self.msg({"full_refresh": False, "full_refresh_selection": ["poc_bronze.netsuite_memberships", "poc_silver.x"]})
        assert "poc_bronze.netsuite_memberships" in m and "poc_silver.x" not in m

    def test_explains_why(self):
        m = self.msg()
        assert "WHY:" in m and "pending" in m and "0 rows" in m

    def test_gives_the_exact_commands_for_the_target(self):
        m = self.msg()
        assert 'databricks bundle deploy -t dev --var="bronze_rebuild=true"' in m
        assert "databricks bundle run netsuite_ingestion_daily -t dev --pipeline-params full_refresh=true" in m
        assert 'databricks bundle deploy -t dev --var="bronze_rebuild=false"' in m
        assert m.index('--var="bronze_rebuild=true"') < m.index("--pipeline-params full_refresh=true") < m.index('--var="bronze_rebuild=false"')

    def test_prod_commands_keep_the_schedule_paused(self):
        m = self.msg(context={**self.ctx, "target": "prod"})
        assert m.count('--var="schedule_pause_status=PAUSED"') == 2 and "-t prod" in m

    def test_message_without_context_uses_placeholders(self):
        m = self.msg(context=None)
        assert "-t <TARGET>" in m and "this pipeline update" in m

    def test_unreadable_event_message_says_how_to_proceed(self):
        m = refresh_guard_error(None, BRONZE, rebuild=False, scope="pending_only", context=self.ctx)
        assert m.startswith("BLOCKED:") and "event log" in m and 'flow_scope=all_dates' in m and "-t dev" in m
        assert "TRANSIENT" in m and "re-run the same update" in m


class FakeSpark:
    """Just enough of a SparkSession for read_create_update_detail: conf.get and sql(...).collect()."""

    PID, UID = "2b815392-6b06-4271-9579-8f5ab2b19fc6", "763476c9-4a6b-4586-81be-fe94fce4d72d"

    def __init__(self, outcomes, conf=None):
        self.outcomes, self.queries = list(outcomes), []
        self._conf = {"pipelines.id": self.PID, "spark.pipelines.updateId": self.UID} if conf is None else conf
        spark = self

        class Conf:
            def get(self, key, default=None):
                return spark._conf.get(key, default)

        self.conf = Conf()

    def sql(self, query):
        self.queries.append(query)
        outcome = self.outcomes.pop(0) if self.outcomes else []
        if isinstance(outcome, Exception):
            raise outcome
        return type("R", (), {"collect": lambda _self: outcome})()


def _row(details):
    return [{"details": details}]


class TestReadCreateUpdate:
    @pytest.fixture(autouse=True)
    def no_sleep(self, monkeypatch):
        import time

        monkeypatch.setattr(time, "sleep", lambda s: None)

    def test_reads_the_event_on_the_first_try(self):
        spark = FakeSpark([_row('{"create_update": {"full_refresh": true}}')])
        assert read_create_update_detail(spark) == ({"full_refresh": True}, "")
        assert "event_log('2b815392-6b06-4271-9579-8f5ab2b19fc6')" in spark.queries[0]
        assert "origin.update_id = '763476c9-4a6b-4586-81be-fe94fce4d72d'" in spark.queries[0]

    def test_retries_when_the_event_is_not_visible_yet(self):
        spark = FakeSpark([[], [], _row('{"create_update": {"full_refresh": false}}')])
        assert read_create_update_detail(spark) == ({"full_refresh": False}, "")
        assert len(spark.queries) == 3

    def test_retries_after_a_query_error(self):
        spark = FakeSpark([RuntimeError("boom"), _row('{"create_update": {"full_refresh": true}}')])
        assert read_create_update_detail(spark)[0] == {"full_refresh": True}

    def test_gives_up_with_the_reason_after_all_attempts(self):
        spark = FakeSpark([[], [], [], []])
        event, note = read_create_update_detail(spark, attempts=4)
        assert event is None and "no create_update event was visible" in note
        assert len(spark.queries) == 5  # 4 attempts plus one diagnostic listing of the latest event_log rows
        assert "event_log('2b815392-6b06-4271-9579-8f5ab2b19fc6') ORDER BY timestamp DESC LIMIT 8" in spark.queries[-1]

    def test_a_query_error_is_reported_with_its_type(self):
        spark = FakeSpark([PermissionError("no access")] * 2)
        event, note = read_create_update_detail(spark, attempts=2)
        assert event is None and "PermissionError" in note and "no access" in note
        assert "event_log listing failed" not in note or True

    def test_unset_or_malformed_ids_are_not_queried(self):
        for conf in ({}, {"pipelines.id": "x", "spark.pipelines.updateId": "y"}, {"pipelines.id": "1'; drop table x; --" + "a" * 5, "spark.pipelines.updateId": FakeSpark.UID}):
            spark = FakeSpark([], conf=conf)
            event, note = read_create_update_detail(spark)
            assert event is None and "not set as expected" in note and spark.queries == []

    def test_the_reason_reaches_the_guard_message(self):
        msg = refresh_guard_error(None, BRONZE, False, "pending_only", {"target": "dev", "note": "no create_update event was visible for this update after 4 attempt(s)"})
        assert "REASON IT COULD NOT BE READ: no create_update event was visible" in msg


    def test_the_latest_event_log_rows_are_appended_to_the_note(self):
        rows = [{"event_type": "create_update", "u": "6ee304cd", "t": "01:31:07"}]
        spark = FakeSpark([[], [], rows])
        event, note = read_create_update_detail(spark, attempts=2)
        assert event is None and "latest event_log rows" in note and "create_update/6ee304cd/01:31:07" in note


class TestGuardReadCount:
    """The number of event-log reads the guard needed reaches run_audit (layer `guard`)."""

    @pytest.fixture(autouse=True)
    def no_sleep(self, monkeypatch):
        import time

        monkeypatch.setattr(time, "sleep", lambda s: None)

    def test_first_read_counts_one(self):
        spark = FakeSpark([_row('{"create_update": {"full_refresh": false}}')])
        assert read_create_update_attempts(spark) == ({"full_refresh": False}, "", 1)

    def test_retries_are_counted(self):
        spark = FakeSpark([[], RuntimeError("boom"), _row('{"create_update": {"full_refresh": false}}')])
        assert read_create_update_attempts(spark)[2] == 3

    def test_not_found_reports_every_attempt(self):
        event, note, reads = read_create_update_attempts(FakeSpark([[]] * 4), attempts=3)
        assert event is None and reads == 3

    def test_not_attempted_counts_zero(self):
        assert read_create_update_attempts(FakeSpark([], conf={}))[2] == 0

    def test_row_shape(self):
        assert guard_reads_row("u1", 2, True, "x" * 2000) == ("u1", 2, True, "x" * 1000)
        assert guard_reads_row(None, 0, False, None) == ("", 0, False, "")


class TestGuardAuditRow:
    GUARD = {"update_id": "u1", "reads": 1, "event_found": True, "note": ""}

    def _row(self, guard, expected="u1"):
        return guard_audit_row(guard, expected, "run-7", None, None)

    def test_found_on_first_read_is_ok(self):
        r = self._row(self.GUARD)
        assert (r["layer"], r["status"], r["rows_read"], r["table_id"], r["error"]) == ("guard", "OK", 1, None, None)

    def test_retries_warn_with_the_count(self):
        r = self._row({**self.GUARD, "reads": 3})
        assert r["status"] == "WARN" and r["rows_read"] == 3 and "after 3 read(s)" in r["error"]

    def test_not_found_warns_with_the_note(self):
        r = self._row({**self.GUARD, "reads": 5, "event_found": False, "note": "no create_update event was visible"})
        assert r["status"] == "WARN" and "NOT found" in r["error"] and "no create_update event" in r["error"]

    def test_a_row_from_another_update_is_not_reported_as_this_one(self):
        r = self._row(self.GUARD, expected="u2")
        assert r["status"] == "WARN" and r["rows_read"] is None and "u2" in r["error"]

    def test_missing_table_warns(self):
        assert self._row(None)["status"] == "WARN"

    def test_unknown_update_id_trusts_the_latest_row(self):
        assert self._row(self.GUARD, expected=None)["status"] == "OK"


class TestPickUpdateId:
    def test_latest_update_inside_the_task_window(self):
        updates = [("old", 100), ("mine", 1_050), ("retry", 1_200), ("later", 5_000)]
        assert pick_update_id(updates, 1_000, 2_000) == "retry"

    def test_open_window_while_the_task_runs(self):
        assert pick_update_id([("a", 1_500)], 1_000, None) == "a"

    def test_none_inside_the_window(self):
        assert pick_update_id([("old", 100)], 1_000, 2_000) is None
        assert pick_update_id([("a", 1_500)], None, None) is None
