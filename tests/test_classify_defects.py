"""Unit tests for baseline/classify_defects.py (no Databricks access)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "baseline"))

import classify_defects as cd  # noqa: E402

M = "netsuite_memberships"
OK = {"pipeline": {"state": "COMPLETED"}, "job": {"state": "SUCCESS"}}


def snap(silver_rows=(), rejects=(), **extra):
    s = {**OK, "row_state": {"silver": {M: {"rows": [list(r) for r in silver_rows]}}, "rejected": {M: [list(r) for r in rejects]}}}
    s.update(extra)
    return s


def entry(defect, keys, table=M):
    return {"defect": defect, "table": table, "row_count": len(keys), "business_keys": keys}


class TestClassifyEntry:
    def test_rejected_vs_passed_vs_absorbed(self):
        s = snap(silver_rows=[("1", "d", "d")], rejects=[("Not in List", "2")])
        out = cd.classify_entry(entry("invalid_enum", [1, 2, 3]), s)
        assert out == {"passed_to_silver": 1, "rejected": 1, "silently_absorbed": 1}

    def test_duplicate_pair_collapsed_is_absorbed(self):
        s = snap(silver_rows=[("7", "d", "d")])
        assert cd.classify_entry(entry("duplicate_business_key", [7]), s) == {"silently_absorbed": 1}

    def test_duplicate_pair_kept_is_passed(self):
        s = snap(silver_rows=[("7", "d", "d"), ("7", "d", "d")])
        assert cd.classify_entry(entry("duplicate_business_key", [7]), s) == {"passed_to_silver": 1}

    def test_null_keys_are_counted_not_matched_by_key(self):
        s = snap(silver_rows=[(None, "d", "d")] * 2, rejects=[("r", None)])
        out = cd.classify_entry(entry("null_business_key", [1, 2, 3, 4]), s)
        assert out == {"passed_to_silver": 2, "rejected": 1, "silently_absorbed": 1}

    def test_customers_are_bronze_only(self):
        assert cd.classify_entry(entry("customer_malformed_email", [1, 2], "netsuite_customers"), snap()) == {"bronze_only": 2}

    def test_failed_run_attributes_unresolved_rows_to_the_named_table(self):
        s = snap(silver_rows=[("1", "d", "d")])
        s["pipeline"] = {"state": "FAILED", "error_events": [{"message": f"flow {M} failed"}]}
        s["job"] = {"state": "FAILED"}
        assert cd.classify_entry(entry("negative_amount", [1, 2]), s) == {"passed_to_silver": 1, "caused_failure": 1}

    def test_failed_run_without_table_in_error_is_unresolved_not_absorbed(self):
        s = snap()
        s["pipeline"] = {"state": "FAILED", "error_events": [{"message": "something else"}]}
        s["job"] = {"state": "FAILED"}
        assert cd.classify_entry(entry("negative_amount", [1]), s) == {"unresolved": 1}

    def test_missing_state_is_unresolved(self):
        s = {**OK, "row_state": {"silver": {M: {"error": "boom"}}, "rejected": {M: []}}}
        assert cd.classify_entry(entry("negative_amount", [1]), s) == {"unresolved": 1}


class TestAggregate:
    def test_every_row_is_classified_exactly_once(self):
        manifest = {"defects": [entry("invalid_enum", [1, 2, 3]), entry("customer_null_company_name", [9], "netsuite_customers")]}
        classified = cd.classify_manifest(manifest, snap(silver_rows=[("1", "d", "d")]))
        assert all(sum(r["outcomes"].values()) == r["rows"] for r in classified)
        by_defect = cd.aggregate(classified, "defect")
        assert by_defect["invalid_enum"]["rows"] == 3
        assert cd.aggregate(classified, "table")["netsuite_customers"]["bronze_only"] == 1
