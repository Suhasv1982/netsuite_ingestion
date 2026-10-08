"""tools/manifest_table.py and the pure parts of tools/recompute_manifest.py."""

import datetime as dt
import decimal
import json
from collections import Counter

import pytest

import manifest_table as mt
import recompute_manifest as rm

MANIFEST = {
    "mode": "increment", "seed": 774551981137641, "batch_date": "2026-10-08",
    "defects": [
        {"defect": "invalid_enum", "table": "netsuite_memberships", "row_count": 2, "business_keys": [5001, 5002],
         "observable_in_bronze": True, "column": "membership_status"},
        {"defect": "late_arriving", "table": "netsuite_transactions", "row_count": 1, "business_keys": [9],
         "observable_in_bronze": True, "current_watermark": "2026-10-07",
         "rows": [{"key": 9, "created_date": "2026-07-11", "updated_date": "2026-07-12"}]},
        {"defect": "null_business_key", "table": "netsuite_transactions", "row_count": 1, "business_keys": [None],
         "observable_in_bronze": True},
    ],
}


class TestManifestRows:
    def test_one_row_per_defect_row(self):
        rows = mt.manifest_rows(MANIFEST, "job")
        assert [(r["defect"], r["table_name"], r["business_key"]) for r in rows] == [
            ("invalid_enum", "netsuite_memberships", "5001"), ("invalid_enum", "netsuite_memberships", "5002"),
            ("late_arriving", "netsuite_transactions", "9"), ("null_business_key", "netsuite_transactions", None)]
        assert {r["batch_date"] for r in rows} == {"2026-10-08"} and {r["seed"] for r in rows} == {774551981137641}

    def test_column_and_details(self):
        enum, _, late, _ = mt.manifest_rows(MANIFEST, "recomputed", "51b0962")
        assert enum["column_name"] == "membership_status" and enum["detail"] is None
        assert json.loads(late["detail"]) == {"current_watermark": "2026-10-07",
                                              "row": {"key": 9, "created_date": "2026-07-11", "updated_date": "2026-07-12"}}
        assert late["source"] == "recomputed" and late["code_version"] == "51b0962"

    def test_unknown_source_is_refused(self):
        with pytest.raises(ValueError):
            mt.manifest_rows(MANIFEST, "guess")

    def test_columns_match_the_ddl(self):
        assert set(mt.manifest_rows(MANIFEST, "job")[0]) == {n for n, _ in mt.COLUMNS} - {"recorded_at"}
        assert mt.ddl("c.s.t").startswith("CREATE TABLE IF NOT EXISTS c.s.t (batch_date DATE, seed BIGINT")


class TestReplaceBatchStatements:
    def test_deletes_the_batch_then_inserts_in_chunks(self):
        rows = mt.manifest_rows(MANIFEST, "job")
        stmts = mt.replace_batch_statements("t", rows, MANIFEST, "job", chunk=3)
        assert stmts[0] == ("DELETE FROM t WHERE batch_date = DATE'2026-10-08' AND seed = 774551981137641 "
                            "AND source = 'job'")
        assert len(stmts) == 3 and all(s.startswith("INSERT INTO t (batch_date, seed,") for s in stmts[1:])
        assert "CAST(NULL AS STRING)" in stmts[2] and "current_timestamp()" in stmts[1]

    def test_strings_are_escaped(self):
        assert mt._sql_literal("it's \\ odd", "STRING") == "'it\\'s \\\\ odd'"


class TestRecomputeComparison:
    COLS = ["k", "d", "amount"]

    def test_values_from_postgres_and_the_generator_compare_equal(self):
        pg = {"k": 1, "d": dt.date(2026, 10, 8), "amount": decimal.Decimal("12.00")}
        gen = {"k": "1", "d": "2026-10-08", "amount": 12}
        assert rm.row_key(pg, self.COLS) == rm.row_key(gen, self.COLS)

    def test_multiset_diff_counts_duplicates(self):
        pre = [{"k": 1}, {"k": 2}]
        post = pre + [{"k": 3}, {"k": 3}]
        assert rm.multiset_diff(post, pre, ["k"]) == Counter({("3",): 2})

    def test_compare_reports_both_sides(self):
        out = rm.compare(Counter({("1",): 1, ("2",): 1}), Counter({("1",): 1, ("3",): 1}))
        assert out == {"generated": 2, "actual": 2, "only_generated": 1, "only_actual": 1, "match": False}
