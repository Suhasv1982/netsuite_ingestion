"""Offline unit tests for the per-date fingerprint and the row-hash SQL (no Spark, no database).

The fingerprint is computed over distinct (business_key, date, row hash) entries. The pinned vectors below must be
identical in Python, Postgres and Spark SQL: tests/test_fingerprint_integration.py checks that (run with
NETSUITE_INTEGRATION=1; last run 2026-10-08 for the entry-based fingerprint). If an offline test here fails, the
algorithm changed: re-run the integration tests before touching the vectors, because the ledger stores these
numbers and a silent change would make every date look mismatched.
"""

import datetime as dt

import pytest

from metadata import (
    FP_HEX_CHARS,
    NULL_KEY,
    PG_MAX_FUNCTION_ARGS,
    SPARK_FINGERPRINT_SQL,
    date_fingerprints,
    entry_hash,
    find_pending_entries,
    group_pending_by_date,
    hashed_source_query,
    mismatched_dates,
    plan_flows,
    row_hash_sql,
    source_fingerprint_sql,
    spark_fingerprint_sql,
    topup_flow_name,
)

T = "netsuite_memberships"
D1, D2, D3 = dt.date(2026, 7, 11), dt.date(2026, 8, 1), dt.date(2026, 8, 22)
COLS = ["membership_internal_id", "updated_date"]


class TestPinnedVectors:
    def test_entry_hash_values(self):
        assert entry_hash("1", D1, "h1") == 124353006054509477
        assert entry_hash(None, D1, "h1") == 90522870186298299
        assert entry_hash("90001_1", D2, "h2") == 94291063350035145
        assert entry_hash("café-中", D2, "h3") == 620514413926447859

    def test_null_key_hashes_like_the_sentinel(self):
        assert entry_hash(None, D1, "h") == entry_hash(NULL_KEY, D1, "h")

    def test_hashes_are_60_bit_non_negative(self):
        assert FP_HEX_CHARS == 15
        assert 0 <= entry_hash("x", D1, "h") < 2**60

    def test_date_fingerprints_vector(self):
        entries = [("1", D1, "h1"), ("2", D1, "h2"), ("2", D1, "h2"), ("2", D1, "h2b"), (None, D1, "h0"),
                   ("90001_1", D2, "h2")]
        assert date_fingerprints(entries) == {D1: (4, "892318328983550916"), D2: (1, "94291063350035145")}


class TestDateFingerprints:
    def test_duplicate_entries_count_once(self):
        assert date_fingerprints([("1", D1, "a"), ("1", D1, "a")]) == date_fingerprints([("1", D1, "a")])

    def test_a_second_version_of_a_key_on_the_same_date_changes_the_fingerprint(self):
        # incident 2026-10-02: (key, date) pairs could not see this
        one, two = date_fingerprints([("7", D1, "v1")]), date_fingerprints([("7", D1, "v1"), ("7", D1, "v2")])
        assert one[D1] != two[D1] and two[D1][0] == 2

    def test_order_does_not_matter(self):
        entries = [("a", D1, "x"), ("b", D1, "y"), ("c", D2, "z")]
        assert date_fingerprints(entries) == date_fingerprints(list(reversed(entries)))

    def test_int_and_string_keys_are_the_same_entry(self):
        assert date_fingerprints([(5, D1, "h")]) == date_fingerprints([("5", D1, "h")])

    def test_key_date_and_row_hash_all_matter(self):
        assert entry_hash("1", D1, "h") != entry_hash("2", D1, "h")
        assert entry_hash("1", D1, "h") != entry_hash("1", D2, "h")
        assert entry_hash("1", D1, "h") != entry_hash("1", D1, "g")

    def test_sum_is_exact_beyond_64_bits(self):
        fp = date_fingerprints([(str(i), D1, "h") for i in range(64)])
        assert int(fp[D1][1]) > 2**63  # a bigint accumulator would overflow here

    def test_empty_input(self):
        assert date_fingerprints([]) == {}


class TestMismatchedDates:
    def test_identical_sides_have_no_mismatch(self):
        fp = date_fingerprints([("1", D1, "a"), ("2", D2, "b")])
        assert mismatched_dates(fp, dict(fp)) == []

    def test_dates_on_one_side_only_are_not_reported_here(self):
        both, one = date_fingerprints([("1", D1, "a"), ("2", D2, "b")]), date_fingerprints([("1", D1, "a")])
        assert mismatched_dates(both, one) == [] and mismatched_dates(one, both) == []

    def test_a_late_row_changes_count_and_sum(self):
        assert mismatched_dates(date_fingerprints([("1", D1, "a"), ("9", D1, "i")]),
                                date_fingerprints([("1", D1, "a")])) == [D1]

    def test_lists_and_tuples_compare_equal(self):
        assert mismatched_dates({D1: (1, "5")}, {D1: [1, "5"]}) == []


class TestOneRowMovesOutOneLateRowMovesIn:
    """Update moves key 1 out of D1 (to D3) while late key 9 lands on D1: D1 keeps 2 rows."""

    ledger = [("1", D1, "a"), ("2", D1, "b"), ("3", D2, "c")]
    source = [("2", D1, "b"), ("9", D1, "i"), ("1", D3, "a3"), ("3", D2, "c")]

    def test_the_counts_are_equal_but_the_fingerprints_are_not(self):
        led, src = date_fingerprints(self.ledger), date_fingerprints(self.source)
        assert led[D1][0] == src[D1][0] == 2
        assert led[D1] != src[D1]

    def test_only_the_changed_date_is_mismatched_and_the_untouched_date_is_skipped(self):
        led, src = date_fingerprints(self.ledger), date_fingerprints(self.source)
        assert mismatched_dates(src, led) == [D1]  # D2 matches, D3 is new (not in the ledger)

    def test_the_plan_defines_a_topup_for_d1_and_a_snapshot_flow_for_the_new_date(self):
        led, src = date_fingerprints(self.ledger), date_fingerprints(self.source)
        bad = set(mismatched_dates(src, led))
        pending = group_pending_by_date(find_pending_entries([e for e in self.source if e[1] in bad],
                                                             [e for e in self.ledger if e[1] in bad]))
        flows = plan_flows(T, sorted(src), set(led), pending, scope="pending_only")
        (topup,) = [f for f in flows if f.kind == "topup"]
        assert topup.snapshot_date == D1 and topup.entries == (("9", "i"),)
        assert topup.name == topup_flow_name(T, D1, [("9", "i")])
        assert [f.snapshot_date for f in flows if f.kind == "snapshot"] == [D3]


class TestRowHashSql:
    def test_hashes_the_given_columns_in_order_as_json(self):
        assert row_hash_sql(["b", "a"]) == 'md5(json_build_array("b", "a")::text)'

    def test_identifiers_are_quoted_and_escaped(self):
        assert row_hash_sql(['we"ird']) == 'md5(json_build_array("we""ird")::text)'

    def test_more_than_100_columns_are_nested(self):
        cols = [f"c{i}" for i in range(PG_MAX_FUNCTION_ARGS + 1)]
        sql = row_hash_sql(cols)
        assert sql.startswith("md5(json_build_array(json_build_array(") and '"c100"' in sql

    def test_no_columns_is_an_error(self):
        with pytest.raises(ValueError):
            row_hash_sql([])

    def test_hashed_source_query_selects_only_the_given_columns_plus_the_hash(self):
        q = hashed_source_query("netsuite", "netsuite_transactions", ["transaction_internal_id", "updated_date"])
        assert q == ('(SELECT "transaction_internal_id", "updated_date", '
                     'md5(json_build_array("transaction_internal_id", "updated_date")::text) AS _row_hash '
                     'FROM "netsuite"."netsuite_transactions") src')
        assert "custbody_region" not in q and "*" not in q  # raw source columns outside source_columns are not read


class TestSqlBuilders:
    def test_postgres_query_shape(self):
        sql = source_fingerprint_sql("netsuite", T, "membership_internal_id", "updated_date", dt.date(1900, 1, 1), COLS)
        assert sql.startswith("(SELECT d AS snapshot_date, count(*) AS row_count, sum(x)::text AS fp_sum")
        assert sql.endswith(") fp")  # a JDBC subquery needs an alias
        assert '"netsuite"."netsuite_memberships"' in sql
        assert "SELECT DISTINCT coalesce(\"membership_internal_id\"::text, '<NULL>') AS k" in sql
        assert row_hash_sql(COLS) + " AS h" in sql
        assert "\"updated_date\" >= DATE '1900-01-01'" in sql
        assert "substr(md5(k || '|' || to_char(d, 'YYYY-MM-DD') || '|' || h), 1, 15)" in sql
        assert "::bit(60)::bigint" in sql

    def test_identifiers_are_quoted_and_escaped(self):
        sql = source_fingerprint_sql('we"ird', "t", 'k"ey', "wm", dt.date(2000, 1, 1), ["c"])
        assert '"we""ird"."t"' in sql and '"k""ey"' in sql

    def test_spark_query_uses_the_same_hash_recipe(self):
        sql = spark_fingerprint_sql("R")
        assert "md5(concat(k, '|', date_format(d, 'yyyy-MM-dd'), '|', h))" in sql
        assert "substring(" in sql and ", 1, 15), 16, 10)" in sql
        assert "decimal(38, 0)" in sql  # exact wide sum
        assert "FROM (SELECT DISTINCT k, d, h FROM R)" in sql
        assert "{relation}" in SPARK_FINGERPRINT_SQL

    def test_both_sides_hash_the_same_number_of_hex_chars(self):
        pg = source_fingerprint_sql("s", "t", "k", "d", dt.date(1900, 1, 1), ["k", "d"])
        assert f"1, {FP_HEX_CHARS})" in pg and f"1, {FP_HEX_CHARS})" in spark_fingerprint_sql("R")
        assert f"::bit({FP_HEX_CHARS * 4})" in pg
