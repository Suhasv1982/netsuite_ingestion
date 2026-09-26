"""Offline unit tests for the per-date fingerprint (no Spark, no database).

The pinned vectors below were checked to be identical in Python, Postgres and
Spark SQL on 2026-09-25 by tests/test_fingerprint_integration.py (run with
NETSUITE_INTEGRATION=1). If an offline test here fails, the algorithm changed:
re-run the integration tests before touching the vectors, because the ledger
stores these numbers and a silent change would make every date look mismatched.
"""

import datetime as dt

import pytest

from metadata import (
    FP_HEX_CHARS,
    NULL_KEY,
    SPARK_FINGERPRINT_SQL,
    date_fingerprints,
    find_pending_pairs,
    group_pending_by_date,
    mismatched_dates,
    pair_hash,
    plan_flows,
    source_fingerprint_sql,
    spark_fingerprint_sql,
    topup_flow_name,
)

T = "netsuite_memberships"
D1, D2, D3 = dt.date(2026, 7, 11), dt.date(2026, 8, 1), dt.date(2026, 8, 22)


class TestPinnedVectors:
    def test_pair_hash_values(self):
        assert pair_hash("1", D1) == 1032610721312200158
        assert pair_hash(None, D1) == 983488822622139267
        assert pair_hash("90001_1", D2) == 394247485405758848
        assert pair_hash("café-中", D2) == 159244595734093444

    def test_null_key_hashes_like_the_sentinel(self):
        assert pair_hash(None, D1) == pair_hash(NULL_KEY, D1)

    def test_hashes_are_60_bit_non_negative(self):
        assert FP_HEX_CHARS == 15
        assert 0 <= pair_hash("x", D1) < 2**60

    def test_date_fingerprints_vector(self):
        pairs = [("1", D1), ("2", D1), ("2", D1), (None, D1), ("90001_1", D2)]
        assert date_fingerprints(pairs) == {D1: (3, "2379024750800249540"), D2: (1, "394247485405758848")}


class TestDateFingerprints:
    def test_duplicate_pairs_count_once(self):
        assert date_fingerprints([("1", D1), ("1", D1)]) == date_fingerprints([("1", D1)])

    def test_order_does_not_matter(self):
        pairs = [("a", D1), ("b", D1), ("c", D2)]
        assert date_fingerprints(pairs) == date_fingerprints(list(reversed(pairs)))

    def test_int_and_string_keys_are_the_same_pair(self):
        assert date_fingerprints([(5, D1)]) == date_fingerprints([("5", D1)])

    def test_the_key_and_the_date_both_matter(self):
        assert date_fingerprints([("1", D1)])[D1] != date_fingerprints([("2", D1)])[D1]
        assert pair_hash("1", D1) != pair_hash("1", D2)

    def test_sum_is_exact_beyond_64_bits(self):
        fp = date_fingerprints([(str(i), D1) for i in range(64)])
        assert int(fp[D1][1]) > 2**63  # a bigint accumulator would overflow here

    def test_empty_input(self):
        assert date_fingerprints([]) == {}


class TestMismatchedDates:
    def test_identical_sides_have_no_mismatch(self):
        fp = date_fingerprints([("1", D1), ("2", D2)])
        assert mismatched_dates(fp, dict(fp)) == []

    def test_dates_on_one_side_only_are_not_reported_here(self):
        assert mismatched_dates(date_fingerprints([("1", D1), ("2", D2)]), date_fingerprints([("1", D1)])) == []
        assert mismatched_dates(date_fingerprints([("1", D1)]), date_fingerprints([("1", D1), ("2", D2)])) == []

    def test_a_late_row_changes_count_and_sum(self):
        assert mismatched_dates(date_fingerprints([("1", D1), ("9", D1)]), date_fingerprints([("1", D1)])) == [D1]

    def test_lists_and_tuples_compare_equal(self):
        assert mismatched_dates({D1: (1, "5")}, {D1: [1, "5"]}) == []


class TestOneRowMovesOutOneLateRowMovesIn:
    """Update moves key 1 out of D1 (to D3) while late key 9 lands on D1: D1 keeps 2 rows."""

    ledger = [("1", D1), ("2", D1), ("3", D2)]
    source = [("2", D1), ("9", D1), ("1", D3), ("3", D2)]

    def test_the_counts_are_equal_but_the_fingerprints_are_not(self):
        led, src = date_fingerprints(self.ledger), date_fingerprints(self.source)
        assert led[D1][0] == src[D1][0] == 2
        assert led[D1] != src[D1]

    def test_only_the_changed_date_is_mismatched_and_the_untouched_date_is_skipped(self):
        led, src = date_fingerprints(self.ledger), date_fingerprints(self.source)
        assert mismatched_dates(src, led) == [D1]  # D2 matches, D3 is new (not in the ledger)

    def test_pairs_are_fetched_only_for_the_mismatched_date_and_find_the_late_row(self):
        led, src = date_fingerprints(self.ledger), date_fingerprints(self.source)
        bad = set(mismatched_dates(src, led))
        src_pairs = [(k, d) for k, d in self.source if d in bad]
        led_pairs = [(k, d) for k, d in self.ledger if d in bad]
        assert find_pending_pairs(src_pairs, led_pairs) == {("9", D1)}

    def test_the_plan_defines_a_topup_for_d1_and_a_snapshot_flow_for_the_new_date(self):
        led, src = date_fingerprints(self.ledger), date_fingerprints(self.source)
        bad = mismatched_dates(src, led)
        pending = group_pending_by_date(
            find_pending_pairs([(k, d) for k, d in self.source if d in bad], [(k, d) for k, d in self.ledger if d in bad])
        )
        flows = plan_flows(T, sorted(src), set(led), pending, scope="pending_only")
        assert {f.kind for f in flows} == {"snapshot", "topup"}
        (topup,) = [f for f in flows if f.kind == "topup"]
        assert topup.snapshot_date == D1 and topup.keys == ("9",) and topup.name == topup_flow_name(T, D1, ["9"])
        assert [f.snapshot_date for f in flows if f.kind == "snapshot"] == [D3]

    def test_a_count_comparison_would_have_missed_it(self):
        led, src = date_fingerprints(self.ledger), date_fingerprints(self.source)
        count_only_mismatch = [d for d in src if d in led and src[d][0] != led[d][0]]
        assert D1 not in count_only_mismatch  # counts alone see nothing wrong on D1


class TestSqlBuilders:
    def test_postgres_query_shape(self):
        sql = source_fingerprint_sql("netsuite", "netsuite_memberships", "membership_internal_id", "updated_date", dt.date(1900, 1, 1))
        assert sql.startswith("(SELECT d AS snapshot_date, count(*) AS row_count, sum(h)::text AS fp_sum")
        assert sql.endswith(") fp")  # a JDBC subquery needs an alias
        assert '"netsuite"."netsuite_memberships"' in sql
        assert "SELECT DISTINCT coalesce(\"membership_internal_id\"::text, '<NULL>')" in sql
        assert "\"updated_date\" >= DATE '1900-01-01'" in sql
        assert "substr(md5(k || '|' || to_char(d, 'YYYY-MM-DD')), 1, 15)" in sql
        assert "::bit(60)::bigint" in sql

    def test_identifiers_are_quoted_and_escaped(self):
        sql = source_fingerprint_sql('we"ird', "t", 'k"ey', "wm", dt.date(2000, 1, 1))
        assert '"we""ird"."t"' in sql and '"k""ey"' in sql

    def test_spark_query_uses_the_same_hash_recipe(self):
        sql = spark_fingerprint_sql("R")
        assert "md5(concat(k, '|', date_format(d, 'yyyy-MM-dd')))" in sql
        assert "substring(" in sql and ", 1, 15), 16, 10)" in sql
        assert "decimal(38, 0)" in sql  # exact wide sum
        assert "FROM (SELECT DISTINCT k, d FROM R)" in sql
        assert "{relation}" in SPARK_FINGERPRINT_SQL

    def test_both_sides_hash_the_same_number_of_hex_chars(self):
        pg = source_fingerprint_sql("s", "t", "k", "d", dt.date(1900, 1, 1))
        assert f"1, {FP_HEX_CHARS})" in pg and f"1, {FP_HEX_CHARS})" in spark_fingerprint_sql("R")
        assert f"::bit({FP_HEX_CHARS * 4})" in pg
