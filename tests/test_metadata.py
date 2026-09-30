"""Unit tests for the pure builder functions in metadata.py.

No Spark session required -- build_column_list / build_dq_predicate /
build_dq_reason_expr take plain dicts (shaped like source_columns /
data_quality_rules rows) and return plain Python values.

metadata.py is imported as a flat sibling module (not a package) because
that's how the deployed pipeline imports it too -- see pyproject.toml's
pytest pythonpath and the transformation files' `from metadata import ...`.
"""

import pytest

from metadata import (
    bronze_table_name,
    build_column_list,
    build_dq_predicate,
    build_dq_reason_expr,
    _blank_to_none,
    build_soft_expectations,
    filter_active_rules,
    has_merge_keys,
)


class TestBuildColumnList:
    def test_orders_by_ordinal(self):
        cols = [
            {"column_name": "b", "ordinal": 2, "is_active": True},
            {"column_name": "a", "ordinal": 1, "is_active": True},
        ]
        assert build_column_list(cols) == ["a", "b"]

    def test_skips_inactive_columns(self):
        cols = [
            {"column_name": "a", "ordinal": 1, "is_active": True},
            {"column_name": "b", "ordinal": 2, "is_active": False},
        ]
        assert build_column_list(cols) == ["a"]

    def test_null_is_active_treated_as_active(self):
        cols = [{"column_name": "a", "ordinal": 1, "is_active": None}]
        assert build_column_list(cols) == ["a"]

    def test_null_ordinal_sorts_last(self):
        cols = [
            {"column_name": "a", "ordinal": None, "is_active": True},
            {"column_name": "b", "ordinal": 1, "is_active": True},
        ]
        assert build_column_list(cols) == ["b", "a"]

    def test_empty_columns_raises(self):
        with pytest.raises(ValueError):
            build_column_list([])

    def test_all_inactive_raises(self):
        cols = [{"column_name": "a", "ordinal": 1, "is_active": False}]
        with pytest.raises(ValueError):
            build_column_list(cols)

    def test_matches_netsuite_memberships_shape(self):
        # Mirrors the real source_columns rows for table_id=2 (netsuite_memberships).
        cols = [
            {"column_name": "membership_internal_id", "ordinal": 1, "is_active": True},
            {"column_name": "customer_internal_id", "ordinal": 2, "is_active": True},
            {"column_name": "membership_type", "ordinal": 3, "is_active": True},
            {"column_name": "start_date", "ordinal": 4, "is_active": True},
            {"column_name": "end_date", "ordinal": 5, "is_active": True},
            {"column_name": "membership_status", "ordinal": 6, "is_active": True},
            {"column_name": "created_date", "ordinal": 7, "is_active": True},
            {"column_name": "updated_date", "ordinal": 8, "is_active": True},
        ]
        assert build_column_list(cols) == [
            "membership_internal_id",
            "customer_internal_id",
            "membership_type",
            "start_date",
            "end_date",
            "membership_status",
            "created_date",
            "updated_date",
        ]


class TestBuildDqPredicate:
    def test_no_rules_returns_true_literal(self):
        assert build_dq_predicate([]) == "true"

    def test_ignores_non_hard_rules(self):
        rules = [{"severity": "SOFT", "rule_expr": "amount > 0"}]
        assert build_dq_predicate(rules) == "true"

    def test_single_hard_rule(self):
        rules = [{"severity": "HARD", "rule_expr": "amount > 0"}]
        assert build_dq_predicate(rules) == "(amount > 0)"

    def test_multiple_hard_rules_anded(self):
        rules = [
            {"severity": "HARD", "rule_expr": "amount > 0"},
            {"severity": "HARD", "rule_expr": "status IS NOT NULL"},
        ]
        assert build_dq_predicate(rules) == "(amount > 0) AND (status IS NOT NULL)"

    def test_mixed_severity_only_hard_included(self):
        rules = [
            {"severity": "HARD", "rule_expr": "amount > 0"},
            {"severity": "SOFT", "rule_expr": "note IS NOT NULL"},
        ]
        assert build_dq_predicate(rules) == "(amount > 0)"

    def test_blank_rule_expr_is_skipped(self):
        rules = [
            {"severity": "HARD", "rule_expr": ""},
            {"severity": "HARD", "rule_expr": "amount > 0"},
        ]
        assert build_dq_predicate(rules) == "(amount > 0)"

    def test_matches_real_certification_rules(self):
        # Mirrors the current HARD rules for table_id=3 (netsuite_certifications).
        rules = [
            {"severity": "HARD", "rule_expr": "certification_type in ('SCP','CP')"},
            {"severity": "HARD", "rule_expr": "certification_start_date < DATE '2027-01-01'"},
        ]
        assert build_dq_predicate(rules) == (
            "(certification_type in ('SCP','CP')) AND "
            "(certification_start_date < DATE '2027-01-01')"
        )


class TestBuildDqReasonExpr:
    def test_no_rules_returns_null_literal(self):
        assert build_dq_reason_expr([]) == "NULL"

    def test_single_hard_rule_produces_case_expr(self):
        rules = [{"severity": "HARD", "rule_name": "Positive Amount", "rule_expr": "amount > 0"}]
        expr = build_dq_reason_expr(rules)
        assert "CASE WHEN NOT (amount > 0) THEN 'Positive Amount' END" in expr
        assert expr.startswith("array_join(array_compact(array(")

    def test_ignores_non_hard_rules(self):
        rules = [{"severity": "SOFT", "rule_name": "Soft Rule", "rule_expr": "amount > 0"}]
        assert build_dq_reason_expr(rules) == "NULL"


class TestBuildSoftExpectations:
    def test_no_rules_returns_empty(self):
        assert build_soft_expectations([]) == {}

    def test_only_soft_rules_included(self):
        rules = [
            {"severity": "SOFT", "rule_name": "Has Email", "rule_expr": " email IS NOT NULL "},
            {"severity": "HARD", "rule_name": "Positive Amount", "rule_expr": "amount > 0"},
        ]
        assert build_soft_expectations(rules) == {"Has Email": "email IS NOT NULL"}

    def test_blank_rule_expr_is_skipped(self):
        rules = [{"severity": "SOFT", "rule_name": "Blank", "rule_expr": "  "}]
        assert build_soft_expectations(rules) == {}

    def test_duplicate_names_are_kept_apart_by_rule_id(self):
        rules = [
            {"rule_id": 7, "severity": "SOFT", "rule_name": "Range", "rule_expr": "a > 0"},
            {"rule_id": 9, "severity": "SOFT", "rule_name": "Range", "rule_expr": "b > 0"},
            {"rule_id": 11, "severity": "SOFT", "rule_name": "Other", "rule_expr": "c > 0"},
        ]
        assert build_soft_expectations(rules) == {"Range (rule 7)": "a > 0", "Range (rule 9)": "b > 0", "Other": "c > 0"}


class TestBlankToNone:
    def test_none_becomes_none(self):
        assert _blank_to_none(None) is None

    def test_empty_and_whitespace_become_none(self):
        assert _blank_to_none("") is None
        assert _blank_to_none("  \t\r\n") is None

    def test_value_is_stripped(self):
        assert _blank_to_none("updated_date\r\n") == "updated_date"


class TestHasMergeKeys:
    def test_both_keys_present(self):
        assert has_merge_keys({"business_key": "id", "watermark_col": "updated_date"}) is True

    def test_matches_netsuite_customers_shape(self):
        # netsuite_customers (table_id=1): business_key set, watermark_col
        # is an empty string in source_table_def -- no Silver table for it.
        assert has_merge_keys({"business_key": "customer_internal_id", "watermark_col": ""}) is False

    def test_missing_business_key(self):
        assert has_merge_keys({"business_key": None, "watermark_col": "updated_date"}) is False

    def test_missing_watermark_col(self):
        assert has_merge_keys({"business_key": "id", "watermark_col": None}) is False

    def test_missing_both(self):
        assert has_merge_keys({}) is False


class TestBronzeTableName:
    def test_one_bronze_table_per_source(self):
        assert bronze_table_name("netsuite_customers") == "poc_bronze.netsuite_customers"
        assert bronze_table_name("netsuite_memberships") == "poc_bronze.netsuite_memberships"


class TestFilterActiveRules:
    def test_inactive_rule_is_dropped(self):
        rules = [
            {"rule_name": "on", "is_active": True},
            {"rule_name": "off", "is_active": False},
        ]
        assert [r["rule_name"] for r in filter_active_rules(rules)] == ["on"]

    def test_missing_or_null_is_active_counts_as_active(self):
        # metadata database without migration 001: the column does not exist yet
        rules = [{"rule_name": "old"}, {"rule_name": "null", "is_active": None}]
        assert len(filter_active_rules(rules)) == 2

    def test_inactive_rules_do_not_reach_any_builder(self):
        rules = filter_active_rules([
            {"severity": "HARD", "rule_name": "H", "rule_expr": "a > 0", "is_active": False},
            {"severity": "SOFT", "rule_name": "S", "rule_expr": "b > 0", "is_active": False},
            {"severity": "HARD", "rule_name": "K", "rule_expr": "c > 0", "is_active": True},
        ])
        assert build_dq_predicate(rules) == "(c > 0)"
        assert build_soft_expectations(rules) == {}
        assert "'H'" not in build_dq_reason_expr(rules)


class TestPgConnFromConf:
    """pg_conn_from_conf picks the secret key from <prefix>_pg_token_key, defaulting to <prefix>_pg_token."""

    class _Conf:
        def __init__(self, values):
            self.values = values

        def get(self, key, default=None):
            return self.values.get(key, default)

    class _Spark:
        def __init__(self, values):
            self.conf = TestPgConnFromConf._Conf(values)

    class _Secrets:
        def __init__(self):
            self.asked = None

        def get(self, scope, key):
            self.asked = (scope, key)
            return "tok"

    class _Dbutils:
        def __init__(self):
            self.secrets = TestPgConnFromConf._Secrets()

    def test_default_key_when_no_override(self):
        from metadata import pg_conn_from_conf

        dbu = self._Dbutils()
        conn = pg_conn_from_conf(self._Spark({"meta_pg_host": "h", "meta_pg_user": "u"}), dbu, "meta")
        assert dbu.secrets.asked == ("netsuite_ingestion_poc", "meta_pg_token")
        assert (conn.host, conn.user, conn.token) == ("h", "u", "tok")

    def test_override_key_for_dev(self):
        from metadata import pg_conn_from_conf

        dbu = self._Dbutils()
        conf = {"meta_pg_host": "h", "meta_pg_user": "u", "meta_pg_token_key": "meta_pg_token_dev"}
        pg_conn_from_conf(self._Spark(conf), dbu, "meta")
        assert dbu.secrets.asked == ("netsuite_ingestion_poc", "meta_pg_token_dev")

    def test_scope_comes_from_the_pipeline_configuration(self):
        from metadata import pg_conn_from_conf

        dbu = self._Dbutils()
        conf = {"meta_pg_host": "h", "meta_pg_user": "u", "secret_scope": "netsuite_ingestion_dev"}
        pg_conn_from_conf(self._Spark(conf), dbu, "meta")
        assert dbu.secrets.asked == ("netsuite_ingestion_dev", "meta_pg_token")

    def test_explicit_scope_wins(self):
        from metadata import pg_conn_from_conf

        dbu = self._Dbutils()
        pg_conn_from_conf(self._Spark({"meta_pg_host": "h", "meta_pg_user": "u", "secret_scope": "x"}), dbu, "meta", "y")
        assert dbu.secrets.asked == ("y", "meta_pg_token")
