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
