"""tools/check_soft_expectations.py: which SOFT rules must appear, how event rows are read, what counts as present."""

import pytest

from check_soft_expectations import collect_expectations, compare, event_log_sql, expected_rules

TABLE_DEFS = [{"table_id": 2, "source_table": "netsuite_memberships"}, {"table_id": 5, "source_table": "netsuite_transaction_lines"}]
RULES = [
    {"rule_id": 1, "table_id": 2, "rule_name": "Not in List", "rule_expr": "x in ('a')", "severity": "HARD", "is_active": True},
    {"rule_id": 4, "table_id": 5, "rule_name": "Amount Equals Qty x Rate", "rule_expr": "amount = quantity * rate", "severity": "SOFT", "is_active": True},
    {"rule_id": 5, "table_id": 2, "rule_name": "End Not Before Start", "rule_expr": "end_date >= start_date", "severity": "SOFT", "is_active": True},
    {"rule_id": 6, "table_id": 2, "rule_name": "Retired", "rule_expr": "1 = 1", "severity": "SOFT", "is_active": False},
]
EXPECTED = [("netsuite_memberships", "End Not Before Start"), ("netsuite_transaction_lines", "Amount Equals Qty x Rate")]


def test_expected_rules_are_the_active_soft_ones():
    assert expected_rules(TABLE_DEFS, RULES) == EXPECTED


def _row(flow, *exps):
    import json

    return {"update_id": "u", "flow_name": flow, "expectations": json.dumps(list(exps))}


def _exp(name, dataset, passed, failed):
    return {"name": name, "dataset": dataset, "passed_records": passed, "failed_records": failed}


def test_counts_are_summed_per_name_dataset_and_flow():
    rows = [
        _row("poc_silver.netsuite_transaction_lines", _exp("Amount Equals Qty x Rate", "netsuite_transaction_lines_valid", 10, 1)),
        _row("poc_silver.netsuite_transaction_lines", _exp("Amount Equals Qty x Rate", "netsuite_transaction_lines_valid", 5, 0)),
    ]
    assert collect_expectations(rows) == {
        "Amount Equals Qty x Rate": [
            {"dataset": "netsuite_transaction_lines_valid", "flow": "poc_silver.netsuite_transaction_lines", "passed": 15, "failed": 1}
        ]
    }


def test_blank_or_null_expectations_are_ignored():
    assert collect_expectations([{"flow_name": "f", "expectations": None}, {"flow_name": "f", "expectations": ""}]) == {}


def test_present_and_missing():
    found = collect_expectations([_row("poc_silver.netsuite_transaction_lines", _exp("Amount Equals Qty x Rate", "netsuite_transaction_lines_valid", 3, 2))])
    present, missing = compare(EXPECTED, found)
    assert [(t, n) for t, n, _ in present] == [("netsuite_transaction_lines", "Amount Equals Qty x Rate")]
    assert missing == [("netsuite_memberships", "End Not Before Start")]


def test_a_same_named_expectation_of_another_table_does_not_count():
    found = collect_expectations([_row("poc_silver.netsuite_certifications", _exp("End Not Before Start", "netsuite_certifications_valid", 1, 0))])
    assert compare(EXPECTED[:1], found) == ([], EXPECTED[:1])


class TestEventLogSql:
    PID, UID = "2b815392-6b06-4271-9579-8f5ab2b19fc6", "763476c9-4a6b-4586-81be-fe94fce4d72d"

    def test_latest_update_by_default(self):
        sql = event_log_sql(self.PID)
        assert f"event_log('{self.PID}')" in sql and "event_type = 'create_update'" in sql and "flow_progress" in sql

    def test_explicit_update(self):
        assert f"origin.update_id = '{self.UID}'" in event_log_sql(self.PID, self.UID)

    def test_rejects_anything_but_ids(self):
        with pytest.raises(ValueError):
            event_log_sql("x'); DROP TABLE t; --")
