"""aidq_mcp.dq_tools: the profiler's SQL templates and the candidate checklist (pure; no warehouse)."""

from aidq_mcp import dq_tools as dq

COLS = [{"column_name": "customer_internal_id", "data_type": "bigint"}, {"column_name": "email", "data_type": "text"},
        {"column_name": "company_name", "data_type": "text"}, {"column_name": "start_date", "data_type": "date"},
        {"column_name": "end_date", "data_type": "date"}]


def test_profile_sql_counts_bad_email_formats_without_returning_values():
    sql = dq.profile_sql("workspace.poc_bronze.t", COLS, "customer_internal_id")
    assert f"NOT `email` rlike '{dq.EMAIL_PATTERN}') AS `email__bad_format`" in sql
    assert "`company_name__bad_format`" not in sql            # only columns named like an email
    assert "\\" not in dq.EMAIL_PATTERN                         # no escapes to get wrong in SQL or JSON


def test_shape_profile_gives_a_valid_format_rate():
    agg = {"rows": 1000, "email__nulls": 10, "email__bad_format": 4, "email__distinct": 990}
    [email] = [c for c in dq.shape_profile(agg, COLS[1:2], None)["columns"]]
    assert email["bad_format_count"] == 4 and email["valid_format_rate"] == 0.986


def test_late_rows_are_new_keys_loaded_after_their_day():
    sql = dq.late_rows_sql("workspace.poc_bronze.netsuite_memberships", "membership_internal_id")
    assert "min(d) AS fd" in sql and "b.d = s.fd" in sql
    assert f"b.t > f.ft + INTERVAL {dq.LATE_LOAD_HOURS} HOURS" in sql


def test_rare_values_flag_blanks_variants_and_rare_values_only():
    top = [{"value": "Cash Sale", "count": 19853}, {"value": "Invoice", "count": 13149}, {"value": "invoice", "count": 103},
           {"value": "N/A", "count": 73}, {"value": "", "count": 52}, {"value": "<masked>", "count": 3},
           {"value": None, "count": 2}]
    rare = dq.rare_values(top, 33285)
    assert [(r["value"], r["why"]) for r in rare] == [
        ("invoice", "variant of 'Invoice'"), ("N/A", "under 1% of rows"), ("", "blank")]


PROFILE = {
    "rows": 10000, "business_key": "membership_internal_id",
    "columns": [{"column": "membership_internal_id", "null_rate": 0.0025},
                {"column": "end_date", "future_count": 4000, "null_rate": 0.0},
                {"column": "start_date", "future_count": 0, "null_rate": 0.0},
                {"column": "membership_status", "null_rate": 0.0,
                 "rare_values": [{"value": "NA", "count": 17, "why": "under 1% of rows"}]},
                {"column": "notes", "null_rate": 0.4},
                {"column": "company_name", "null_rate": 0.0009}],
    "candidate_checks": {"end_date_before_start_date": 78, "null_business_key": 25,
                         "keys_with_conflicting_created_date_same_day": 24, "amount_ne_quantity_x_rate": 0},
    "references_without_target": {"customer_internal_id -> netsuite_customers": 12},
    "late_arrivals": {"late_rows": 19, "late_dates": 14},
}


def test_every_check_with_violations_becomes_a_candidate():
    c = {x["check"]: x for x in dq.candidates("netsuite_memberships", PROFILE)}
    assert set(c) == {"end_date_before_start_date", "null_business_key", "duplicate_business_key", "end_date_future",
                      "membership_status_rare_values", "company_name_rare_nulls", "orphan:customer_internal_id",
                      "late_rows"}
    assert c["end_date_before_start_date"] == {
        "id": "netsuite_memberships:end_date_before_start_date", "check": "end_date_before_start_date", "count": 78,
        "rate": 0.0078, "kind": "row_rule", "columns": ["end_date", "start_date"]}
    for nx in ("duplicate_business_key", "orphan:customer_internal_id", "late_rows"):
        assert c[nx]["kind"] == "not_expressible"
    assert c["membership_status_rare_values"]["values"][0]["value"] == "NA"
    assert c["company_name_rare_nulls"]["count"] == 9
    # zero counts, a key column's nulls (covered by null_business_key) and common nulls are not candidates
    assert "amount_ne_quantity_x_rate" not in c and "membership_internal_id_rare_nulls" not in c
    assert "notes_rare_nulls" not in c and "start_date_future" not in c


def test_a_clean_table_has_no_candidates():
    assert dq.candidates("t", {"rows": 5, "columns": [{"column": "a", "null_rate": 0.0}], "candidate_checks": {}}) == []
