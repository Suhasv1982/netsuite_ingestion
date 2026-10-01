"""tools/apply_pg_grants.py: exact-match diff of the data-generator's source privileges."""

import pytest

yaml = pytest.importorskip("yaml")

from apply_pg_grants import SPEC, diff_table_privileges, grant_statements  # noqa: E402

TABLES = yaml.safe_load(SPEC.read_text(encoding="utf-8"))["tables"]


def test_spec_is_least_privilege():
    assert TABLES["netsuite.netsuite_customers"] == ["SELECT", "INSERT", "UPDATE"]
    for t, privs in TABLES.items():
        assert not {"DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"} & set(privs), t
        if t != "netsuite.netsuite_customers":
            assert privs == ["SELECT", "INSERT"], t


def test_exact_match_has_no_findings():
    assert diff_table_privileges(TABLES, {t: set(p) for t, p in TABLES.items()}) == ([], [])


def test_missing_and_extra_are_reported():
    actual = {t: set(p) for t, p in TABLES.items()}
    actual["netsuite.netsuite_memberships"] = {"SELECT", "INSERT", "DELETE", "UPDATE"}
    actual["netsuite.netsuite_customers"] = {"SELECT", "INSERT"}
    missing, extra = diff_table_privileges(TABLES, actual)
    assert missing == ["netsuite.netsuite_customers: UPDATE"]
    assert extra == ["netsuite.netsuite_memberships: DELETE", "netsuite.netsuite_memberships: UPDATE"]


def test_grant_statements_quote_the_role():
    s = grant_statements("9ae7-x", ["netsuite.netsuite_customers: UPDATE"], ["netsuite"], ["CREATE"])
    assert s == [
        'GRANT USAGE ON SCHEMA netsuite TO "9ae7-x"',
        'GRANT CREATE ON DATABASE databricks_postgres TO "9ae7-x"',
        'GRANT UPDATE ON netsuite.netsuite_customers TO "9ae7-x"',
    ]
