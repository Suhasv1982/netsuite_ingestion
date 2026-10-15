"""tools/apply_pg_grants.py: exact-match diff of the data-generator's source privileges."""

import pytest

yaml = pytest.importorskip("yaml")

from apply_pg_grants import diff_table_privileges, grant_statements, load_roles  # noqa: E402

ROLES = load_roles()
TABLES = ROLES["data_generator"]["tables"]


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


def test_dq_agent_reads_the_five_source_tables_and_nothing_else():
    dq = ROLES["dq_agent"]
    assert dq["role_variable"] == "DQ_AGENT_SP" and dq["schema_usage"] == ["netsuite"] and dq["database"] == ["CONNECT"]
    assert set(dq["tables"]) == set(TABLES)                      # the same five tables as the generator
    assert all(privs == ["SELECT"] for privs in dq["tables"].values())


def test_a_role_without_an_id_is_skipped_not_failed(monkeypatch, capsys):
    import apply_pg_grants as g

    monkeypatch.delenv("DATA_GENERATOR_SP", raising=False)
    monkeypatch.delenv("DQ_AGENT_SP", raising=False)
    monkeypatch.setattr(g, "connect", lambda profile: (_ for _ in ()).throw(AssertionError("no connection expected")))
    assert g.main(["--check"]) == 0
    out = capsys.readouterr().out
    assert "SKIPPED  data_generator: DATA_GENERATOR_SP is not set here" in out and "SKIPPED  dq_agent" in out
    assert g.main(["--check", "--role", "x"]) == 1                # --role needs --only
