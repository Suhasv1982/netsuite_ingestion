"""tools/dq_roles.py: the DQ recommender's Postgres group roles from grants/dev.yml (pure parts)."""

import pytest

import dq_roles as dr

P = {"agent": "agent-app", "ci": "ci-app", "owner": "owner@example.com"}


@pytest.fixture(scope="module")
def spec():
    return dr.load_spec()


def test_the_roles_and_their_members(spec):
    assert set(spec) == {"aidq_agent", "aidq_validator", "aidq_reviewer", "aidq_llm_logger"}
    assert {r: s["members"] for r, s in spec.items()} == {
        "aidq_agent": ["agent"], "aidq_validator": ["ci"], "aidq_reviewer": ["owner"],
        "aidq_llm_logger": ["ci", "agent"]}


def test_the_llm_logger_can_only_add_rows_and_prune(spec):
    sql = [s for s in dr.role_sql(spec, P) if '"aidq_llm_logger"' in s and "GRANT" in s and " TO " in s]
    assert 'GRANT INSERT ON aidq_metadata."llm_calls" TO "aidq_llm_logger"' in sql
    assert 'GRANT EXECUTE ON FUNCTION aidq_metadata.prune_llm_calls(integer) TO "aidq_llm_logger"' in sql
    assert not any("SELECT" in s or "UPDATE" in s or "DELETE" in s for s in sql)


def test_the_agent_can_only_read_and_insert(spec):
    sql = [s for s in dr.role_sql(spec, P) if '"aidq_agent"' in s]
    assert not any("UPDATE" in s or "DELETE" in s or "EXECUTE" in s for s in sql)
    assert 'GRANT INSERT ON aidq_metadata."config_proposals" TO "aidq_agent"' in sql
    assert 'GRANT INSERT ON aidq_metadata."incidents" TO "aidq_agent"' in sql


def test_validator_and_reviewer_columns(spec):
    sql = dr.role_sql(spec, P)
    assert ('GRANT UPDATE ("status", "validation_result", "requires_rebuild") ON aidq_metadata."config_proposals" '
            'TO "aidq_validator"') in sql
    assert any('"approved_severity"' in s and '"aidq_reviewer"' in s for s in sql)
    assert not any('"approved_severity"' in s and '"aidq_validator"' in s for s in sql)
    assert 'GRANT EXECUTE ON FUNCTION aidq_metadata.apply_proposal(bigint, text) TO "aidq_reviewer"' in sql


def test_roles_are_nologin_and_idempotent(spec):
    creates = [s for s in dr.role_sql(spec, P) if "CREATE ROLE" in s]
    assert len(creates) == 4 and all("NOLOGIN" in s and "IF NOT EXISTS" in s for s in creates)


def test_nothing_is_granted_aidq_owner(spec):
    assert not any("aidq_owner" in s for s in dr.role_sql(spec, P))


def test_a_missing_principal_is_an_error(spec):
    with pytest.raises(ValueError, match="agent"):
        dr.role_sql(spec, {**P, "agent": None})


def _actual(spec, **over):
    a = {"roles": set(spec), "members": {"aidq_agent": {"agent-app"}, "aidq_validator": {"ci-app"},
                                         "aidq_reviewer": {"owner@example.com"}, "aidq_llm_logger": {"ci-app", "agent-app"}},
         "owner_members": {"ci-app", "owner@example.com"},
         "table_privs": {"aidq_agent": {("config_proposals", "INSERT"), ("incidents", "INSERT"), ("x", "SELECT")},
                         "aidq_llm_logger": {("llm_calls", "INSERT")}},
         "select_complete": {r: True for r in spec},
         "column_privs": {"aidq_validator": {("config_proposals", "status", "UPDATE"),
                                            ("config_proposals", "validation_result", "UPDATE"),
                                            ("config_proposals", "requires_rebuild", "UPDATE")},
                          "aidq_reviewer": {("config_proposals", c, "UPDATE") for c in
                                            ("status", "reviewed_by", "reviewed_at", "review_comment", "approved_severity")}},
         "exec": {"aidq_reviewer": {"apply_proposal"}, "aidq_llm_logger": {"prune_llm_calls"}}}
    a.update(over)
    return a


class TestDiff:
    def test_exact_match(self, spec):
        assert dr.diff(spec, P, _actual(spec)) == ([], [])

    def test_ci_and_owner_in_aidq_owner_are_fine_but_the_agent_is_not(self, spec):
        _, extra = dr.diff(spec, P, _actual(spec, owner_members={"ci-app", "agent-app"}))
        assert extra == ["agent-app is a member of aidq_owner"]
        _, extra = dr.diff(spec, P, _actual(spec, owner_members={"aidq_agent"}))
        assert extra == ["aidq_agent is a member of aidq_owner"]

    def test_an_update_right_for_the_agent_is_extra(self, spec):
        a = _actual(spec)
        a["table_privs"]["aidq_agent"].add(("config_proposals", "UPDATE"))
        assert "aidq_agent: UPDATE on config_proposals" in dr.diff(spec, P, a)[1]

    def test_missing_role_and_column(self, spec):
        a = _actual(spec, roles={"aidq_agent", "aidq_validator"})
        a["column_privs"]["aidq_validator"].discard(("config_proposals", "validation_result", "UPDATE"))
        missing, _ = dr.diff(spec, P, a)
        assert "role aidq_reviewer" in missing and "aidq_validator: UPDATE(validation_result) on config_proposals" in missing
