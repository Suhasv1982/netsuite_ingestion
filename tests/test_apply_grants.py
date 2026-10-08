"""tools/apply_grants.py: grant files resolve to per-schema privileges; only missing privileges are granted."""

import pytest

yaml = pytest.importorskip("yaml")

from apply_grants import (  # noqa: E402
    GRANTS_DIR,
    desired_catalog_grants,
    desired_grants,
    desired_warehouse_permissions,
    has_level,
    missing_privileges,
)

PRINCIPALS = {"ci": "sp-1", "owner": "owner@example.com"}


@pytest.mark.parametrize("target", ["dev", "prod"])
def test_grant_files_resolve(target):
    spec = yaml.safe_load((GRANTS_DIR / f"{target}.yml").read_text(encoding="utf-8"))
    d = desired_grants(spec, PRINCIPALS)
    catalog = {"dev": "workspace", "prod": "poc_netsuite"}[target]
    assert all(s.startswith(f"{catalog}.") for s in d)
    for schema in ("poc_bronze", "poc_silver", "poc_reject", "poc_gold", "ledger", "canary"):
        assert {"USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"} <= d[f"{catalog}.{schema}"]["sp-1"]
        assert d[f"{catalog}.{schema}"]["owner@example.com"] == {"USE_SCHEMA", "SELECT"}


def test_prod_default_schema_lets_ci_create_but_not_read_or_modify():
    spec = yaml.safe_load((GRANTS_DIR / "prod.yml").read_text(encoding="utf-8"))
    assert desired_grants(spec, PRINCIPALS)["poc_netsuite.default"] == {
        "sp-1": {"USE_SCHEMA", "CREATE_TABLE", "CREATE_MATERIALIZED_VIEW"}}


def test_dev_does_not_manage_workspace_default():
    spec = yaml.safe_load((GRANTS_DIR / "dev.yml").read_text(encoding="utf-8"))
    assert "workspace.default" not in desired_grants(spec, PRINCIPALS)


def test_unresolved_principal_is_an_error():
    spec = {"catalog": "c", "grants": [{"principal": "owner", "schemas": ["s"], "privileges": ["SELECT"]}]}
    with pytest.raises(ValueError, match="owner"):
        desired_grants(spec, {"ci": "sp-1", "owner": None})


class TestMissing:
    def test_only_what_is_missing(self):
        assert missing_privileges({"a": {"SELECT", "MODIFY"}}, {"a": {"SELECT"}}) == {"a": ["MODIFY"]}

    def test_nothing_missing(self):
        assert missing_privileges({"a": {"SELECT"}}, {"a": {"SELECT", "MODIFY"}, "b": {"X"}}) == {}

    def test_all_privileges_covers_everything(self):
        assert missing_privileges({"a": {"SELECT", "MODIFY"}}, {"a": {"ALL_PRIVILEGES"}}) == {}

    def test_never_proposes_a_revoke(self):
        # extra privileges and other principals are left alone: the result only ever lists grants to add
        assert missing_privileges({"a": {"SELECT"}}, {"a": {"SELECT", "MODIFY"}, "x": {"ALL_PRIVILEGES"}}) == {}

    def test_the_owner_needs_no_grant(self):
        assert missing_privileges({"me": {"SELECT"}, "sp": {"SELECT"}}, {}, owner="me") == {"sp": ["SELECT"]}


ALL = {"ci": "sp-1", "owner": "owner@example.com", "data_generator": "gen-1", "agent": "agent-1"}


class TestPrincipalTypesAndOwnerApplied:
    def test_optional_entries_are_skipped_until_their_principal_exists(self):
        spec = yaml.safe_load((GRANTS_DIR / "dev.yml").read_text(encoding="utf-8"))
        assert "workspace.generator" not in desired_grants(spec, PRINCIPALS)   # data-generator not set: skipped
        assert desired_grants(spec, ALL)["workspace.generator"]["gen-1"] == {"USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"}

    def test_generator_grants_are_owner_applied_and_include_use_catalog(self):
        spec = yaml.safe_load((GRANTS_DIR / "dev.yml").read_text(encoding="utf-8"))
        assert "workspace.generator" not in desired_grants(spec, ALL, owner_applied=False)  # CI never grants them
        assert "workspace.generator" in desired_grants(spec, ALL, owner_applied=True)
        assert desired_catalog_grants(spec, ALL, owner_applied=True)["gen-1"] == {"USE_CATALOG"}
        assert desired_catalog_grants(spec, ALL, owner_applied=False) == {}

    def test_ci_entries_are_unchanged(self):
        spec = yaml.safe_load((GRANTS_DIR / "dev.yml").read_text(encoding="utf-8"))
        assert desired_grants(spec, ALL, owner_applied=False) == desired_grants(spec, PRINCIPALS)

    def test_unknown_principal_type_is_an_error(self):
        with pytest.raises(ValueError, match="unknown principal type"):
            desired_grants({"catalog": "c", "grants": [{"principal": "robot", "schemas": ["s"], "privileges": ["SELECT"]}]}, ALL)

    def test_warehouse_permissions(self):
        spec = {"catalog": "c", "grants": [{"principal": "agent", "warehouses": [{"name": "W", "level": "CAN_USE"}]}]}
        assert desired_warehouse_permissions(spec, ALL) == {"W": {"agent-1": "CAN_USE"}}

    def test_warehouse_levels_include_lower_ones(self):
        assert has_level({"CAN_MANAGE"}, "CAN_USE") and has_level({"CAN_USE"}, "CAN_USE")
        assert not has_level({"CAN_VIEW"}, "CAN_USE") and not has_level(set(), "CAN_USE")
