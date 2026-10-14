"""tools/sp_roles.py: the Service Principal User rule set is only added to, and checked for an exact match."""

import sp_roles

MANAGER = {"role": "roles/servicePrincipal.manager", "principals": ["users/owner@example.com"]}


def test_spec_lists_ci_dev_on_both_run_as_principals():
    assert sp_roles.load_spec() == {"data-generator": ["ci-dev"], "dq-agent": ["ci-dev"]}


def test_merge_adds_the_user_rule_and_keeps_everything_else():
    rules = sp_roles.merged_rules([MANAGER], ["servicePrincipals/ci"])
    assert rules == [MANAGER, {"role": sp_roles.USER_ROLE, "principals": ["servicePrincipals/ci"]}]


def test_merge_adds_to_an_existing_user_rule_without_removing_or_duplicating():
    existing = [MANAGER, {"role": sp_roles.USER_ROLE, "principals": ["servicePrincipals/other"]}]
    rules = sp_roles.merged_rules(existing, ["servicePrincipals/ci", "servicePrincipals/other"])
    assert rules[1]["principals"] == ["servicePrincipals/other", "servicePrincipals/ci"]
    assert existing[1]["principals"] == ["servicePrincipals/other"]      # the input is not mutated


def test_check_is_an_exact_match():
    assert sp_roles.check([MANAGER], ["servicePrincipals/ci"]) == (["servicePrincipals/ci"], [])
    held = [MANAGER, {"role": sp_roles.USER_ROLE, "principals": ["servicePrincipals/ci", "users/x@example.com"]}]
    assert sp_roles.check(held, ["servicePrincipals/ci"]) == ([], ["users/x@example.com"])
    # the manager role is not the user role: it is neither required nor counted as extra
    assert sp_roles.check([MANAGER, {"role": sp_roles.USER_ROLE, "principals": ["servicePrincipals/ci"]}],
                          ["servicePrincipals/ci"]) == ([], [])


def test_rule_set_name():
    assert sp_roles.rule_set_name("acc", "app") == "accounts/acc/servicePrincipals/app/ruleSets/default"
