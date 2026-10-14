"""Service principal roles (grants/dev.yml, `service_principal_users`): who may act as a service principal.

    python tools/sp_roles.py --check --profile DEFAULT     # read-only: exact match or exit 1
    python tools/sp_roles.py --apply --profile DEFAULT     # the OWNER (via tools/setup_dq_roles.sh)

"Service Principal User" (roles/servicePrincipal.user) on a service principal lets the holder deploy a job that
runs as it (`run_as`). ci-dev needs it on dq-agent to deploy the DQ recommender job (plan #39 step 10), as it has
it on data-generator. The rule set is read and written through the workspace's access-control proxy API, with its
etag, so a concurrent change is refused rather than overwritten. --apply only ADDS the listed principals: other
rules and other principals are kept. --check fails when a listed holder is missing, or when anyone else holds the
user role on a listed service principal (exact match, like tools/dq_roles.py).

Principals are resolved by display name at runtime (`ci-dev`, `dq-agent`); no ids are stored. The account id comes
from the CLI profile's configuration.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

import yaml

ROOT = Path(__file__).resolve().parent.parent
USER_ROLE = "roles/servicePrincipal.user"
API = "/api/2.0/preview/accounts/access-control/rule-sets"


# -- pure -------------------------------------------------------------------


def load_spec(path: Path = ROOT / "grants" / "dev.yml") -> dict[str, list[str]]:
    """service principal display name -> display names that must hold the user role on it."""
    return yaml.safe_load(path.read_text(encoding="utf-8")).get("service_principal_users") or {}


def merged_rules(grant_rules: list[dict], holders: list[str]) -> list[dict]:
    """The rule set with `holders` (as "servicePrincipals/<application id>") added to the user role. Every other
    rule and principal is kept; nothing is removed."""
    rules = [dict(r, principals=list(r.get("principals", []))) for r in grant_rules]
    user = next((r for r in rules if r.get("role") == USER_ROLE), None)
    if user is None:
        user = {"role": USER_ROLE, "principals": []}
        rules.append(user)
    for h in holders:
        if h not in user["principals"]:
            user["principals"].append(h)
    return rules


def check(grant_rules: list[dict], holders: list[str]) -> tuple[list[str], list[str]]:
    """(missing, extra) holders of the user role."""
    have = set(next((r.get("principals", []) for r in grant_rules if r.get("role") == USER_ROLE), []))
    return sorted(set(holders) - have), sorted(have - set(holders))


# -- I/O --------------------------------------------------------------------


def _cli(profile, *args, body=None):
    cmd = ["databricks", *args, "-o", "json"] + (["--profile", profile] if profile else [])
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "MSYS_NO_PATHCONV": "1"})
    if p.returncode:
        raise RuntimeError(f"databricks {' '.join(args[:3])}: {p.stderr.strip()[:300]}")
    return json.loads(p.stdout) if p.stdout.strip() else {}


def account_id(profile) -> str:
    d = _cli(profile, "auth", "describe")
    acc = (((d.get("details") or {}).get("configuration") or {}).get("account_id") or {}).get("value")
    if not acc:
        raise RuntimeError("the CLI profile has no account_id; add it to the profile")
    return acc


def app_ids(profile) -> dict[str, str]:
    return {s["displayName"]: s["applicationId"] for s in _cli(profile, "service-principals", "list")
            if s.get("displayName") and s.get("applicationId")}


def rule_set_name(account: str, sp_app_id: str) -> str:
    return f"accounts/{account}/servicePrincipals/{sp_app_id}/ruleSets/default"


def get_rule_set(profile, name: str) -> dict:
    return _cli(profile, "api", "get", f"{API}?name={quote(name, safe='/')}&etag=")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    ap.add_argument("--profile", required=True)
    args = ap.parse_args(argv)

    spec, ids, acc = load_spec(), app_ids(args.profile), account_id(args.profile)
    failed = False
    for sp, holder_names in spec.items():
        unknown = [n for n in [sp, *holder_names] if n not in ids]
        if unknown:
            print(f"FAIL {sp}: service principal(s) not found: {unknown}")
            failed = True
            continue
        holders = [f"servicePrincipals/{ids[n]}" for n in holder_names]
        name = rule_set_name(acc, ids[sp])
        rs = get_rule_set(args.profile, name)
        if args.apply:
            missing, _ = check(rs.get("grant_rules", []), holders)
            if missing:
                _cli(args.profile, "api", "put", API, body={"name": name, "rule_set": {
                    "name": name, "etag": rs["etag"], "grant_rules": merged_rules(rs.get("grant_rules", []), holders)}})
                print(f"granted Service Principal User on {sp} to {', '.join(holder_names)}")
            else:
                print(f"Service Principal User on {sp}: already held by {', '.join(holder_names)}")
            rs = get_rule_set(args.profile, name)
        missing, extra = check(rs.get("grant_rules", []), holders)
        names = {f"servicePrincipals/{v}": k for k, v in ids.items()}
        if missing or extra:
            failed = True
            print(f"FAIL Service Principal User on {sp}: missing {[names.get(m, m) for m in missing]}, "
                  f"extra {[names.get(e, e.split('/')[0] + '/...') for e in extra]}")
        else:
            print(f"OK Service Principal User on {sp}: exactly {', '.join(holder_names)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
