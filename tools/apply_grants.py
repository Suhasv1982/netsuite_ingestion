"""Apply the Unity Catalog grants a target needs (grants/<target>.yml). Additive only: grants what is missing,
never revokes anything, so it cannot take access away from anyone.

    python tools/apply_grants.py --target dev --check                      # print what is missing, change nothing
    python tools/apply_grants.py --target dev                              # grant what is missing (CI)
    python tools/apply_grants.py --target dev --owner-applied --profile DEFAULT   # the owner: entries CI cannot grant

Principals in the file: `ci` = the identity running this (DATABRICKS_CLIENT_ID in CI, or --ci-principal),
`owner` = GRANT_OWNER_PRINCIPAL (GitHub Actions variable) or --owner-principal, `data_generator` =
DATA_GENERATOR_SP or --data-generator-principal, `agent` = DQ_AGENT_SP or --agent-principal (the dq-agent
service principal). An entry marked `optional: true` is skipped while its principal is not set (e.g. before the
service principal exists).

An entry may grant schema privileges (`schemas` + `privileges`), catalog privileges (`catalog_privileges`) and SQL
warehouse permissions (`warehouses: [{name, level}]`). An entry marked `applied_by: owner` holds grants the CI
identity cannot make (catalog privileges, schemas it neither owns nor manages): CI only reports them (never a
failure); the owner applies them with --owner-applied. Uses the grants and permissions APIs, no SQL warehouse.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

GRANTS_DIR = Path(__file__).resolve().parent.parent / "grants"
PRINCIPAL_ENV = {"ci": "DATABRICKS_CLIENT_ID", "owner": "GRANT_OWNER_PRINCIPAL",
                 "data_generator": "DATA_GENERATOR_SP", "agent": "DQ_AGENT_SP"}


# -- pure -------------------------------------------------------------------


def _entries(spec: dict, principals: dict[str, str], owner_applied: bool | None):
    """(entry, principal id) for every entry whose principal is resolved. owner_applied: None = all entries,
    True/False = only entries with / without `applied_by: owner`."""
    for g in spec["grants"]:
        if owner_applied is not None and (g.get("applied_by") == "owner") != owner_applied:
            continue
        role = g["principal"]
        if role not in PRINCIPAL_ENV:
            raise ValueError(f"unknown principal type {role!r} (one of {', '.join(PRINCIPAL_ENV)})")
        if not principals.get(role):
            if g.get("optional"):
                continue
            raise ValueError(f"principal {role!r} is not resolved (set it with --{role.replace('_', '-')}-principal "
                             f"or {PRINCIPAL_ENV[role]})")
        yield g, principals[role]


def desired_grants(spec: dict, principals: dict[str, str], owner_applied: bool | None = None) -> dict[str, dict[str, set[str]]]:
    """'<catalog>.<schema>' -> principal -> privileges, from a grants file and the resolved principals."""
    out: dict[str, dict[str, set[str]]] = {}
    for g, who in _entries(spec, principals, owner_applied):
        for schema in g.get("schemas", []):
            out.setdefault(f"{spec['catalog']}.{schema}", {}).setdefault(who, set()).update(g["privileges"])
    return out


def desired_catalog_grants(spec: dict, principals: dict[str, str], owner_applied: bool | None = None) -> dict[str, set[str]]:
    """principal -> privileges on the catalog itself."""
    out: dict[str, set[str]] = {}
    for g, who in _entries(spec, principals, owner_applied):
        if g.get("catalog_privileges"):
            out.setdefault(who, set()).update(g["catalog_privileges"])
    return out


def desired_warehouse_permissions(spec: dict, principals: dict[str, str], owner_applied: bool | None = None) -> dict[str, dict[str, str]]:
    """warehouse name -> principal -> permission level."""
    out: dict[str, dict[str, str]] = {}
    for g, who in _entries(spec, principals, owner_applied):
        for w in g.get("warehouses", []):
            out.setdefault(w["name"], {})[who] = w["level"]
    return out


def missing_privileges(desired: dict[str, set[str]], current: dict[str, set[str]], owner: str | None = None) -> dict[str, list[str]]:
    """principal -> privileges still to grant. The securable's owner, and ALL_PRIVILEGES, cover everything."""
    out = {}
    for principal, privs in desired.items():
        if principal == owner:
            continue
        have = current.get(principal, set())
        need = set() if "ALL_PRIVILEGES" in have else privs - have
        if need:
            out[principal] = sorted(need)
    return out


WAREHOUSE_LEVELS = ["CAN_VIEW", "CAN_MONITOR", "CAN_USE", "CAN_MANAGE", "IS_OWNER"]


def has_level(have: set[str], want: str) -> bool:
    """A warehouse permission level includes the lower ones."""
    return any(WAREHOUSE_LEVELS.index(h) >= WAREHOUSE_LEVELS.index(want) for h in have if h in WAREHOUSE_LEVELS)


# -- I/O --------------------------------------------------------------------


def _cli(profile, *args, body=None):
    cmd = ["databricks", *args, "-o", "json"] + (["--profile", profile] if profile else [])
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode:
        raise RuntimeError(f"databricks {' '.join(args[:3])}: {p.stderr.strip()[:300]}")
    return json.loads(p.stdout) if p.stdout.strip() else {}


def securable_owner(profile, kind: str, name: str) -> str | None:
    return _cli(profile, "catalogs" if kind == "catalog" else "schemas", "get", name).get("owner")


def current_grants(profile, kind: str, name: str) -> dict[str, set[str]]:
    d = _cli(profile, "grants", "get", kind, name)
    return {a["principal"]: set(a.get("privileges") or []) for a in d.get("privilege_assignments", [])}


def warehouse_acl(profile, warehouse_id: str) -> dict[str, set[str]]:
    d = _cli(profile, "permissions", "get", "warehouses", warehouse_id)
    out: dict[str, set[str]] = {}
    for a in d.get("access_control_list", []):
        who = a.get("service_principal_name") or a.get("user_name") or a.get("group_name")
        out[who] = {p["permission_level"] for p in a.get("all_permissions", [])}
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--target", required=True, choices=["dev", "prod"])
    p.add_argument("--check", action="store_true", help="report what is missing, change nothing")
    p.add_argument("--owner-applied", action="store_true",
                   help="apply the `applied_by: owner` entries (run by the owner, with --profile)")
    for role, env in PRINCIPAL_ENV.items():
        p.add_argument(f"--{role.replace('_', '-')}-principal", dest=f"{role}_principal", default=os.environ.get(env))
    p.add_argument("--profile", help="Databricks CLI profile (omit in CI)")
    args = p.parse_args(argv)

    spec = yaml.safe_load((GRANTS_DIR / f"{args.target}.yml").read_text(encoding="utf-8"))
    principals = {role: getattr(args, f"{role}_principal") for role in PRINCIPAL_ENV}
    names = {v: k for k, v in principals.items() if v}
    failed = 0

    def handle(label: str, need: dict[str, list[str]], grant, report_only: bool):
        nonlocal failed
        for principal, privs in need.items():
            who = names.get(principal, principal)
            if args.check or report_only:
                print(f"{'PENDING ' if report_only else 'MISSING '} {label}: {who} lacks {', '.join(privs)}"
                      + ("  (owner applies: --owner-applied)" if report_only else ""))
                failed += 0 if report_only else 1
                continue
            try:
                grant(principal, privs)
                print(f"GRANTED  {label}: {who} += {', '.join(privs)}")
            except RuntimeError as exc:
                print(f"FAILED   {label}: {who} += {', '.join(privs)}: {exc}")
                failed += 1

    # CI applies the plain entries and only reports the owner-applied ones; --owner-applied does the reverse.
    for owner_applied in (False, True):
        report_only = owner_applied != args.owner_applied
        if args.owner_applied and not owner_applied:
            continue
        for schema, wanted in desired_grants(spec, principals, owner_applied).items():
            need = missing_privileges(wanted, current_grants(args.profile, "schema", schema),
                                      securable_owner(args.profile, "schema", schema))
            if not need:
                print(f"OK       {schema}")
            handle(schema, need, lambda pr, pv, s=schema: _cli(args.profile, "grants", "update", "schema", s,
                   body={"changes": [{"principal": pr, "add": pv}]}), report_only)
        cat = desired_catalog_grants(spec, principals, owner_applied)
        if cat:
            catalog = spec["catalog"]
            need = missing_privileges(cat, current_grants(args.profile, "catalog", catalog),
                                      securable_owner(args.profile, "catalog", catalog))
            if not need:
                print(f"OK       catalog {catalog}")
            handle(f"catalog {catalog}", need, lambda pr, pv: _cli(args.profile, "grants", "update", "catalog",
                   catalog, body={"changes": [{"principal": pr, "add": pv}]}), report_only)
        for wname, levels in desired_warehouse_permissions(spec, principals, owner_applied).items():
            ids = [w["id"] for w in _cli(args.profile, "warehouses", "list") if w["name"] == wname]
            if not ids:
                print(f"FAILED   warehouse {wname!r}: not found")
                failed += 1
                continue
            acl = warehouse_acl(args.profile, ids[0])
            need = {pr: [lvl] for pr, lvl in levels.items() if not has_level(acl.get(pr, set()), lvl)}
            if not need:
                print(f"OK       warehouse {wname}")
            handle(f"warehouse {wname}", need, lambda pr, pv, wid=ids[0]: _cli(
                args.profile, "permissions", "update", "warehouses", wid,
                body={"access_control_list": [{"service_principal_name": pr, "permission_level": pv[0]}]}),
                report_only)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
