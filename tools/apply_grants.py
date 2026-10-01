"""Apply the Unity Catalog grants a target needs (grants/<target>.yml). Additive only: grants what is missing,
never revokes anything, so it cannot take access away from anyone.

    python tools/apply_grants.py --target dev --check     # print what is missing, change nothing
    python tools/apply_grants.py --target dev             # grant what is missing

Principals in the file: `ci` = the identity running this (DATABRICKS_CLIENT_ID in CI, or --ci-principal),
`owner` = GRANT_OWNER_PRINCIPAL (GitHub Actions variable) or --owner-principal. Uses the grants API, so it needs
no SQL warehouse. The caller must be allowed to grant on the schemas (owner of the schema, or MANAGE on it).
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


# -- pure -------------------------------------------------------------------


def desired_grants(spec: dict, principals: dict[str, str]) -> dict[str, dict[str, set[str]]]:
    """'<catalog>.<schema>' -> principal -> privileges, from a grants file and the resolved principals."""
    out: dict[str, dict[str, set[str]]] = {}
    for g in spec["grants"]:
        role = g["principal"]
        if role not in principals or not principals[role]:
            raise ValueError(f"principal {role!r} is not resolved (set it with --{role}-principal or its env variable)")
        for schema in g["schemas"]:
            out.setdefault(f"{spec['catalog']}.{schema}", {}).setdefault(principals[role], set()).update(g["privileges"])
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


# -- I/O --------------------------------------------------------------------


def _cli(profile, *args, body=None):
    cmd = ["databricks", *args, "-o", "json"] + (["--profile", profile] if profile else [])
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode:
        raise RuntimeError(f"databricks {' '.join(args[:3])}: {p.stderr.strip()[:300]}")
    return json.loads(p.stdout) if p.stdout.strip() else {}


def schema_owner(profile, schema: str) -> str | None:
    return _cli(profile, "schemas", "get", schema).get("owner")


def current_grants(profile, schema: str) -> dict[str, set[str]]:
    d = _cli(profile, "grants", "get", "schema", schema)
    return {a["principal"]: set(a.get("privileges") or []) for a in d.get("privilege_assignments", [])}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--target", required=True, choices=["dev", "prod"])
    p.add_argument("--check", action="store_true", help="report what is missing, change nothing")
    p.add_argument("--ci-principal", default=os.environ.get("DATABRICKS_CLIENT_ID"))
    p.add_argument("--owner-principal", default=os.environ.get("GRANT_OWNER_PRINCIPAL"))
    p.add_argument("--profile", help="Databricks CLI profile (omit in CI)")
    args = p.parse_args(argv)

    spec = yaml.safe_load((GRANTS_DIR / f"{args.target}.yml").read_text(encoding="utf-8"))
    desired = desired_grants(spec, {"ci": args.ci_principal, "owner": args.owner_principal})
    failed = 0
    for schema, wanted in desired.items():
        need = missing_privileges(wanted, current_grants(args.profile, schema), schema_owner(args.profile, schema))
        if not need:
            print(f"OK       {schema}")
            continue
        for principal, privs in need.items():
            who = "ci" if principal == args.ci_principal else "owner"
            if args.check:
                print(f"MISSING  {schema}: {who} lacks {', '.join(privs)}")
                failed += 1
                continue
            try:
                _cli(args.profile, "grants", "update", "schema", schema,
                     body={"changes": [{"principal": principal, "add": privs}]})
                print(f"GRANTED  {schema}: {who} += {', '.join(privs)}")
            except RuntimeError as exc:
                print(f"FAILED   {schema}: {who} += {', '.join(privs)}: {exc}")
                failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
