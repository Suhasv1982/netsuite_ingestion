"""Verify (CI) or apply (owner) the source grants in grants/source.yml for the data-generator role.

    python tools/apply_pg_grants.py --check                     # CI, as ci-dev: exact match or exit 1
    python tools/apply_pg_grants.py --apply --profile DEFAULT   # the owner: GRANT what is missing

--check fails on anything missing AND on anything extra: a privilege not listed for a table (e.g. DELETE, TRUNCATE,
UPDATE on an append-only table), ownership of a source table, or membership in another role. --apply only grants
missing privileges; it never revokes (an extra privilege is reported for the owner to decide).
Connects to netsuite-sample / production as the caller (CLI profile locally, DATABRICKS_* variables in CI).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

SPEC = Path(__file__).resolve().parent.parent / "grants" / "source.yml"
ENDPOINT = "projects/netsuite-sample/branches/production/endpoints/primary"
DATABASE = "databricks_postgres"
ALL_TABLE_PRIVS = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"]


# -- pure -------------------------------------------------------------------


def diff_table_privileges(spec_tables: dict, actual: dict[str, set[str]]) -> tuple[list[str], list[str]]:
    """(missing, extra) as 'table: PRIV' strings. `actual` = table -> privileges the role holds."""
    missing, extra = [], []
    for table, wanted in spec_tables.items():
        have = actual.get(table, set())
        missing += [f"{table}: {p}" for p in wanted if p not in have]
        extra += [f"{table}: {p}" for p in sorted(have - set(wanted))]
    return missing, extra


def grant_statements(role: str, missing_tables: list[str], schema_missing: list[str], db_missing: list[str]) -> list[str]:
    q = '"' + role.replace('"', '""') + '"'
    out = [f"GRANT USAGE ON SCHEMA {s} TO {q}" for s in schema_missing]
    out += [f"GRANT {p} ON DATABASE {DATABASE} TO {q}" for p in db_missing]
    for item in missing_tables:
        table, priv = item.split(": ")
        out.append(f"GRANT {priv} ON {table} TO {q}")
    return out


# -- I/O --------------------------------------------------------------------


def _cli(profile, *args):
    cmd = ["databricks", *args, "-o", "json"] + (["--profile", profile] if profile else [])
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode:
        raise RuntimeError(f"databricks {' '.join(args[:3])}: {p.stderr.strip()[:300]}")
    return json.loads(p.stdout) if p.stdout.strip() else {}


def connect(profile):
    import psycopg

    ep = _cli(profile, "postgres", "get-endpoint", ENDPOINT)
    if ep["status"].get("disabled"):
        raise RuntimeError(f"{ENDPOINT} is disabled; enable it first (this tool never does).")
    token = _cli(profile, "postgres", "generate-database-credential", ENDPOINT)["token"]
    user = _cli(profile, "current-user", "me")["userName"]
    return psycopg.connect(host=ep["status"]["hosts"]["host"], dbname=DATABASE, user=user, password=token,
                           sslmode="require", connect_timeout=60)


def inspect(conn, role: str, spec: dict) -> dict:
    def one(sql, *a):
        return conn.execute(sql, a).fetchone()[0]

    if not one("SELECT count(*) FROM pg_roles WHERE rolname = %s", role):
        return {"exists": False}
    tables = {t: {p for p in ALL_TABLE_PRIVS if one("SELECT has_table_privilege(%s, %s, %s)", role, t, p)}
              for t in spec["tables"]}
    return {
        "exists": True,
        "tables": tables,
        "schema_missing": [s for s in spec["schema_usage"]
                           if not one("SELECT has_schema_privilege(%s, %s, 'USAGE')", role, s)],
        "db_missing": [p for p in spec["database"]
                       if not one("SELECT has_database_privilege(%s, %s, %s)", role, DATABASE, p)],
        "owns": [t for t in spec["tables"]
                 if one("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = %s::regclass", t) == role],
        "member_of": [r[0] for r in conn.execute(
            "SELECT b.rolname FROM pg_auth_members m JOIN pg_roles a ON a.oid = m.member "
            "JOIN pg_roles b ON b.oid = m.roleid WHERE a.rolname = %s", (role,)).fetchall()],
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    p.add_argument("--role", help="Postgres role (default: the role_variable environment variable)")
    p.add_argument("--profile")
    args = p.parse_args(argv)

    spec = yaml.safe_load(SPEC.read_text(encoding="utf-8"))
    role = args.role or os.environ.get(spec["role_variable"])
    if not role:
        print(f"FAILED: no role (set {spec['role_variable']} or --role)")
        return 1
    with connect(args.profile) as conn:
        info = inspect(conn, role, spec)
        if not info["exists"]:
            print(f"FAILED: role {role} does not exist on the source branch")
            return 1
        missing, extra = diff_table_privileges(spec["tables"], info["tables"])
        problems = ([f"extra privilege {e}" for e in extra] + [f"owns {t}" for t in info["owns"]]
                    + [f"member of role {r}" for r in info["member_of"]])
        if args.apply:
            stmts = grant_statements(role, missing, info["schema_missing"], info["db_missing"])
            for s in stmts:
                conn.execute(s)
            conn.commit()
            for s in stmts:
                print(f"GRANTED  {s}")
            if not stmts:
                print("nothing to grant")
            missing, info["schema_missing"], info["db_missing"] = [], [], []
        else:
            conn.rollback()
        for m in missing:
            print(f"MISSING  {m}")
        for s in info["schema_missing"]:
            print(f"MISSING  USAGE on schema {s}")
        for d in info["db_missing"]:
            print(f"MISSING  {d} on database {DATABASE}")
        for pr in problems:
            print(f"FORBIDDEN {pr}")
        bad = missing or info["schema_missing"] or info["db_missing"] or problems
        if not bad:
            print("source grants: exact match with grants/source.yml")
        return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
