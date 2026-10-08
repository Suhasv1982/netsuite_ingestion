"""Postgres group roles for the DQ recommender on the dev metadata branch (grants/dev.yml, `metadata_roles`).

    python tools/dq_roles.py --sql --agent-principal <app id>                    # print the SQL, change nothing
    python tools/dq_roles.py --check --agent-principal <app id> --profile DEFAULT  # exact match or exit 1
    python tools/dq_roles.py --apply --agent-principal <app id> --profile DEFAULT  # the OWNER: create and grant

--apply is run by the owner only (via tools/setup_dq_roles.sh): creating roles and role memberships is owner-only.
It is idempotent and runs in one transaction. --check is read-only and fails on anything missing or extra:
a missing role, membership or privilege, any extra table privilege for aidq_agent (it must never UPDATE or
DELETE), and any of the three roles (or the agent's login role) being a member of aidq_owner.
Principals: `agent` = the dq-agent service principal's application id (DQ_AGENT_SP), `ci` = ci-dev's
(DATABRICKS_CLIENT_ID or --ci-principal), `owner` = the owner's user name (GRANT_OWNER_PRINCIPAL).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = "aidq_metadata"
ENDPOINT = "projects/aidq-metadata/branches/dev/endpoints/primary"
OWNER_ROLE = "aidq_owner"
PRINCIPAL_ENV = {"agent": "DQ_AGENT_SP", "ci": "DATABRICKS_CLIENT_ID", "owner": "GRANT_OWNER_PRINCIPAL"}


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# -- pure -------------------------------------------------------------------


def load_spec(path: Path = ROOT / "grants" / "dev.yml") -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))["metadata_roles"]


def role_sql(spec: dict, principals: dict[str, str]) -> list[str]:
    """Idempotent statements that create the group roles, their memberships and their object privileges."""
    out = []
    for role, r in spec.items():
        out.append(f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN "
                   f"CREATE ROLE {q(role)} NOLOGIN; END IF; END $$")
        out.append(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {q(role)}")
        if r.get("select_all"):
            out.append(f"GRANT SELECT ON ALL TABLES IN SCHEMA {SCHEMA} TO {q(role)}")
        for table in r.get("insert", []):
            out.append(f"GRANT INSERT ON {SCHEMA}.{q(table)} TO {q(role)}")
        for table, cols in (r.get("update") or {}).items():
            out.append(f"GRANT UPDATE ({', '.join(q(c) for c in cols)}) ON {SCHEMA}.{q(table)} TO {q(role)}")
        for fn in r.get("execute", []):
            out.append(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{fn} TO {q(role)}")
        for member in r.get("members", []):
            if not principals.get(member):
                raise ValueError(f"principal {member!r} for {role} is not set (--{member}-principal or "
                                 f"{PRINCIPAL_ENV[member]})")
            out.append(f"GRANT {q(role)} TO {q(principals[member])}")
    return out


def diff(spec: dict, principals: dict[str, str], actual: dict) -> tuple[list[str], list[str]]:
    """(missing, extra) against `actual` = {"roles": set, "members": {role: set}, "owner_members": set,
    "table_privs": {role: {(table, priv)}}, "column_privs": {role: {(table, column, priv)}}, "exec": {role: set}}."""
    missing, extra = [], []
    for role, r in spec.items():
        if role not in actual["roles"]:
            missing.append(f"role {role}")
            continue
        for m in r.get("members", []):
            if principals.get(m) not in actual["members"].get(role, set()):
                missing.append(f"{role}: member {m}")
        want_tab = {(t, "INSERT") for t in r.get("insert", [])}
        have_tab = actual["table_privs"].get(role, set())
        missing += [f"{role}: {p} on {t}" for t, p in sorted(want_tab - have_tab)]
        extra += [f"{role}: {p} on {t}" for t, p in sorted(have_tab - want_tab) if p != "SELECT"]
        if r.get("select_all") and not actual.get("select_complete", {}).get(role, False):
            missing.append(f"{role}: SELECT on every table")
        want_col = {(t, c, "UPDATE") for t, cols in (r.get("update") or {}).items() for c in cols}
        have_col = actual["column_privs"].get(role, set())
        missing += [f"{role}: UPDATE({c}) on {t}" for t, c, _ in sorted(want_col - have_col)]
        extra += [f"{role}: UPDATE({c}) on {t}" for t, c, _ in sorted(have_col - want_col)]
        want_ex = {fn.split("(")[0] for fn in r.get("execute", [])}
        missing += [f"{role}: EXECUTE {f}" for f in sorted(want_ex - actual["exec"].get(role, set()))]
    banned = set(spec) | {principals[m] for r in spec.values() for m in r.get("members", [])
                          if m == "agent" and principals.get(m)}
    extra += [f"{name} is a member of {OWNER_ROLE}" for name in sorted(banned & actual["owner_members"])]
    return missing, extra


# -- I/O --------------------------------------------------------------------


def _cli(profile, *args):
    cmd = ["databricks", *args, "-o", "json"] + (["--profile", profile] if profile else [])
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode:
        raise RuntimeError(f"databricks {' '.join(args[:3])}: {p.stderr.strip()[:300]}")
    return json.loads(p.stdout)


def connect(profile):
    import psycopg

    ep = _cli(profile, "postgres", "get-endpoint", ENDPOINT)
    if ep["status"].get("disabled"):
        raise RuntimeError(f"{ENDPOINT} is disabled (this tool never enables it)")
    token = _cli(profile, "postgres", "generate-database-credential", ENDPOINT)["token"]
    user = _cli(profile, "current-user", "me")["userName"]
    return psycopg.connect(host=ep["status"]["hosts"]["host"], dbname="databricks_postgres", user=user,
                           password=token, sslmode="require", connect_timeout=60)


def read_actual(conn, roles: list[str]) -> dict:
    roles_present = {r for (r,) in conn.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", (roles,))}
    members: dict[str, set] = {}
    for role, member in conn.execute(
            "SELECT g.rolname, m.rolname FROM pg_auth_members a JOIN pg_roles g ON g.oid = a.roleid "
            "JOIN pg_roles m ON m.oid = a.member WHERE g.rolname = ANY(%s)", (roles,)):
        members.setdefault(role, set()).add(member)
    owner_members = {m for (m,) in conn.execute(
        "SELECT m.rolname FROM pg_auth_members a JOIN pg_roles g ON g.oid = a.roleid "
        "JOIN pg_roles m ON m.oid = a.member WHERE g.rolname = %s", (OWNER_ROLE,))}
    table_privs: dict[str, set] = {}
    for grantee, table, priv in conn.execute(
            "SELECT grantee, table_name, privilege_type FROM information_schema.role_table_grants "
            "WHERE table_schema = %s AND grantee = ANY(%s)", (SCHEMA, roles)):
        table_privs.setdefault(grantee, set()).add((table, priv))
    n_tables = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = %s",
                            (SCHEMA,)).fetchone()[0]
    select_complete = {r: sum(1 for _, p in table_privs.get(r, set()) if p == "SELECT") >= n_tables for r in roles}
    column_privs: dict[str, set] = {}
    for grantee, table, column, priv in conn.execute(
            "SELECT grantee, table_name, column_name, privilege_type FROM information_schema.column_privileges "
            "WHERE table_schema = %s AND grantee = ANY(%s) AND privilege_type = 'UPDATE'", (SCHEMA, roles)):
        column_privs.setdefault(grantee, set()).add((table, column, priv))
    # a table-level UPDATE shows up for every column; report it as table privilege instead
    for role, privs in table_privs.items():
        if any(p == "UPDATE" for _, p in privs):
            column_privs.pop(role, None)
    exec_: dict[str, set] = {}
    for role in roles_present:
        for (fn,) in conn.execute(
                "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = %s AND has_function_privilege(%s, p.oid, 'EXECUTE')", (SCHEMA, role)):
            exec_.setdefault(role, set()).add(fn)
    conn.rollback()
    return {"roles": roles_present, "members": members, "owner_members": owner_members, "table_privs": table_privs,
            "select_complete": select_complete, "column_privs": column_privs, "exec": exec_}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--sql", action="store_true", help="print the statements, change nothing")
    mode.add_argument("--check", action="store_true", help="read-only: exact match or exit 1")
    mode.add_argument("--apply", action="store_true", help="the owner: create roles and grant, one transaction")
    for role, env in PRINCIPAL_ENV.items():
        p.add_argument(f"--{role}-principal", dest=role, default=os.environ.get(env))
    p.add_argument("--profile", default=None)
    args = p.parse_args(argv)
    spec = load_spec()
    principals = {r: getattr(args, r) for r in PRINCIPAL_ENV}
    stmts = role_sql(spec, principals)
    if args.sql:
        print(";\n".join(stmts) + ";")
        return 0
    conn = connect(args.profile)
    try:
        if args.apply:
            for s in stmts:
                conn.execute(s)
            conn.commit()
            print(f"applied {len(stmts)} statements")
        actual = read_actual(conn, list(spec))
    finally:
        conn.close()
    missing, extra = diff(spec, principals, actual)
    for m in missing:
        print(f"MISSING  {m}")
    for e in extra:
        print(f"EXTRA    {e}")
    if not missing and not extra:
        print(f"OK       {', '.join(spec)} match grants/dev.yml")
    return 1 if missing or extra else 0


if __name__ == "__main__":
    sys.exit(main())
