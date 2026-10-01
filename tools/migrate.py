"""Schema migrations for the aidq_metadata database (Lakebase project aidq-metadata).

    python tools/migrate.py --env dev --plan              # read-only: what is applied, pending, changed
    python tools/migrate.py --env dev --apply             # apply pending files, in order
    python tools/migrate.py --env dev --backfill 002      # record files applied before this runner existed

Files are migrations/NNN_name.sql, applied in NNN order, forward-only. A file contains no BEGIN / COMMIT /
ROLLBACK: the runner owns the transaction. `--apply` runs each pending file and inserts its
aidq_metadata.schema_migrations row in ONE transaction, so a file is either applied and recorded or neither.
It stops at the first failure and refuses to run at all if an applied file's checksum changed (applied files
are byte-stable: add a new migration instead of editing one).

`--plan` changes nothing: it lists each file's state and dry-runs the pending files in order, all in one
transaction that is rolled back, so each file sees what the earlier ones create (as with --apply). It reports
whether each would apply cleanly or the error it would hit.

`--backfill UPTO` is for files that were applied by hand before this runner existed (001 and 002 on dev). It
records them only after checking that the live schema already matches them: it replays the files in a
transaction, compares a snapshot of the schema catalog (tables, columns, constraints, indexes, views,
functions, triggers) before and after, and rolls back. An identical snapshot means the files would change
nothing, so the schema matches them. Any difference is printed and nothing is recorded.

Environments: dev = branch `dev`, prod = branch `production`. Credentials: a short-lived database token for the
current Databricks identity (the owner locally with --profile, the ci-dev service principal in CI through the
DATABRICKS_* environment variables). Nothing is ever printed that contains the token.

Every migration transaction starts with SET LOCAL ROLE aidq_owner, so objects created by a migration are owned
by that no-login role (and stay alterable by later migrations run by any of its members), not by the caller.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

PROJECT = "aidq-metadata"
BRANCHES = {"dev": "dev", "prod": "production"}
DATABASE = "databricks_postgres"
SCHEMA = "aidq_metadata"
# Every migration transaction runs as this no-login role (SET LOCAL ROLE), so objects a migration creates are owned
# by it, not by whoever ran the migration (the owner locally, ci-dev in CI). Both are members of it.
OWNER_ROLE = "aidq_owner"
SET_OWNER_ROLE = f"SET LOCAL ROLE {OWNER_ROLE}"
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
FILE_RE = re.compile(r"^(\d{3})_([a-z0-9_]+)\.sql$")

TRACKING_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.schema_migrations (
  version     text PRIMARY KEY,
  name        text NOT NULL,
  checksum    text NOT NULL,
  applied_at  timestamptz NOT NULL DEFAULT now(),
  applied_by  text NOT NULL DEFAULT current_user,
  environment text NOT NULL
)
"""

# Schema catalog snapshot used by --backfill: one sorted list of text lines per object kind.
CATALOG_SNAPSHOT_SQL = {
    "columns": """
        SELECT table_name || '.' || column_name || ' ' || data_type || ' null=' || is_nullable
               || ' default=' || coalesce(column_default, '')
        FROM information_schema.columns WHERE table_schema = %(s)s""",
    "constraints": """
        SELECT c.conrelid::regclass::text || ' ' || c.conname || ' ' || pg_get_constraintdef(c.oid)
        FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = %(s)s""",
    "indexes": "SELECT indexdef FROM pg_indexes WHERE schemaname = %(s)s",
    "views": "SELECT viewname || ' ' || md5(definition) FROM pg_views WHERE schemaname = %(s)s",
    "functions": """
        SELECT p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ') ' || md5(pg_get_functiondef(p.oid))
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = %(s)s""",
    "triggers": """
        SELECT pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %(s)s AND NOT t.tgisinternal""",
}


# -- pure -------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Migration:
    version: str
    name: str
    path: Path
    sql: str
    checksum: str


def checksum(text: str) -> str:
    """sha256 of the file text with CRLF normalized to LF (a Windows checkout must not change it)."""
    return hashlib.sha256(text.replace("\r\n", "\n").encode("utf-8")).hexdigest()


def strip_bodies(sql: str) -> str:
    """The SQL with comments, quoted strings and dollar-quoted bodies removed, for statement scanning."""
    out, i, n = [], 0, len(sql)
    while i < n:
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif sql[i] == "'":
            j = i + 1
            while j < n and not (sql[j] == "'" and not sql.startswith("''", j)):
                j += 2 if sql.startswith("''", j) else 1
            i = j + 1
        elif sql[i] == "$" and (m := re.match(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$", sql[i:])):
            tag = m.group(0)
            j = sql.find(tag, i + len(tag))
            i = n if j < 0 else j + len(tag)
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)


_TXN_STMT = re.compile(
    r"(?:\A|(?<=;))\s*(BEGIN|COMMIT|ROLLBACK|END|ABORT|START\s+TRANSACTION)(?:\s+(?:TRANSACTION|WORK))?\s*(?=;|\Z)",
    re.IGNORECASE,
)


def transaction_statements(sql: str) -> list[str]:
    """Top-level transaction-control statements in a migration file (must be none). PL/pgSQL BEGIN/END inside
    dollar-quoted bodies (DO blocks, functions) does not count: bodies are stripped first."""
    return [re.sub(r"\s+", " ", m.group(1).upper()) for m in _TXN_STMT.finditer(strip_bodies(sql))]


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    out, seen = [], set()
    for path in sorted(directory.glob("*.sql")):
        m = FILE_RE.match(path.name)
        if not m:
            raise ValueError(f"{path.name}: migration files are named NNN_lower_snake.sql")
        if m.group(1) in seen:
            raise ValueError(f"{path.name}: version {m.group(1)} is used twice")
        seen.add(m.group(1))
        text = path.read_text(encoding="utf-8")
        bad = transaction_statements(text)
        if bad:
            raise ValueError(f"{path.name}: contains {', '.join(bad)}; the runner owns the transaction")
        out.append(Migration(m.group(1), m.group(2), path, text, checksum(text)))
    return out


def plan(files: list[Migration], applied: dict[str, dict]) -> list[tuple[Migration, str]]:
    """(file, state) with state 'applied', 'CHANGED' (checksum differs from the recorded one) or 'pending'.
    Also reports recorded versions whose file is gone as ('<missing file>', 'MISSING')."""
    out = []
    for f in files:
        rec = applied.get(f.version)
        out.append((f, "pending" if rec is None else ("applied" if rec["checksum"] == f.checksum else "CHANGED")))
    for version in sorted(set(applied) - {f.version for f in files}):
        out.append((Migration(version, applied[version]["name"], Path(), "", ""), "MISSING"))
    return out


def snapshot_diff(before: dict[str, list[str]], after: dict[str, list[str]]) -> list[str]:
    lines = []
    for kind in sorted(set(before) | set(after)):
        b, a = set(before.get(kind, [])), set(after.get(kind, []))
        lines += [f"{kind}: - {x}" for x in sorted(b - a)] + [f"{kind}: + {x}" for x in sorted(a - b)]
    return lines


# -- database ---------------------------------------------------------------


def _cli_json(profile: str | None, *args: str):
    cmd = ["databricks", *args, "-o", "json"] + (["--profile", profile] if profile else [])
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args[:2])} failed: {proc.stderr.strip()[:400]}")
    return json.loads(proc.stdout) if proc.stdout.strip() else {}


def connect(env: str, profile: str | None):
    import psycopg

    endpoint_name = f"projects/{PROJECT}/branches/{BRANCHES[env]}/endpoints/primary"
    endpoint = _cli_json(profile, "postgres", "get-endpoint", endpoint_name)
    if endpoint["status"].get("disabled"):
        raise RuntimeError(f"Endpoint {endpoint_name} is disabled; enable it first (this tool never does).")
    token = _cli_json(profile, "postgres", "generate-database-credential", endpoint_name)["token"]
    user = _cli_json(profile, "current-user", "me")["userName"]
    return psycopg.connect(host=endpoint["status"]["hosts"]["host"], dbname=DATABASE, user=user, password=token,
                           sslmode="require", connect_timeout=60, autocommit=False)


def check_owner_role(conn) -> str | None:
    """None when the current user may SET ROLE to OWNER_ROLE, else why not."""
    row = conn.execute(
        "SELECT (SELECT count(*) FROM pg_roles WHERE rolname = %s), "
        "CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s) THEN pg_has_role(current_user, %s, 'MEMBER') END",
        (OWNER_ROLE, OWNER_ROLE, OWNER_ROLE),
    ).fetchone()
    conn.rollback()
    if not row[0]:
        return f"role {OWNER_ROLE} does not exist in this database (the owner creates it first)"
    if not row[1]:
        return f"the current user is not a member of {OWNER_ROLE}"
    return None


def read_applied(conn) -> dict[str, dict]:
    exists = conn.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.schema_migrations",)).fetchone()[0]
    if not exists:
        return {}
    rows = conn.execute(f"SELECT version, name, checksum FROM {SCHEMA}.schema_migrations").fetchall()
    return {v: {"name": n, "checksum": c} for v, n, c in rows}


def catalog_snapshot(conn) -> dict[str, list[str]]:
    # regclass and pg_get_*def print names relative to search_path, and migration files set it; pin it so the
    # before and after snapshots render every name schema-qualified and only real differences show.
    conn.execute("SET LOCAL search_path TO pg_catalog")
    return {k: sorted(r[0] for r in conn.execute(q, {"s": SCHEMA}).fetchall()) for k, q in CATALOG_SNAPSHOT_SQL.items()}


def dry_run(conn, m: Migration) -> str | None:
    """Run the file in a transaction (as OWNER_ROLE, like --apply) and roll back. None = clean, else the error."""
    try:
        conn.execute(SET_OWNER_ROLE)
        conn.execute(m.sql)
        return None
    except Exception as exc:
        return f"{type(exc).__name__}: {str(exc).splitlines()[0][:300]}"
    finally:
        conn.rollback()


def dry_run_sequence(conn, pending: list[Migration]) -> dict[str, str | None]:
    """Dry-run the pending files IN ORDER in one transaction (as OWNER_ROLE), then roll back everything.

    Each file sees what the earlier ones created, exactly as --apply runs them (e.g. 002 needs 001's table).
    version -> None (clean), the error, or "not tried" for files after the first failure.
    """
    out: dict[str, str | None] = {}
    failed = False
    try:
        conn.execute(SET_OWNER_ROLE)
        for m in pending:
            if failed:
                out[m.version] = "not tried: an earlier file failed"
                continue
            try:
                conn.execute(m.sql)
                out[m.version] = None
            except Exception as exc:
                out[m.version] = f"{type(exc).__name__}: {str(exc).splitlines()[0][:300]}"
                failed = True
    finally:
        conn.rollback()
    return out


def apply_one(conn, m: Migration, env: str) -> None:
    """The file and its tracking row, in one transaction, as OWNER_ROLE."""
    try:
        conn.execute(SET_OWNER_ROLE)
        conn.execute(TRACKING_DDL)
        conn.execute(m.sql)
        conn.execute(
            f"INSERT INTO {SCHEMA}.schema_migrations (version, name, checksum, environment) VALUES (%s, %s, %s, %s)",
            (m.version, m.name, m.checksum, env),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# -- commands ---------------------------------------------------------------


def cmd_plan(conn, files, env) -> int:
    rows = plan(files, read_applied(conn))
    print(f"aidq_metadata migrations on {env} ({BRANCHES[env]} branch):")
    dry = dry_run_sequence(conn, [m for m, s in rows if s == "pending"])
    bad = 0
    for m, state in rows:
        note = ""
        if state == "pending":
            err = dry[m.version]
            note = "dry run OK" if err is None else f"dry run FAILS: {err}"
            bad += err is not None
        elif state in ("CHANGED", "MISSING"):
            bad += 1
            note = "applied file was edited: add a new migration instead" if state == "CHANGED" else "recorded, but the file is gone"
        print(f"  {m.version} {m.name:<32} {state:<8} {note}".rstrip())
    return 1 if bad else 0


def cmd_apply(conn, files, env) -> int:
    rows = plan(files, read_applied(conn))
    problems = [(m, s) for m, s in rows if s in ("CHANGED", "MISSING")]
    if problems:
        for m, s in problems:
            print(f"REFUSED: {m.version} {m.name} is {s}")
        return 1
    pending = [m for m, s in rows if s == "pending"]
    if not pending:
        print(f"{env}: nothing to apply")
        return 0
    for m in pending:
        try:
            apply_one(conn, m, env)
        except Exception as exc:
            print(f"FAILED {m.version} {m.name}: {type(exc).__name__}: {str(exc).splitlines()[0][:300]} (rolled back; later files not run)")
            return 1
        print(f"applied {m.version} {m.name}")
    return 0


def cmd_backfill(conn, files, env, upto: str) -> int:
    applied = read_applied(conn)
    targets = [m for m in files if m.version <= upto and m.version not in applied]
    if not targets:
        print("nothing to backfill")
        return 0
    try:
        conn.execute(SET_OWNER_ROLE)
        before = catalog_snapshot(conn)
        for m in targets:
            conn.execute(m.sql)
        after = catalog_snapshot(conn)
    except Exception as exc:
        conn.rollback()
        print(f"REFUSED: replaying {', '.join(m.version for m in targets)} failed, so the schema cannot be verified: "
              f"{type(exc).__name__}: {str(exc).splitlines()[0][:300]}")
        return 1
    conn.rollback()
    diff = snapshot_diff(before, after)
    if diff:
        print("REFUSED: the live schema does not match the files; replaying them would change:")
        for line in diff:
            print(f"  {line}")
        return 1
    try:
        conn.execute(SET_OWNER_ROLE)
        conn.execute(TRACKING_DDL)
        for m in targets:
            conn.execute(
                f"INSERT INTO {SCHEMA}.schema_migrations (version, name, checksum, environment, applied_by) "
                "VALUES (%s, %s, %s, %s, current_user || ' (backfill)')",
                (m.version, m.name, m.checksum, env),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    print(f"schema matches {', '.join(m.version for m in targets)}: recorded as applied (backfill)")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--env", required=True, choices=sorted(BRANCHES))
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--backfill", metavar="UPTO", help="record files up to this version after verifying the schema")
    p.add_argument("--profile", help="Databricks CLI profile (omit in CI: DATABRICKS_* variables are used)")
    args = p.parse_args(argv)

    files = discover()
    conn = connect(args.env, args.profile)
    try:
        problem = check_owner_role(conn)
        if problem:
            print(f"REFUSED: {problem}; migrations run as {OWNER_ROLE} so it owns what they create")
            return 1
        if args.plan:
            return cmd_plan(conn, files, args.env)
        if args.apply:
            return cmd_apply(conn, files, args.env)
        return cmd_backfill(conn, files, args.env, args.backfill)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
