"""Postgres side of the NetSuite generator: connect, back up, read, write.

Connects to the netsuite-sample Lakebase project with a short-lived OAuth
database credential minted through the Databricks CLI. The token stays in
memory: it is never printed, logged or written to disk. If the compute
endpoint is disabled this module refuses to run instead of enabling it.

All writes happen inside one transaction per run, so a failure leaves the
database as it was.
"""

from __future__ import annotations

import datetime as dt
import json
import subprocess

from netsuite_gen import (
    COLUMNS,
    DRIFT_COLUMN,
    DRIFT_TABLE,
    INCREMENTAL_TABLES,
    SCHEMA,
    T_CUSTOMERS,
    T_LINES,
    T_MEMBERSHIPS,
    T_CERTIFICATIONS,
    TABLES,
    VALID_CERT_TYPE,
    VALID_MEMBERSHIP_STATUS,
    columns_for,
)

PROJECT = "netsuite-sample"
BRANCH = "production"
ENDPOINT = f"projects/{PROJECT}/branches/{BRANCH}/endpoints/primary"
DATABASE = "databricks_postgres"
COPY_BATCH = 5000


def _cli_json(profile: str, *args: str):
    proc = subprocess.run(
        ["databricks", *args, "--profile", profile, "-o", "json"], capture_output=True, text=True
    )
    if proc.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args[:2])} failed: {proc.stderr.strip()[:400]}")
    return json.loads(proc.stdout) if proc.stdout.strip() else {}


def connect(profile: str = "DEFAULT"):
    import psycopg
    from psycopg.rows import dict_row

    endpoint = _cli_json(profile, "postgres", "get-endpoint", ENDPOINT)
    if endpoint["status"].get("disabled"):
        raise RuntimeError(
            f"Endpoint {ENDPOINT} is disabled. Enable it first: databricks postgres update-endpoint "
            f'{ENDPOINT} spec.disabled --json \'{{"spec":{{"disabled":false}}}}\' --profile {profile}'
        )
    host = endpoint["status"]["hosts"]["host"]
    token = _cli_json(profile, "postgres", "generate-database-credential", ENDPOINT)["token"]
    user = _cli_json(profile, "current-user", "me")["userName"]
    return psycopg.connect(
        host=host, user=user, password=token, dbname=DATABASE, sslmode="require",
        connect_timeout=30, row_factory=dict_row,
    )


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table(table: str, schema: str = SCHEMA) -> str:
    return f"{_ident(schema)}.{_ident(table)}"


# -- reads ------------------------------------------------------------------


def read_live_columns(conn) -> dict[str, list[str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = %s ORDER BY table_name, ordinal_position",
            (SCHEMA,),
        )
        out: dict[str, list[str]] = {}
        for row in cur.fetchall():
            out.setdefault(row["table_name"], []).append(row["column_name"])
    conn.rollback()
    return out


def read_existing(conn, live_columns: dict[str, list[str]]) -> dict[str, list[dict]]:
    existing: dict[str, list[dict]] = {}
    with conn.cursor() as cur:
        for table in TABLES:
            cols = [c for c in columns_for(table, True) if c in live_columns.get(table, [])]
            cur.execute(f"SELECT {', '.join(_ident(c) for c in cols)} FROM {_table(table)}")
            existing[table] = list(cur.fetchall())
    conn.rollback()
    return existing


def items_from_rows(lines: list[dict]) -> list[tuple]:
    """(item_id, item_name, rate) per distinct item seen on existing lines."""
    items: dict = {}
    for r in lines:
        if r.get("item_id") is not None and r.get("item_name") and isinstance(r.get("rate"), int):
            items[r["item_id"]] = (r["item_id"], r["item_name"], r["rate"])
    return list(items.values())


def read_items(conn) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT item_id, min(item_name) AS item_name, max(rate) AS rate FROM {_table(T_LINES)} "
            "WHERE item_id IS NOT NULL AND item_name IS NOT NULL AND rate IS NOT NULL GROUP BY item_id"
        )
        rows = [(r["item_id"], r["item_name"], r["rate"]) for r in cur.fetchall()]
    conn.rollback()
    return rows


# -- backup -----------------------------------------------------------------


def _pre_existing_anomalies(cur) -> dict:
    cur.execute(
        f"SELECT count(*) AS n FROM {_table(T_MEMBERSHIPS)} WHERE membership_status <> ALL(%s)",
        (list(VALID_MEMBERSHIP_STATUS),),
    )
    bad_status = cur.fetchone()["n"]
    cur.execute(
        f"SELECT count(*) AS n FROM {_table(T_CERTIFICATIONS)} WHERE certification_type <> ALL(%s)",
        (list(VALID_CERT_TYPE),),
    )
    bad_cert = cur.fetchone()["n"]
    cur.execute(
        f"SELECT count(*) AS n FROM {_table(T_CUSTOMERS)} WHERE created_date IS NULL AND updated_date IS NULL"
    )
    null_dates = cur.fetchone()["n"]
    return {
        "membership_status_invalid_rows": bad_status,
        "certification_type_invalid_rows": bad_cert,
        "customers_rows_with_null_created_and_updated_date": null_dates,
    }


def create_backup(conn, profile: str = "DEFAULT") -> dict:
    """Back up the five tables before --init touches anything.

    1. Lakebase branch (copy-on-write snapshot of the whole project), best effort.
    2. A copy of the five tables in a new backup schema, verified by row counts.
    The run only proceeds if the schema copy is verified (see require_backup).
    """
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M")
    info: dict = {
        "performed": True, "taken_at": stamp, "branch": None, "schema": None,
        "row_counts": {}, "verified": False, "pre_existing": {},
    }

    branch_id = f"pre-synthetic-backup-{stamp}"
    try:
        _cli_json(
            profile, "postgres", "create-branch", f"projects/{PROJECT}", branch_id,
            "--json", json.dumps({"spec": {"source_branch": f"projects/{PROJECT}/branches/{BRANCH}", "no_expiry": True}}),
        )
        info["branch"] = {"name": f"projects/{PROJECT}/branches/{branch_id}", "status": "created"}
    except RuntimeError as exc:  # e.g. branch quota reached
        info["branch"] = {"name": branch_id, "status": "failed", "error": str(exc)[:300]}

    schema = f"netsuite_backup_{stamp}"
    with conn.cursor() as cur:
        info["pre_existing"] = _pre_existing_anomalies(cur)
        cur.execute(f"CREATE SCHEMA {_ident(schema)}")
        counts = {}
        for table in TABLES:
            cur.execute(f"CREATE TABLE {_table(table, schema)} AS TABLE {_table(table)}")
            cur.execute(f"SELECT count(*) AS n FROM {_table(table)}")
            source_n = cur.fetchone()["n"]
            cur.execute(f"SELECT count(*) AS n FROM {_table(table, schema)}")
            counts[table] = {"source": source_n, "backup": cur.fetchone()["n"]}
    conn.commit()
    info["schema"] = schema
    info["row_counts"] = counts
    info["verified"] = all(c["source"] == c["backup"] for c in counts.values())
    return info


def require_backup(info: dict) -> None:
    """Refuse to modify data unless a verified backup exists."""
    if not info or not info.get("performed") or not info.get("verified"):
        raise RuntimeError("Refusing to modify netsuite tables: no verified backup (see manifest 'backup').")


# -- writes -----------------------------------------------------------------


def _copy(cur, table: str, cols: list[str], rows: list[dict]) -> None:
    if not rows:
        return
    sql = f"COPY {_table(table)} ({', '.join(_ident(c) for c in cols)}) FROM STDIN"
    for i in range(0, len(rows), COPY_BATCH):
        with cur.copy(sql) as cp:
            for row in rows[i : i + COPY_BATCH]:
                cp.write_row([row.get(c) for c in cols])


def load_init(conn, data: dict[str, list[dict]]) -> None:
    """Truncate the five tables and load the base snapshot (single transaction)."""
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE " + ", ".join(_table(t) for t in TABLES))
            for table in TABLES:
                _copy(cur, table, COLUMNS[table], data[table])
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def load_increment(conn, result, apply_drift: bool = False, region_present: bool = False) -> None:
    populate_region = apply_drift or region_present
    cust_cols = COLUMNS[T_CUSTOMERS]
    assignments = ", ".join(f"{_ident(c)} = EXCLUDED.{_ident(c)}" for c in cust_cols[1:])
    upsert_sql = (
        f"INSERT INTO {_table(T_CUSTOMERS)} ({', '.join(_ident(c) for c in cust_cols)}) "
        f"VALUES ({', '.join(['%s'] * len(cust_cols))}) "
        f"ON CONFLICT ({_ident(cust_cols[0])}) DO UPDATE SET {assignments}"
    )
    try:
        with conn.cursor() as cur:
            if apply_drift:
                cur.execute(
                    f"ALTER TABLE {_table(DRIFT_TABLE)} ADD COLUMN IF NOT EXISTS {_ident(DRIFT_COLUMN)} text"
                )
            _copy(cur, T_CUSTOMERS, cust_cols, result.data[T_CUSTOMERS])
            upserts = result.upserts.get(T_CUSTOMERS, [])
            for i in range(0, len(upserts), 1000):
                cur.executemany(upsert_sql, [[r.get(c) for c in cust_cols] for r in upserts[i : i + 1000]])
            for table in INCREMENTAL_TABLES:
                _copy(cur, table, columns_for(table, populate_region), result.data[table])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
