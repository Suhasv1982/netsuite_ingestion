"""Collect a pipeline "baseline" snapshot after a netsuite_ingestion run.

Records, as JSON: source row counts (Lakebase netsuite-sample), bronze/silver/
reject row counts and reject reasons (Unity Catalog), null/duplicate business
keys in silver, the latest pipeline update and its error events, the job run's
task states, and the run_audit rows the job logged (Lakebase aidq-metadata).

Read-only. Needs the `tools` extra (psycopg) and a Databricks CLI profile.

    python baseline/collect_metrics.py --label clean --job-run-id 123 --out baseline/clean.json
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

CATALOG = "poc_netsuite"  # v1 (prod) layout; --catalog overrides (v2 dev runs in `workspace`)
PIPELINE_ID = "d002bb31-38f5-4980-b6bb-5902c04c9098"  # prod netsuite_ingestion_poc; --pipeline-id overrides
WAREHOUSE_ID = "2c9afb562fcca499"
META_ENDPOINT = "projects/aidq-metadata/branches/production/endpoints/primary"
SILVER_KEYS = {
    "netsuite_memberships": "membership_internal_id",
    "netsuite_certifications": "certification_internal_id",
    "netsuite_transactions": "transaction_internal_id",
    "netsuite_transaction_lines": "transaction_line_id",
}


def cli(profile, *args, parse=True):
    proc = subprocess.run(["databricks", *args, "--profile", profile, "-o", "json"], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args[:2])}: {proc.stderr.strip()[:300]}")
    return json.loads(proc.stdout) if parse and proc.stdout.strip() else proc.stdout


def sql(profile, statement):
    body = {"warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": "50s", "format": "JSON_ARRAY"}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(body, fh)
    try:
        res = cli(profile, "api", "post", "/api/2.0/sql/statements", "--json", f"@{fh.name}")
    finally:
        os.unlink(fh.name)
    state = res["status"]["state"]
    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL {state}: {res['status'].get('error', {}).get('message', '')[:300]}")
    cols = [c["name"] for c in res.get("manifest", {}).get("schema", {}).get("columns", [])]  # DDL/DML return none
    result = res.get("result", {})
    rows = list(result.get("data_array", []))
    while result.get("next_chunk_internal_link"):  # large results arrive in chunks
        result = cli(profile, "api", "get", result["next_chunk_internal_link"])
        rows.extend(result.get("data_array", []))
    return [dict(zip(cols, row)) for row in rows]


def tables_in(profile, schema):
    return sorted(r["tableName"] for r in sql(profile, f"SHOW TABLES IN {CATALOG}.{schema}"))


def uc_counts(profile):
    out = {}
    for schema in ("poc_bronze", "poc_silver", "poc_reject"):
        out[schema] = {}
        for table in tables_in(profile, schema):
            try:
                out[schema][table] = int(sql(profile, f"SELECT count(*) AS n FROM {CATALOG}.{schema}.`{table}`")[0]["n"])
            except RuntimeError as exc:
                out[schema][table] = f"ERROR: {exc}"
    return out


def reject_breakdown(profile):
    try:
        rows = sql(profile, f"SELECT source_table, reason, count(*) AS n FROM {CATALOG}.poc_reject.rejected_rows GROUP BY 1, 2 ORDER BY 1, 2")
    except RuntimeError as exc:
        return {"error": str(exc)}
    return [{"source_table": r["source_table"], "reason": r["reason"], "rows": int(r["n"])} for r in rows]


def silver_key_quality(profile):
    out = {}
    for table, key in SILVER_KEYS.items():
        try:
            r = sql(
                profile,
                f"SELECT count(*) AS total, count(DISTINCT `{key}`) AS distinct_keys, "
                f"sum(CASE WHEN `{key}` IS NULL THEN 1 ELSE 0 END) AS null_keys "
                f"FROM {CATALOG}.poc_silver.{table}",
            )[0]
            out[table] = {k: int(v or 0) for k, v in r.items()}
        except RuntimeError as exc:
            out[table] = {"error": str(exc)}
    return out


ALL_KEYS = {"netsuite_customers": "customer_internal_id", **SILVER_KEYS}


def row_state(profile):
    """Row-level outcome data used to classify injected defects: which business
    keys reached silver (with their versions) and which were rejected."""
    silver, rejected = {}, {}
    for table, key in SILVER_KEYS.items():
        try:
            rows = sql(profile, f"SELECT `{key}` AS k, created_date AS c, updated_date AS u FROM {CATALOG}.poc_silver.{table}")
            silver[table] = {"rows": [[r["k"], r["c"], r["u"]] for r in rows]}
        except RuntimeError as exc:
            silver[table] = {"error": str(exc)}
        try:
            rows = sql(
                profile,
                f"SELECT reason, get_json_object(payload, '$.{key}') AS k FROM {CATALOG}.poc_reject.rejected_rows "
                f"WHERE source_table = '{table}'",
            )
            rejected[table] = [[r["reason"], r["k"]] for r in rows]
        except RuntimeError as exc:
            rejected[table] = {"error": str(exc)}
    return {"silver": silver, "rejected": rejected}


def table_columns(profile):
    out = {}
    for schema in ("poc_bronze", "poc_silver"):
        for table in tables_in(profile, schema):
            info = cli(profile, "tables", "get", f"{CATALOG}.{schema}.{table}")
            out[f"{schema}.{table}"] = [c["name"] for c in info.get("columns", [])]
    return out


def bronze_expected_vs_actual(profile):
    """Bronze tables the pipeline defines for the source's *current* watermark
    values (what bronze.py/dq.py resolve) vs the tables that actually exist
    (what log_run_audit matches with SHOW TABLES ... LIKE '<table>*')."""
    import netsuite_gen as ng
    import pg_writer

    conn = pg_writer.connect(profile)
    try:
        with conn.cursor() as cur:
            expected = {"netsuite_customers": ["netsuite_customers"]}
            for table in ng.INCREMENTAL_TABLES:
                cur.execute(f'SELECT DISTINCT created_date::date AS d FROM netsuite."{table}" ORDER BY 1')
                expected[table] = [f"{table}__{r['d']:%Y_%m_%d}" for r in cur.fetchall()]
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='netsuite' AND table_name='netsuite_transactions' ORDER BY ordinal_position")
            src_cols = [r["column_name"] for r in cur.fetchall()]
    finally:
        conn.close()
    actual = tables_in(profile, "poc_bronze")
    matched, leftovers = {}, []
    for table, names in expected.items():
        like = [n for n in actual if n.startswith(table)]  # LIKE 'table*'
        matched[table] = like
        leftovers += [n for n in like if n not in names]
    return {"expected_by_dq": expected, "matched_by_log_run_audit": matched,
            "leftover_bronze_tables": sorted(leftovers), "source_transactions_columns": src_cols}


def bronze_v2_state(profile):
    """v2 layout: ONE bronze table per incremental source with _snapshot_date, plus the key ledger and its
    per-date fingerprints (all read-only)."""
    import netsuite_gen as ng
    import pg_writer

    out = {"bronze": {}, "ledger": {}, "fingerprints": {}, "source_dates": {}, "source_columns": {}}
    for table in ng.INCREMENTAL_TABLES:
        try:
            rows = sql(profile, f"SELECT _snapshot_date AS d, count(*) AS n, count(DISTINCT `{ng.BUSINESS_KEY[table]}`) AS keys "
                                f"FROM {CATALOG}.poc_bronze.{table} GROUP BY 1 ORDER BY 1")
            out["bronze"][table] = [{"snapshot_date": r["d"], "rows": int(r["n"]), "distinct_keys": int(r["keys"])} for r in rows]
        except RuntimeError as exc:
            out["bronze"][table] = {"error": str(exc)}
        try:
            r = sql(profile, f"SELECT count(*) AS n, count(DISTINCT snapshot_date) AS dates FROM {CATALOG}.ledger.bronze_keys WHERE table_name = '{table}'")[0]
            out["ledger"][table] = {"pairs": int(r["n"]), "dates": int(r["dates"])}
            out["fingerprints"][table] = int(sql(profile, f"SELECT count(*) AS n FROM {CATALOG}.ledger.bronze_fingerprints WHERE table_name = '{table}'")[0]["n"])
        except RuntimeError as exc:
            out["ledger"][table] = {"error": str(exc)}
    conn = pg_writer.connect(profile)
    try:
        with conn.cursor() as cur:
            for table in ng.INCREMENTAL_TABLES:
                cur.execute(f'SELECT updated_date::date AS d, count(*) AS n FROM netsuite."{table}" GROUP BY 1 ORDER BY 1')
                out["source_dates"][table] = [{"snapshot_date": str(r["d"]), "rows": r["n"]} for r in cur.fetchall()]
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='netsuite' AND table_name='netsuite_transactions' ORDER BY ordinal_position")
            out["source_columns"]["netsuite_transactions"] = [r["column_name"] for r in cur.fetchall()]
    finally:
        conn.close()
    return out


def pg_connect(endpoint, profile):
    import psycopg
    from psycopg.rows import dict_row

    host = cli(profile, "postgres", "get-endpoint", endpoint)["status"]["hosts"]["host"]
    token = cli(profile, "postgres", "generate-database-credential", endpoint)["token"]
    user = cli(profile, "current-user", "me")["userName"]
    return psycopg.connect(host=host, user=user, password=token, dbname="databricks_postgres", sslmode="require", row_factory=dict_row)


def source_counts(profile):
    import netsuite_gen as ng
    import pg_writer

    conn = pg_writer.connect(profile)
    try:
        with conn.cursor() as cur:
            out = {}
            for table in ng.TABLES:
                cur.execute(f'SELECT count(*) AS n FROM netsuite."{table}"')
                out[table] = cur.fetchone()["n"]
        return out
    finally:
        conn.close()


def run_audit(profile, job_run_id):
    conn = pg_connect(META_ENDPOINT, profile)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM aidq_metadata.run_audit WHERE run_id = %s ORDER BY table_id, layer", (str(job_run_id),))
            rows = cur.fetchall()
        return [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()} for r in rows]
    finally:
        conn.close()


def pipeline_info(profile):
    pipe = cli(profile, "pipelines", "get", PIPELINE_ID)
    latest = (pipe.get("latest_updates") or [{}])[0]
    info = {"update_id": latest.get("update_id"), "state": latest.get("state"), "creation_time": latest.get("creation_time")}
    try:
        events = cli(profile, "pipelines", "list-pipeline-events", PIPELINE_ID, "--filter", "level='ERROR'", "--max-results", "25")
        info["error_events"] = [
            {"timestamp": e.get("timestamp"), "message": (e.get("message") or "")[:400],
             "exception": ((e.get("error") or {}).get("exceptions") or [{}])[0].get("message", "")[:400]}
            for e in events
            if not info["update_id"] or (e.get("origin") or {}).get("update_id") == info["update_id"]
        ]
    except RuntimeError as exc:
        info["error_events"] = [{"message": f"could not read events: {exc}"}]
    return info


def job_info(profile, job_run_id):
    run = cli(profile, "jobs", "get-run", str(job_run_id))
    return {
        "state": (run.get("state") or {}).get("result_state"),
        "message": (run.get("state") or {}).get("state_message"),
        "tasks": {t["task_key"]: (t.get("state") or {}).get("result_state") for t in run.get("tasks", [])},
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--label", required=True)
    p.add_argument("--job-run-id", type=int, required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--profile", default="DEFAULT")
    p.add_argument("--layout", choices=["v1", "v2"], default="v1")
    p.add_argument("--catalog")
    p.add_argument("--pipeline-id")
    args = p.parse_args()
    global CATALOG, PIPELINE_ID
    CATALOG = args.catalog or CATALOG
    PIPELINE_ID = args.pipeline_id or PIPELINE_ID

    snap = {"label": args.label, "job_run_id": args.job_run_id}
    for name, fn in [
        ("job", lambda: job_info(args.profile, args.job_run_id)),
        ("pipeline", lambda: pipeline_info(args.profile)),
        ("source_row_counts", lambda: source_counts(args.profile)),
        ("uc_row_counts", lambda: uc_counts(args.profile)),
        ("reject_breakdown", lambda: reject_breakdown(args.profile)),
        ("silver_key_quality", lambda: silver_key_quality(args.profile)),
        ("run_audit", lambda: run_audit(args.profile, args.job_run_id)),
        ("row_state", lambda: row_state(args.profile)),
        ("table_columns", lambda: table_columns(args.profile)),
        ("bronze_expected_vs_actual", lambda: bronze_expected_vs_actual(args.profile) if args.layout == "v1" else {}),
        ("v2", lambda: bronze_v2_state(args.profile) if args.layout == "v2" else {}),
    ]:
        try:
            snap[name] = fn()
        except Exception as exc:  # keep collecting: a failed run still yields a useful snapshot
            snap[name] = {"error": str(exc)[:400]}
    Path(args.out).write_text(json.dumps(snap, indent=2, default=str), encoding="utf-8")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
