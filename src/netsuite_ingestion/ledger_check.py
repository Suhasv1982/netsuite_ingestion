"""Job task: ledger repair check -- compare the key ledger with bronze and WARN on mismatch.

Runs after sync_ledger, when the two should be identical. For every
incremental table it checks
  * pairs (business_key, snapshot_date) bronze holds but the ledger lacks (missing:
    a top-up could re-load them) and pairs the ledger holds but bronze lacks (extra:
    bronze was rebuilt or rows were lost);
  * the stored per-date fingerprints against fingerprints recomputed from bronze
    (a wrong fingerprint makes bronze.py miss late rows or examine dates needlessly).

A mismatch never fails the job. It is printed as a WARN line and logged as a
`ledger` layer row in aidq_metadata.run_audit (status OK / WARN, rows_read =
ledger pairs, rows_written = bronze pairs, rows_rejected = pair mismatches +
fingerprint mismatches, error = the details). Recovery is `sync_ledger
--bronze-rebuild true`.
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from log_run_audit import RUN_AUDIT_SCHEMA
from metadata import (
    bronze_pairs_df,
    ensure_ledger_table,
    fingerprint_table_name,
    fingerprints_df,
    is_incremental,
    pg_conn_from_job_secret,
    read_table_defs,
    write_pg_rows,
)

KEY_COLUMNS = ["table_name", "business_key", "snapshot_date"]


def check_table(spark, catalog: str, ledger: str, table_def: dict) -> dict:
    table = table_def["source_table"]
    if not spark.catalog.tableExists(f"{catalog}.poc_bronze.{table}"):
        return {"table": table, "status": "WARN", "bronze": 0, "ledger": 0, "missing": 0, "extra": 0, "fp_bad": 0,
                "note": "bronze table does not exist yet"}
    bronze = bronze_pairs_df(spark, catalog, table_def).select(*KEY_COLUMNS)
    ledger_df = spark.table(ledger).where(f"table_name = '{table}'").select(*KEY_COLUMNS)
    missing = bronze.join(ledger_df, KEY_COLUMNS, "left_anti").count()
    extra = ledger_df.join(bronze, KEY_COLUMNS, "left_anti").count()

    # fingerprints: recomputed from bronze vs the stored ones (a date on only one side counts as a mismatch)
    actual = fingerprints_df(
        spark, bronze.select(F.col("business_key").alias("k"), F.col("snapshot_date").alias("d"))
    ).select("snapshot_date", "row_count", "fp_sum")
    stored = (
        spark.table(fingerprint_table_name(catalog))
        .where(f"table_name = '{table}'")
        .select("snapshot_date", "row_count", "fp_sum")
    )
    fp_bad = actual.exceptAll(stored).select("snapshot_date").union(stored.exceptAll(actual).select("snapshot_date")).distinct().count()
    return {
        "table": table, "status": "OK" if missing == 0 and extra == 0 and fp_bad == 0 else "WARN",
        "bronze": bronze.count(), "ledger": ledger_df.count(), "missing": missing, "extra": extra, "fp_bad": fp_bad, "note": "",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--meta-pg-host", required=True)
    parser.add_argument("--meta-pg-user", required=True)
    parser.add_argument("--meta-pg-key", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    meta_conn = pg_conn_from_job_secret(args.secret_scope, args.meta_pg_key, args.meta_pg_host, args.meta_pg_user)
    ledger = ensure_ledger_table(spark, args.catalog)
    started = datetime.now(timezone.utc)

    rows = []
    for table_def in read_table_defs(spark, meta_conn):
        if not is_incremental(table_def):
            continue
        r = check_table(spark, args.catalog, ledger, table_def)
        detail = r["note"] or (
            "" if r["status"] == "OK"
            else f"{r['missing']} pairs in bronze not in ledger, {r['extra']} pairs in ledger not in bronze, "
                 f"{r['fp_bad']} dates whose stored fingerprint differs from bronze; "
                 "recover with sync_ledger --bronze-rebuild true"
        )
        print(f"{r['status']} {r['table']}: bronze={r['bronze']} ledger={r['ledger']} missing={r['missing']} "
              f"extra={r['extra']} fingerprint_mismatches={r['fp_bad']} {detail}".rstrip())
        rows.append({
            "run_id": str(args.run_id), "table_id": table_def["table_id"], "layer": "ledger", "status": r["status"],
            "rows_read": r["ledger"], "rows_written": r["bronze"], "rows_rejected": r["missing"] + r["extra"] + r["fp_bad"],
            "started_at": started, "ended_at": datetime.now(timezone.utc), "error": detail or None,
        })
    if rows:
        write_pg_rows(spark, meta_conn, "aidq_metadata", "run_audit", RUN_AUDIT_SCHEMA, rows)
    if any(r["status"] == "WARN" for r in rows):
        print("WARN: ledger and bronze differ (see above); the job is not failed")


if __name__ == "__main__":
    main()
