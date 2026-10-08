"""Job task: keep the bronze key ledger and its per-date fingerprints in step with what bronze holds.

The ledger (<catalog>.ledger.bronze_keys) records the distinct
(table_name, business_key, snapshot_date, row_hash) entries loaded into each
incremental poc_bronze table; row_hash is bronze's `_row_hash`, the md5 Postgres
computed over the table's active source_columns. <catalog>.ledger.bronze_fingerprints
stores, per (table_name, snapshot_date), the fingerprint bronze.py compares with
the one Postgres computes for the source: the count of distinct entries and the
sum of their md5-derived bigints (see metadata.py). bronze.py reads both at graph
build to find late rows and second versions of a key on a loaded date -- a
pipeline cannot read its own tables at graph build, so this job task maintains
them from outside the pipeline, after every run.

Normal mode is idempotent: it appends only the entries bronze has that the
ledger lacks and recomputes the fingerprints of the dates that changed. With
--bronze-rebuild true (the run after a bronze_rebuild full refresh) the rows of
each table are deleted and rebuilt from bronze, and the hashed column set is
recorded in <catalog>.ledger.row_hash_columns -- only then, and only when
bronze's columns equal the active source_columns: bronze.py refuses normal runs
when that record is missing or differs from source_columns.

The task reads bronze, so it reflects reality even after a partly failed
pipeline update (run it with run_if ALL_DONE).
"""

import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from metadata import (
    ROW_HASH_COL,
    bronze_entries_df,
    bronze_source_columns,
    build_column_list,
    ensure_ledger_table,
    fingerprint_table_name,
    fingerprints_df,
    is_incremental,
    pg_conn_from_job_secret,
    read_source_columns,
    read_table_defs,
    row_hash_spec_table_name,
)

KEY_COLUMNS = ["table_name", "business_key", "snapshot_date", "row_hash"]


def refresh_fingerprints(spark, ledger: str, fp_table: str, table: str, dates) -> int:
    """Recompute the fingerprints of `dates` (all dates when None) from the ledger entries and merge them in."""
    entries = (
        spark.table(ledger)
        .where(f"table_name = '{table}'")
        .select(F.col("business_key").alias("k"), F.col("snapshot_date").alias("d"), F.col("row_hash").alias("h"))
    )
    if dates is not None:
        entries = entries.where(F.col("d").isin(list(dates)))
    fp = fingerprints_df(spark, entries).withColumn("table_name", F.lit(table)).select(
        "table_name", "snapshot_date", "row_count", "fp_sum"
    )
    fp.createOrReplaceTempView("_fp_new")
    spark.sql(
        f"MERGE INTO {fp_table} t USING _fp_new n ON t.table_name = n.table_name AND t.snapshot_date = n.snapshot_date "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    return fp.count()


def record_hashed_columns(spark, spec_table: str, table: str, columns: list[str]) -> None:
    """Record (replace) the column set bronze of `table` is hashed over."""
    spark.createDataFrame([(table, json.dumps(columns))], "table_name STRING, columns STRING").withColumn(
        "recorded_at", F.current_timestamp()
    ).createOrReplaceTempView("_spec_new")
    spark.sql(
        f"MERGE INTO {spec_table} t USING _spec_new n ON t.table_name = n.table_name "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--meta-pg-host", required=True)
    parser.add_argument("--meta-pg-user", required=True)
    parser.add_argument("--meta-pg-key", required=True)
    parser.add_argument("--bronze-rebuild", default="false", help='"true" rebuilds the ledger from bronze')
    args = parser.parse_args()
    rebuild = args.bronze_rebuild.strip().lower() == "true"

    spark = SparkSession.builder.getOrCreate()
    meta_conn = pg_conn_from_job_secret(args.secret_scope, args.meta_pg_key, args.meta_pg_host, args.meta_pg_user)
    ledger = ensure_ledger_table(spark, args.catalog)
    fp_table = fingerprint_table_name(args.catalog)
    spec_table = row_hash_spec_table_name(args.catalog)
    columns_by_table = read_source_columns(spark, meta_conn)

    for table_def in read_table_defs(spark, meta_conn):
        if not is_incremental(table_def):
            continue
        table = table_def["source_table"]
        bronze_name = f"{args.catalog}.poc_bronze.{table}"
        if not spark.catalog.tableExists(bronze_name):
            print(f"WARN {table}: bronze table does not exist yet, ledger left unchanged")
            continue
        bronze_columns = [f.name for f in spark.table(bronze_name).schema.fields]
        if ROW_HASH_COL not in bronze_columns:
            print(f"WARN {table}: bronze has no {ROW_HASH_COL} (not rebuilt since rows are keyed by content), "
                  "ledger left unchanged; run a bronze rebuild")
            continue
        bronze = bronze_entries_df(spark, args.catalog, table_def)
        if rebuild:
            spark.sql(f"DELETE FROM {ledger} WHERE table_name = '{table}'")
            spark.sql(f"DELETE FROM {fp_table} WHERE table_name = '{table}'")
            new = bronze
        else:
            existing = spark.table(ledger).where(f"table_name = '{table}'")
            new = bronze.join(existing, KEY_COLUMNS, "left_anti")
        new = new.select(*KEY_COLUMNS)
        added = new.count()
        affected = None
        if added:
            affected = [r["snapshot_date"] for r in new.select("snapshot_date").distinct().collect()]
            new.write.mode("append").saveAsTable(ledger)
        have_fp = spark.table(fp_table).where(f"table_name = '{table}'").limit(1).count() > 0
        if rebuild or not have_fp:
            dates = None  # first run after the fingerprint table was introduced, or a rebuild: all dates
        else:
            dates = affected
        refreshed = refresh_fingerprints(spark, ledger, fp_table, table, dates) if (dates is None or dates) else 0
        print(f"{table}: {'rebuilt' if rebuild else 'appended'} {added} ledger entries, refreshed {refreshed} date fingerprints")
        if rebuild:
            hashed = bronze_source_columns(bronze_columns)
            active = build_column_list(columns_by_table.get(table_def["table_id"], []))
            if hashed == active:
                record_hashed_columns(spark, spec_table, table, active)
                print(f"{table}: recorded hashed columns {active}")
            else:
                print(f"WARN {table}: bronze columns {hashed} differ from the active source_columns {active}; "
                      "hashed column set NOT recorded, so normal runs stay blocked until a rebuild matches")


if __name__ == "__main__":
    main()
