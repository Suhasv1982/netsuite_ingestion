"""Job task: keep the bronze key ledger and its per-date fingerprints in step with what bronze holds.

The ledger (<catalog>.ledger.bronze_keys) records the distinct
(table_name, business_key, snapshot_date) pairs loaded into each incremental
poc_bronze table. <catalog>.ledger.bronze_fingerprints stores, per
(table_name, snapshot_date), the fingerprint bronze.py compares with the one
Postgres computes for the source: the count of distinct pairs and the sum of
their md5-derived bigints (see metadata.py). bronze.py reads both at graph
build to find late rows -- a pipeline cannot read its own tables, so this job
task maintains them from outside the pipeline, after every run.

Normal mode is idempotent: it appends only the pairs bronze has that the
ledger lacks and recomputes the fingerprints of the dates that changed. With
--bronze-rebuild true (the run after a bronze_rebuild full refresh) the rows of
each table are deleted and rebuilt from bronze.

The task reads bronze, so it reflects reality even after a partly failed
pipeline update (run it with run_if ALL_DONE).
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from metadata import (
    bronze_pairs_df,
    ensure_ledger_table,
    fingerprint_table_name,
    fingerprints_df,
    is_incremental,
    pg_conn_from_job_secret,
    read_table_defs,
)

KEY_COLUMNS = ["table_name", "business_key", "snapshot_date"]


def refresh_fingerprints(spark, ledger: str, fp_table: str, table: str, dates) -> int:
    """Recompute the fingerprints of `dates` (all dates when None) from the ledger pairs and merge them in."""
    pairs = (
        spark.table(ledger)
        .where(f"table_name = '{table}'")
        .select(F.col("business_key").alias("k"), F.col("snapshot_date").alias("d"))
    )
    if dates is not None:
        pairs = pairs.where(F.col("d").isin(list(dates)))
    fp = fingerprints_df(spark, pairs).withColumn("table_name", F.lit(table)).select(
        "table_name", "snapshot_date", "row_count", "fp_sum"
    )
    fp.createOrReplaceTempView("_fp_new")
    spark.sql(
        f"MERGE INTO {fp_table} t USING _fp_new n ON t.table_name = n.table_name AND t.snapshot_date = n.snapshot_date "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    return fp.count()


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

    for table_def in read_table_defs(spark, meta_conn):
        if not is_incremental(table_def):
            continue
        table = table_def["source_table"]
        if not spark.catalog.tableExists(f"{args.catalog}.poc_bronze.{table}"):
            print(f"WARN {table}: bronze table does not exist yet, ledger left unchanged")
            continue
        bronze = bronze_pairs_df(spark, args.catalog, table_def)
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
        print(f"{table}: {'rebuilt' if rebuild else 'appended'} {added} ledger pairs, refreshed {refreshed} date fingerprints")


if __name__ == "__main__":
    main()
