"""Job task: after netsuite_ingestion_poc's pipeline task, log one
aidq_metadata.run_audit row per (source table, layer) for this job run.

Row counts are snapshot counts of the poc_bronze/poc_silver/poc_reject
catalog tables as they stand right after the run, not per-flow execution
metrics -- event_log(pipeline_id) for this pipeline carries only executor
timing (executor_time_ms/executor_cpu_time_ms), no num_output_rows /
num_input_rows, so there's no API to isolate "rows touched this run" from
Lakeflow directly (confirmed by inspecting the event log directly). This is
exact for bronze and poc_reject.rejected_rows (both fully overwritten every
run -- see bronze.py / dq.py), but for Silver -- an incremental SCD1 merge
-- rows_written is the table's current total size, not this run's upsert
delta specifically. See self-review notes.

Layers logged per source table (table_id from aidq_metadata.source_table_def):
  bronze -- rows_read = rows_written = current row count of the source's one
            bronze table, poc_bronze.<table>
  dq     -- rows_read = same bronze total; rows_rejected = current count in
            poc_reject.rejected_rows for that source_table; rows_written =
            rows_read - rows_rejected (rows that fed Silver)
  silver -- only for tables with a business_key + watermark_col (see
            metadata.has_merge_keys); rows_read = dq's rows_written;
            rows_written = current row count in poc_silver.<table>

Plus one pipeline-wide row (table_id NULL):
  guard  -- rows_read = event-log reads the full-refresh guard needed in this
            run's pipeline update (from poc_bronze.guard_reads); status OK when
            the event was found on the first read, WARN otherwise (see
            metadata.guard_audit_row). Also printed to the task output.

If the run_pipeline task did not succeed, one row per (table, layer) is
still written with status=FAILED and the task's error message instead of
row counts (the catalog state may not reflect a completed run).

Run as a Databricks Jobs spark_python_task -- WorkspaceClient() picks up the
job's run-as identity automatically, no profile/token needed.
"""

import argparse
import base64
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

from metadata import (
    GUARD_READS_TABLE,
    PgConn,
    guard_audit_row,
    has_merge_keys,
    pick_update_id,
    read_table_defs,
    write_pg_rows,
)

RUN_AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("table_id", IntegerType()),
        StructField("layer", StringType()),
        StructField("status", StringType()),
        StructField("rows_read", LongType()),
        StructField("rows_written", LongType()),
        StructField("rows_rejected", LongType()),
        StructField("started_at", TimestampType()),
        StructField("ended_at", TimestampType()),
        StructField("error", StringType()),
    ]
)


def _run_pipeline_task(w: WorkspaceClient, run_id: int, task_key: str):
    run = w.jobs.get_run(run_id=run_id)
    for t in run.tasks or []:
        if t.task_key == task_key:
            return t
    raise ValueError(f"task_key {task_key!r} not found in job run {run_id}")


def _to_utc(ms):
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _bronze_row_count(spark, catalog: str, source_table: str) -> int:
    """Rows in the source's ONE bronze table (poc_bronze.<table>). The table name is exact:
    no LIKE matching, so leftover objects can no longer inflate the count."""
    return spark.table(f"{catalog}.poc_bronze.{source_table}").count()


def _reject_counts_by_table(spark, catalog: str) -> dict[str, int]:
    """source_table -> rejected row count, in one aggregate scan of
    poc_reject.rejected_rows rather than one filtered scan per source table."""
    rows = spark.table(f"{catalog}.poc_reject.rejected_rows").groupBy("source_table").count().collect()
    return {r["source_table"]: r["count"] for r in rows}


def _silver_row_count(spark, catalog: str, source_table: str) -> int:
    return spark.table(f"{catalog}.poc_silver.{source_table}").count()


def _success_rows(spark, catalog: str, table_defs: list[dict], run_id: str, started_at, ended_at) -> list[dict]:
    reject_counts = _reject_counts_by_table(spark, catalog)
    rows = []
    for td in table_defs:
        source_table = td["source_table"]

        bronze_count = _bronze_row_count(spark, catalog, source_table)
        rows.append(
            {
                "run_id": run_id,
                "table_id": td["table_id"],
                "layer": "bronze",
                "status": "SUCCESS",
                "rows_read": bronze_count,
                "rows_written": bronze_count,
                "rows_rejected": None,
                "started_at": started_at,
                "ended_at": ended_at,
                "error": None,
            }
        )

        rejected_count = reject_counts.get(source_table, 0)
        valid_count = bronze_count - rejected_count
        rows.append(
            {
                "run_id": run_id,
                "table_id": td["table_id"],
                "layer": "dq",
                "status": "SUCCESS",
                "rows_read": bronze_count,
                "rows_written": valid_count,
                "rows_rejected": rejected_count,
                "started_at": started_at,
                "ended_at": ended_at,
                "error": None,
            }
        )

        if has_merge_keys(td):
            silver_count = _silver_row_count(spark, catalog, source_table)
            rows.append(
                {
                    "run_id": run_id,
                    "table_id": td["table_id"],
                    "layer": "silver",
                    "status": "SUCCESS",
                    "rows_read": valid_count,
                    "rows_written": silver_count,
                    "rows_rejected": None,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "error": None,
                }
            )
    return rows


def _pipeline_update_id(w: WorkspaceClient, task):
    pipeline_id = task.pipeline_task.pipeline_id if task.pipeline_task else None
    if not pipeline_id:
        return None
    resp = w.pipelines.list_updates(pipeline_id=pipeline_id, max_results=25)
    updates = [(u.update_id, u.creation_time) for u in (resp.updates or [])]
    return pick_update_id(updates, task.start_time, task.end_time)


def _guard_row(spark, w, catalog: str, task, run_id: str, started_at, ended_at) -> dict:
    """run_audit `guard` row: event-log reads the bronze guard needed in this run's pipeline update."""
    try:
        update_id = _pipeline_update_id(w, task)
    except Exception as exc:  # never fail the audit over the guard row
        print(f"WARN guard: could not list pipeline updates: {type(exc).__name__}: {str(exc)[:200]}")
        update_id = None
    table = f"{catalog}.{GUARD_READS_TABLE}"
    guard = None
    if spark.catalog.tableExists(table):
        rows = spark.table(table).orderBy("recorded_at", ascending=False).limit(1).collect()
        guard = rows[0].asDict() if rows else None
    row = guard_audit_row(guard, update_id, run_id, started_at, ended_at)
    print(f"guard: {row['status']} reads={row['rows_read']} {row['error'] or ''}".rstrip())
    return row


def _failure_rows(table_defs: list[dict], run_id: str, started_at, ended_at, error: str) -> list[dict]:
    rows = []
    for td in table_defs:
        layers = ["bronze", "dq"] + (["silver"] if has_merge_keys(td) else [])
        for layer in layers:
            rows.append(
                {
                    "run_id": run_id,
                    "table_id": td["table_id"],
                    "layer": layer,
                    "status": "FAILED",
                    "rows_read": None,
                    "rows_written": None,
                    "rows_rejected": None,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "error": error,
                }
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--meta-pg-host", required=True)
    parser.add_argument("--meta-pg-user", required=True)
    parser.add_argument("--meta-pg-key", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--pipeline-task-key", default="run_pipeline")
    args = parser.parse_args()

    w = WorkspaceClient()
    spark = SparkSession.builder.getOrCreate()

    token_b64 = w.secrets.get_secret(scope=args.secret_scope, key=args.meta_pg_key).value
    token = base64.b64decode(token_b64).decode("utf-8")
    meta_conn = PgConn(host=args.meta_pg_host, user=args.meta_pg_user, token=token)

    table_defs = read_table_defs(spark, meta_conn)

    task = _run_pipeline_task(w, args.run_id, args.pipeline_task_key)
    started_at = _to_utc(task.start_time)
    ended_at = _to_utc(task.end_time)
    result_state = task.state.result_state if task.state else None
    run_id_str = str(args.run_id)

    if result_state == RunResultState.SUCCESS:
        rows = _success_rows(spark, args.catalog, table_defs, run_id_str, started_at, ended_at)
    else:
        error = (task.state.state_message if task.state else None) or f"run_pipeline task result_state={result_state}"
        rows = _failure_rows(table_defs, run_id_str, started_at, ended_at, error)
    rows.append(_guard_row(spark, w, args.catalog, task, run_id_str, started_at, ended_at))

    write_pg_rows(spark, meta_conn, "aidq_metadata", "run_audit", RUN_AUDIT_SCHEMA, rows)
    print(f"Logged {len(rows)} run_audit rows for job run {args.run_id}")


if __name__ == "__main__":
    main()
