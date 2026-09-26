"""Bronze layer: land each source table into poc_bronze.<table>.

Reads go straight over JDBC to the Lakebase Postgres projects (see
metadata.PgConn / read_jdbc_table) rather than through Unity Catalog
Lakehouse Federation -- there is no UC connection/foreign catalog involved.

Write behavior is driven by source_table_def (a blank watermark_col means
FullLoad, a non-blank one means Incremental):

  - FullLoad tables (netsuite_customers): a single batch table, fully
    recomputed/overwritten every run (@dp.table, full read).

  - Incremental tables: ONE append-only streaming table per source,
    poc_bronze.<table>, with `_snapshot_date` (the watermark column's date) and
    `_loaded_at`. It is fed by `once` append flows (metadata.plan_flows):
      * a snapshot flow per distinct watermark date at or above the floor
        (source_table_def.bronze_watermark). Names come from the date, so a
        date that already loaded is never re-extracted;
      * a top-up flow for (business_key, date) pairs the source has but the
        key ledger does not, on an already-loaded date (late rows). Postgres
        first returns one fingerprint (pair count + sum of md5-derived bigints)
        per date; pairs are fetched only for dates whose fingerprint differs
        from the ledger's. Named from a hash of the pending key set.
    Silver therefore always has a single streaming source.

The key ledger (<catalog>.ledger.bronze_keys, `ledger_table` configuration)
is a Delta table OUTSIDE this pipeline, maintained by the sync_ledger job
task: a pipeline cannot read its own tables at graph build or inside a flow.

Operating rules
  - Normal runs: never a full refresh.
  - A full refresh rebuilds bronze from the source and MUST be run with the
    `bronze_rebuild` configuration set to "true": the ledger is then ignored,
    every date gets a flow and top-ups are off. This is enforced: the graph
    build reads the update's `create_update` event from the pipeline event log
    and raises if it is a full refresh (or a selection naming a bronze table)
    without bronze_rebuild.
  - `flow_scope` defaults to "pending_only": flows are defined only for dates
    missing from the ledger (plus top-ups), because defining a flow for every
    date costs ~0.45 s at graph planning (1,600 flows = ~12 min).
  - Never DROP a poc_bronze table outside Lakeflow: streaming readers (silver)
    would fail with [DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE] and only a
    full refresh recovers.
"""

import datetime

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from metadata import (
    DEFAULT_SCOPE,
    bronze_table_name,
    build_column_list,
    collect_plan_inputs,
    floor_date,
    is_incremental,
    key_filter,
    pg_conn_from_conf,
    plan_flows,
    read_jdbc_table,
    read_create_update_detail,
    refresh_guard_error,
    read_source_columns,
    read_table_defs,
)

source_conn = pg_conn_from_conf(spark, dbutils, "source")
meta_conn = pg_conn_from_conf(spark, dbutils, "meta")
bronze_rebuild = (spark.conf.get("bronze_rebuild", "false") or "false").strip().lower() == "true"
flow_scope = (spark.conf.get("flow_scope", DEFAULT_SCOPE) or DEFAULT_SCOPE).strip()
ledger_table = spark.conf.get("ledger_table")
ledger_fingerprint_table = spark.conf.get("ledger_fingerprint_table")

table_defs = read_table_defs(spark, meta_conn)
columns_by_table = read_source_columns(spark, meta_conn)

# Hard guard: a full refresh of bronze is only allowed with bronze_rebuild=true (see metadata.refresh_guard_error).
_create_update, _create_update_note = read_create_update_detail(spark)
_guard_error = refresh_guard_error(
    _create_update,
    [bronze_table_name(t["source_table"]) for t in table_defs if is_incremental(t)],
    bronze_rebuild,
    flow_scope,
    {
        "target": spark.conf.get("bundle_target", None),
        "update_id": spark.conf.get("spark.pipelines.updateId", None),
        "pipeline": spark.conf.get("pipelines.id", "netsuite_ingestion_poc"),
        "note": _create_update_note,
    },
)
if _guard_error:
    raise RuntimeError(_guard_error)


def _register_full_load_bronze(table_def: dict, column_list: list[str]) -> None:
    source_table = table_def["source_table"]
    source_schema = table_def["dest_schema"]
    target_name = bronze_table_name(source_table)

    @dp.table(
        name=target_name,
        comment=f"Bronze (FullLoad): {source_conn.host}/{source_schema}.{source_table}, overwritten every run",
        table_properties={"delta.enableChangeDataFeed": "true"},
    )
    def _bronze_table():
        return read_jdbc_table(spark, source_conn, source_schema, source_table).select(*column_list)


def _register_extract_flow(target, spec, source_schema, source_table, watermark_col, key_col, column_list) -> None:
    """One `once` append flow. A factory so each loop iteration's spec is captured correctly."""
    day = spec.snapshot_date
    next_day = day + datetime.timedelta(days=1)

    @dp.append_flow(target=target, name=spec.name, once=True)
    def _flow():
        df = read_jdbc_table(spark, source_conn, source_schema, source_table).where(
            (F.col(watermark_col) >= F.lit(day)) & (F.col(watermark_col) < F.lit(next_day))
        )
        if spec.keys:  # top-up: only the pending keys of an already-loaded date
            df = df.where(key_filter(key_col, spec.keys))
        if spec.kind == "anchor":  # placeholder so the streaming table always has a flow; appends nothing
            df = df.where(F.lit(False))
        return (
            df.select(*column_list)
            .withColumn("_snapshot_date", F.lit(day))
            .withColumn("_loaded_at", F.current_timestamp())
        )


def _register_incremental_bronze(table_def: dict, column_list: list[str]) -> None:
    source_table = table_def["source_table"]
    source_schema = table_def["dest_schema"]
    watermark_col = table_def["watermark_col"].strip()
    key_col = table_def["business_key"].strip()
    target = bronze_table_name(source_table)

    dp.create_streaming_table(
        name=target,
        comment=(
            f"Bronze (Incremental): {source_conn.host}/{source_schema}.{source_table}, append-only, "
            f"one once-flow per {watermark_col} date (floor {floor_date(table_def)})"
        ),
    )

    src_dates, ledger_dates, topups = collect_plan_inputs(
        spark, source_conn, table_def, ledger_table, ledger_fingerprint_table, rebuild=bronze_rebuild
    )

    for spec in plan_flows(source_table, src_dates, ledger_dates, topups, flow_scope, bronze_rebuild):
        _register_extract_flow(target, spec, source_schema, source_table, watermark_col, key_col, column_list)


def _register_bronze_table(table_def: dict, columns: list[dict]) -> None:
    column_list = build_column_list(columns)
    if is_incremental(table_def):
        _register_incremental_bronze(table_def, column_list)
    else:
        _register_full_load_bronze(table_def, column_list)


for _table_def in table_defs:
    _register_bronze_table(_table_def, columns_by_table.get(_table_def["table_id"], []))
