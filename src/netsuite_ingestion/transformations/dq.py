"""DQ layer: split each source table's bronze data into valid vs. rejected.

Every source table has exactly ONE bronze table (poc_bronze.<table>, see
bronze.py). For each source table this builds a validity predicate from its
HARD data_quality_rules (true when there are none -- DQ rules are opt-in per
table, not mandatory). Passing rows become the streaming view "<table>_valid"
(feeds Silver). Failing rows are collected into poc_reject.rejected_rows.

HARD rules are NULL-safe: a rule that evaluates to NULL fails (metadata.null_safe), so every bronze row is
either valid or rejected (with that rule named in `reason`); none is silently dropped.

SOFT rules never reject rows: they are attached to "<table>_valid" as
dp.expect_all expectations, so violations show up as event-log metrics only.

poc_reject.rejected_rows is a single batch table (full recompute every run,
unioning every source table's rejects) rather than a streaming table fed by
per-table append flows: simpler, and it never re-appends duplicate reject rows.

"<table>_valid" is a streaming view because Silver's create_auto_cdc_flow
requires a streaming source. For Incremental tables bronze is an append-only
streaming table, so a plain streaming read is correct across runs. The
FullLoad table's bronze is a batch table that is overwritten every run, so its
view reads with skipChangeCommits=true (nothing consumes it today: customers
has no Silver table).

Columns of rejected_rows: reason, run_id, ts, payload, plus source_table so
rows from different tables in the shared reject table can be told apart.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metadata import (
    RUN_ID,
    bronze_table_name,
    build_column_list,
    build_dq_predicate,
    build_dq_reason_expr,
    build_soft_expectations,
    is_incremental,
    pg_conn_from_conf,
    read_dq_rules,
    read_source_columns,
    read_table_defs,
)

meta_conn = pg_conn_from_conf(spark, dbutils, "meta")

table_defs = read_table_defs(spark, meta_conn)
columns_by_table = read_source_columns(spark, meta_conn)
rules_by_table = read_dq_rules(spark, meta_conn)


def _read_bronze(table_def: dict) -> DataFrame:
    return spark.read.table(bronze_table_name(table_def["source_table"]))


def _read_bronze_streaming(table_def: dict) -> DataFrame:
    reader = spark.readStream
    if not is_incremental(table_def):  # batch table overwritten every run
        reader = reader.option("skipChangeCommits", "true")
    return reader.table(bronze_table_name(table_def["source_table"]))


def _rejects_for_table(table_def: dict, columns: list[dict], rules: list[dict]) -> DataFrame:
    source_table = table_def["source_table"]
    column_list = build_column_list(columns)
    predicate = build_dq_predicate(rules)
    reason_expr = build_dq_reason_expr(rules)

    return (
        _read_bronze(table_def)
        .where(f"NOT ({predicate})")
        .withColumn("reason", F.expr(reason_expr))
        .withColumn("run_id", F.lit(RUN_ID))
        .withColumn("ts", F.current_timestamp())
        .withColumn("source_table", F.lit(source_table))
        .withColumn("payload", F.to_json(F.struct(*column_list)))
        .select("reason", "run_id", "ts", "source_table", "payload")
    )


def _register_valid_view(table_def: dict, rules: list[dict]) -> None:
    source_table = table_def["source_table"]
    predicate = build_dq_predicate(rules)
    soft_expectations = build_soft_expectations(rules)

    def _valid_view():
        return _read_bronze_streaming(table_def).where(predicate)

    # SOFT rules are wired as expectations (@dp.expect semantics: violations
    # are recorded in the event log, rows are kept). They are evaluated on
    # rows that already passed the HARD predicate.
    if soft_expectations:
        _valid_view = dp.expect_all(soft_expectations)(_valid_view)
    dp.temporary_view(name=f"{source_table}_valid")(_valid_view)


for _table_def in table_defs:
    _register_valid_view(_table_def, rules_by_table.get(_table_def["table_id"], []))


@dp.table(
    name="poc_reject.rejected_rows",
    comment="Rows that failed a HARD data-quality rule, across all source tables",
)
def rejected_rows():
    per_table_rejects = [
        _rejects_for_table(td, columns_by_table.get(td["table_id"], []), rules_by_table.get(td["table_id"], []))
        for td in table_defs
    ]
    return reduce(lambda a, b: a.unionByName(b), per_table_rejects)
