"""Metadata-driven building blocks for the netsuite_ingestion pipeline.

Reads the control tables in the aidq_metadata catalog (source_table_def,
source_columns, data_quality_rules) and turns each table's metadata rows
into the pieces the bronze/dq/silver transformation files need: a column
list, a DQ validity predicate, and a DQ failure-reason expression.

The build_* functions are plain Python (no Spark dependency) so they can be
unit tested without a Spark session -- see tests/test_metadata.py.
"""

import datetime
import hashlib
import json
import re
import uuid
from dataclasses import dataclass

# One id per pipeline graph build (i.e. per pipeline update/run), shared by
# every transformation file that imports this module.
RUN_ID = str(uuid.uuid4())

@dataclass(frozen=True)
class PgConn:
    """Connection info for one Lakebase Postgres project, read via plain
    JDBC rather than Lakehouse Federation (no UC connection/foreign catalog
    involved -- see the self-review notes on why).

    `token` is a short-lived (~1hr) OAuth database credential, expected to
    be supplied through the pipeline's `configuration` block backed by a
    Databricks secret -- never hardcoded into bundle files.
    """

    host: str
    user: str
    token: str
    database: str = "databricks_postgres"


def _jdbc_url(conn: PgConn) -> str:
    return f"jdbc:postgresql://{conn.host}:5432/{conn.database}?sslmode=require"


def read_jdbc_table(spark, conn: PgConn, schema: str, table: str):
    """Batch-read one Postgres table over JDBC (no UC federation)."""
    return (
        spark.read.format("jdbc")
        .option("url", _jdbc_url(conn))
        .option("user", conn.user)
        .option("password", conn.token)
        .option("dbtable", f'"{schema}"."{table}"')
        .load()
    )


def write_pg_rows(spark, conn: PgConn, schema: str, table: str, rows_schema, rows: list[dict]) -> None:
    """Append `rows` to a Postgres table using Spark's native `postgresql`
    data source (write-side twin of read_jdbc_table, which reads via plain
    `jdbc`). Serverless compute rejects `jdbc`-format writes outright
    (UNSUPPORTED_DATA_SOURCE_WRITE -- `jdbc` isn't on the serverless DML
    allowlist, `postgresql` is) and only allows a fixed write-option set
    (no `sslmode`; host/port/database instead of a JDBC url) -- confirmed
    directly against a serverless session. `rows_schema` is an explicit
    pyspark StructType so column types match the target table regardless
    of which fields happen to be None in `rows` -- used by log_run_audit.py
    to write run_audit."""
    df = spark.createDataFrame(rows, schema=rows_schema)
    (
        df.write.format("postgresql")
        .option("host", conn.host)
        .option("port", "5432")
        .option("database", conn.database)
        .option("user", conn.user)
        .option("password", conn.token)
        .option("dbtable", f'"{schema}"."{table}"')
        .mode("append")
        .save()
    )


def pg_conn_from_conf(spark, dbutils, prefix: str, secret_scope: str | None = None) -> PgConn:
    """Build a PgConn from pipeline configuration entries named
    <prefix>_pg_host / <prefix>_pg_user / <prefix>_pg_database, plus the
    OAuth token read directly from a Databricks secret scope: `secret_scope`, else the
    `secret_scope` configuration entry (per target: netsuite_ingestion_dev / the prod scope), else
    "netsuite_ingestion_poc" (key "<prefix>_pg_token", or the value of the optional configuration entry
    <prefix>_pg_token_key -- dev and prod use different keys so that one target's
    refresh_credentials task can never overwrite the other's token).

    NOTE: the token is fetched via dbutils.secrets.get(), not
    spark.conf.get() -- Lakeflow pipeline `configuration` values do not
    resolve `{{secrets/scope/key}}` references (that substitution only
    applies in specific other contexts, e.g. cluster env vars); a
    configuration value referencing a secret arrives as the literal
    unresolved "{{secrets/...}}" string. See the self-review notes.
    """
    return PgConn(
        host=spark.conf.get(f"{prefix}_pg_host"),
        user=spark.conf.get(f"{prefix}_pg_user"),
        token=dbutils.secrets.get(
            scope=secret_scope or spark.conf.get("secret_scope", "netsuite_ingestion_poc"), key=spark.conf.get(f"{prefix}_pg_token_key", f"{prefix}_pg_token")
        ),
        database=spark.conf.get(f"{prefix}_pg_database", "databricks_postgres"),
    )


def _blank_to_none(value) -> str | None:
    """`(value or "").strip()`, or None when nothing is left -- turns a NULL
    or whitespace-only metadata value into a real NULL."""
    return (value or "").strip() or None


def read_table_defs(spark, meta_conn: PgConn) -> list[dict]:
    """One row per source table, from aidq_metadata.source_table_def.

    A table with no watermark (a FullLoad) has watermark_col NULL, or an empty
    string on a database without migration 003; both are normalized to None here.
    """
    df = read_jdbc_table(spark, meta_conn, "aidq_metadata", "source_table_def")
    rows = [row.asDict() for row in df.collect()]
    for row in rows:
        row["watermark_col"] = _blank_to_none(row.get("watermark_col"))
    return rows


def _group_by_table_id(rows: list[dict]) -> dict[int, list[dict]]:
    grouped: dict[int, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["table_id"], []).append(row)
    return grouped


def read_source_columns(spark, meta_conn: PgConn) -> dict[int, list[dict]]:
    """table_id -> list of source_columns rows for that table."""
    df = read_jdbc_table(spark, meta_conn, "aidq_metadata", "source_columns")
    return _group_by_table_id([row.asDict() for row in df.collect()])


def read_dq_rules(spark, meta_conn: PgConn) -> dict[int, list[dict]]:
    """table_id -> list of ACTIVE data_quality_rules rows for that table."""
    df = read_jdbc_table(spark, meta_conn, "aidq_metadata", "data_quality_rules")
    return _group_by_table_id(filter_active_rules([row.asDict() for row in df.collect()]))


def filter_active_rules(rules: list[dict]) -> list[dict]:
    """Drop rules with is_active = false (migration 001 adds the column, default true).

    A row without the key, or with NULL, counts as active, so this is a no-op on a
    metadata database that has not had migration 001 applied yet -- the code can be
    deployed before or after the migration. Same rule as build_column_list.
    """
    return [r for r in rules if r.get("is_active") is None or bool(r.get("is_active"))]


# --------------------------------------------------------------------------
# Incremental bronze
#
# One streaming table per incremental source (poc_bronze.<table>), fed by
# `once` append flows:
#   * a "snapshot" flow per distinct watermark DATE (updated_date) at or above
#     the floor stored in source_table_def.bronze_watermark. Flow names are
#     derived from the date, so a flow that already ran is never run again
#     (verified on the runtime: baseline/once_flow_experiments.md).
#   * a "topup" flow for source (business_key, date) pairs that are missing
#     from the key ledger although their date is already loaded (late rows).
#     Named from a hash of the pending key set, so a stale ledger reproduces
#     the same name and the flow does not run twice.
#
# The ledger (<catalog>.ledger.bronze_keys) is a plain Delta table OUTSIDE the
# pipeline, keyed on (table_name, business_key, snapshot_date). A pipeline
# cannot read its own tables at graph build or inside a flow, so the ledger is
# maintained by the sync_ledger job task from what bronze really holds.
# --------------------------------------------------------------------------

DEFAULT_FLOOR = datetime.date(1900, 1, 1)
NULL_KEY = "<NULL>"  # how a NULL business key is written in the ledger / pending sets
DEFAULT_MAX_PENDING_KEYS = 200_000
SCOPES = ("all_dates", "pending_only")
DEFAULT_SCOPE = "pending_only"  # 1,600 once-flows cost ~12 min of planning per update; pending_only ~1 min (baseline/once_flow_experiments.md)


class TooManyPendingKeys(Exception):
    """More late (key, date) pairs than the graph-build cap: fail loudly instead of collecting them all."""


def is_incremental(table_def: dict) -> bool:
    """A blank watermark_col means FullLoad; a non-blank one means Incremental (see source_table_def.load_mode)."""
    return (table_def.get("load_mode") or "").strip().lower() == "incremental"


def key_filter(key_col: str, keys):
    """Spark condition selecting the given business keys (strings; NULL_KEY matches NULL)."""
    from pyspark.sql import functions as F

    real = [k for k in keys if k != NULL_KEY]
    cond = F.col(key_col).cast("string").isin(real) if real else F.lit(False)
    if NULL_KEY in keys:
        cond = cond | F.col(key_col).isNull()
    return cond


def bronze_table_name(source_table: str) -> str:
    """poc_bronze.<table>: a FullLoad table's batch table, or an incremental table's streaming table."""
    return f"poc_bronze.{source_table}"


def floor_date(table_def: dict) -> datetime.date:
    """Start date for the first load, from source_table_def.bronze_watermark (ISO YYYY-MM-DD).

    Blank/NULL means no floor (1900-01-01). Anything else raises: a silently
    misparsed floor would skip or reload data.
    """
    raw = _blank_to_none(table_def.get("bronze_watermark"))
    if raw is None:
        return DEFAULT_FLOOR
    try:
        return datetime.date.fromisoformat(raw)
    except ValueError:
        raise ValueError(
            f"bronze_watermark {raw!r} for {table_def.get('source_table')!r} is not an ISO date (YYYY-MM-DD)"
        ) from None


def ledger_key(value) -> str:
    return NULL_KEY if value is None else str(value)


def snapshot_flow_name(source_table: str, day: datetime.date) -> str:
    return f"{source_table}__{day:%Y%m%d}"


def pending_key_hash(keys) -> str:
    """Short, order-independent hash of a pending key set."""
    joined = "\n".join(sorted(ledger_key(k) for k in keys))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:10]


def topup_flow_name(source_table: str, day: datetime.date, keys) -> str:
    return f"{source_table}__{day:%Y%m%d}__topup_{pending_key_hash(keys)}"


def anchor_flow_name(source_table: str) -> str:
    """Constant name of the empty placeholder flow (see plan_flows)."""
    return f"{source_table}__anchor"


@dataclass(frozen=True)
class FlowSpec:
    name: str
    snapshot_date: datetime.date
    kind: str  # "snapshot" | "topup" | "anchor"
    keys: tuple = ()  # topup only: the business keys (as strings) to extract for that date


def find_pending_pairs(source_pairs, ledger_pairs) -> set:
    """(key, date) pairs present in the source but not in the ledger.

    Compares pairs, never counts: an update can move one row out of a date and
    a late row into it, leaving the count unchanged. Pure model of the Spark
    anti-join in collect_plan_inputs().
    """
    ledger = {(ledger_key(k), d) for k, d in ledger_pairs}
    return {(ledger_key(k), d) for k, d in source_pairs} - ledger


def group_pending_by_date(pairs) -> dict:
    grouped: dict = {}
    for key, day in pairs:
        grouped.setdefault(day, []).append(key)
    return {day: sorted(keys) for day, keys in sorted(grouped.items())}


def ledger_diff(bronze_pairs, ledger_pairs) -> tuple[set, set]:
    """(missing_from_ledger, extra_in_ledger) between what bronze holds and the ledger."""
    bronze = {(ledger_key(k), d) for k, d in bronze_pairs}
    ledger = {(ledger_key(k), d) for k, d in ledger_pairs}
    return bronze - ledger, ledger - bronze


def enforce_pending_cap(count: int, cap: int = DEFAULT_MAX_PENDING_KEYS) -> None:
    if count > cap:
        raise TooManyPendingKeys(
            f"{count} late (key, date) pairs exceed the graph-build cap of {cap}; "
            "run a bronze_rebuild full refresh instead of a top-up"
        )


def plan_flows(
    source_table: str,
    src_dates,
    ledger_dates,
    topup_keys_by_date: dict,
    scope: str = DEFAULT_SCOPE,
    rebuild: bool = False,
) -> list[FlowSpec]:
    """Which `once` flows to define for one incremental table in this update.

    rebuild=True (only for a full refresh): a snapshot flow for every source
    date, the ledger is ignored and no top-ups are defined, because the base
    flows re-extract everything and a top-up would load the late rows twice.

    Otherwise:
      * scope "all_dates": a snapshot flow for every source date (loaded ones
        do not re-run because their names are unchanged);
      * scope "pending_only": snapshot flows only for dates not in the ledger;
      * plus one top-up flow per already-loaded date that has pending keys.

    A streaming table with no flow at all fails the update ("No query found for
    dataset", measured on the runtime), so when nothing else would be defined
    (nothing pending in "pending_only" scope, or an empty source) a single
    constant-named empty "anchor" flow is returned. Being a `once` flow it runs
    once, appends nothing, and never runs again.
    """
    if scope not in SCOPES:
        raise ValueError(f"unknown flow scope {scope!r}; expected one of {SCOPES}")
    src_dates = sorted(src_dates)
    if rebuild:
        flows = [FlowSpec(snapshot_flow_name(source_table, d), d, "snapshot") for d in src_dates]
        return flows or [_anchor(source_table)]

    flows = []
    for d in src_dates:
        if scope == "all_dates" or d not in ledger_dates:
            flows.append(FlowSpec(snapshot_flow_name(source_table, d), d, "snapshot"))
    in_source = set(src_dates)
    for d, keys in sorted(topup_keys_by_date.items()):
        if d in ledger_dates and d in in_source and keys:
            keys = tuple(sorted(keys))
            flows.append(FlowSpec(topup_flow_name(source_table, d, keys), d, "topup", keys))
    return flows or [_anchor(source_table)]


def _anchor(source_table: str) -> FlowSpec:
    return FlowSpec(anchor_flow_name(source_table), DEFAULT_FLOOR, "anchor")


def _selection_touches(entry: str, bronze_tables: list[str]) -> bool:
    entry = (entry or "").strip().strip("`")
    return any(entry == t or entry.endswith("." + t) or t.endswith("." + entry) for t in bronze_tables)


def _rebuild_commands(target: str) -> str:
    extra = ' --var="schedule_pause_status=PAUSED"' if target == "prod" else ""
    return (
        f'  1. databricks bundle deploy -t {target} --var="bronze_rebuild=true"{extra} --profile <PROFILE>\n'
        f"  2. databricks bundle run netsuite_ingestion_daily -t {target} --pipeline-params full_refresh=true --profile <PROFILE>\n"
        f'  3. databricks bundle deploy -t {target} --var="bronze_rebuild=false"{extra} --profile <PROFILE>   # back to normal runs'
    )


def refresh_guard_error(create_update, bronze_tables: list[str], rebuild: bool, scope: str, context: dict | None = None) -> str | None:
    """Hard guard for a full refresh of bronze; returns an error message, or None when the update is allowed.

    `create_update` is the current update's `create_update` event (see
    read_create_update): {"full_refresh": bool, "full_refresh_selection": [...]}.
    A full refresh (all tables, or a selection that names a bronze table)
    empties the bronze streaming tables, so it is only safe with
    bronze_rebuild=true, which defines a flow for every date. Measured on the
    runtime: a full refresh in "pending_only" scope without it left every
    bronze table empty.
    When the event cannot be read the guard fails closed for "pending_only"
    (the data-loss case) and lets "all_dates" through (it rebuilds everything
    anyway).

    `context` (optional) makes the message specific: {"target", "update_id", "pipeline"}.
    The message says what was blocked, why, and the exact commands to re-run.
    """
    ctx = context or {}
    target = ctx.get("target") or "<TARGET>"
    who = f"update {ctx['update_id']} of pipeline {ctx.get('pipeline', 'netsuite_ingestion_poc')}" if ctx.get("update_id") else "this pipeline update"

    if create_update is None:
        if scope == "pending_only":
            return (
                f"BLOCKED: {who} was stopped before it changed any data.\n"
                "WHY: its refresh mode could not be read from the pipeline event log, so a full refresh cannot be ruled out, "
                "and with flow_scope=pending_only a full refresh would leave the bronze tables empty.\n"
                + (f"REASON IT COULD NOT BE READ: {ctx['note']}\n" if ctx.get("note") else "")
                + "THIS CAN BE TRANSIENT (the event log is written asynchronously): first re-run the same update. If it repeats, "
                "either fix access to event_log('<pipeline id>') for the pipeline owner, or accept the slower planning of flow_scope=all_dates:\n"
                f'  databricks bundle deploy -t {target} --var="flow_scope=all_dates" --profile <PROFILE>'
            )
        return None
    selection = create_update.get("full_refresh_selection") or []
    hit = [x for x in selection if _selection_touches(x, bronze_tables)]
    touches_bronze = bool(create_update.get("full_refresh")) or bool(hit)
    if touches_bronze and not rebuild:
        what = "a full refresh of ALL tables" if create_update.get("full_refresh") else f"a full refresh of bronze table(s) {', '.join(hit)}"
        return (
            f"BLOCKED: {who} was stopped before it changed any data. It is {what} with bronze_rebuild=false.\n"
            "WHY: normal runs define once-flows only for dates missing from the key ledger (flow_scope=pending_only). "
            "A full refresh empties the bronze streaming tables first, so with only those flows bronze would be rebuilt "
            "from the pending dates and lose everything else (measured: every incremental bronze table ended with 0 rows).\n"
            "TO RE-RUN with bronze_rebuild=true (defines a flow for every source date, ignores the ledger, no top-ups):\n"
            + _rebuild_commands(target)
        )
    return None


_UUID = re.compile(r"[0-9a-fA-F-]{36}")


def read_create_update_detail(spark, attempts: int = 5, wait_seconds: float = 10.0):
    """(create_update event | None, note): see read_create_update_attempts, without the attempt count."""
    event, note, _ = read_create_update_attempts(spark, attempts, wait_seconds)
    return event, note


def read_create_update_attempts(spark, attempts: int = 5, wait_seconds: float = 10.0):
    """(create_update event | None, note, reads): the current update's `create_update` event, read from the
    pipeline's own event log at graph build (allowed: the event log is not a dataset of the pipeline).

    The event log is written asynchronously and one full-refresh update was seen to read back nothing at
    graph build although the event was there, so the read is retried a few times. When it still cannot be
    read, `note` says why (no row yet, or the query error) and is shown in the guard's message.
    `reads` is how many event-log reads were made (1 = found at once; 0 = not attempted), so a platform
    slowdown shows up in run_audit (layer `guard`) before it turns into blocked updates.
    """
    import time

    pipeline_id = spark.conf.get("pipelines.id", None)
    update_id = spark.conf.get("spark.pipelines.updateId", None)
    if not (pipeline_id and update_id and _UUID.fullmatch(pipeline_id) and _UUID.fullmatch(update_id)):
        return None, "pipelines.id / spark.pipelines.updateId are not set as expected", 0
    note = ""
    for attempt in range(1, attempts + 1):
        try:
            rows = spark.sql(
                f"SELECT details FROM event_log('{pipeline_id}') "
                f"WHERE event_type = 'create_update' AND origin.update_id = '{update_id}' ORDER BY timestamp DESC LIMIT 1"
            ).collect()
            if rows:
                return json.loads(rows[0]["details"]).get("create_update"), "", attempt
            note = f"no create_update event was visible for this update after {attempt} attempt(s)"
        except Exception as exc:
            note = f"event_log query failed on attempt {attempt}: {type(exc).__name__}: {str(exc)[:200]}"
        if attempt < attempts:
            time.sleep(wait_seconds)
    try:  # what the event log does contain right now, to diagnose why the event was not visible
        recent = spark.sql(
            f"SELECT event_type, substr(origin.update_id, 1, 8) AS u, date_format(timestamp, 'HH:mm:ss') AS t "
            f"FROM event_log('{pipeline_id}') ORDER BY timestamp DESC LIMIT 8"
        ).collect()
        note += " | latest event_log rows (type, update, time): " + "; ".join(f"{r['event_type']}/{r['u']}/{r['t']}" for r in recent)
    except Exception as exc:
        note += f" | event_log listing failed: {type(exc).__name__}: {str(exc)[:120]}"
    return None, note, attempts


def read_create_update(spark):
    return read_create_update_detail(spark)[0]


# -- guard read audit ---------------------------------------------------------
#
# bronze.py records how many event-log reads the guard needed in a one-row
# table (GUARD_READS_TABLE, rewritten by every update that gets past the guard);
# log_run_audit copies it into run_audit as a `guard` layer row. A blocked
# update fails before its tables are written, so its reads show up in the
# pipeline error message instead (and the row still holds the previous update:
# log_run_audit checks the update id).

GUARD_READS_TABLE = "poc_bronze.guard_reads"
GUARD_READS_COLUMNS = ("update_id", "reads", "event_found", "note")


def guard_reads_row(update_id, reads: int, event_found: bool, note: str) -> tuple:
    return (update_id or "", int(reads), bool(event_found), (note or "")[:1000])


def pick_update_id(updates, start_ms, end_ms):
    """The pipeline update a job run's pipeline task started: the latest update created within the task's
    run window. `updates` = [(update_id, creation_time_ms)]. None when none falls in the window."""
    if not start_ms:
        return None
    hits = [(ms, uid) for uid, ms in updates if ms and ms >= start_ms and (not end_ms or ms <= end_ms)]
    return max(hits)[1] if hits else None


def guard_audit_row(guard: dict | None, expected_update_id: str | None, run_id: str, started_at, ended_at) -> dict:
    """run_audit row (layer `guard`, table_id NULL) for the guard read of this job run's pipeline update.

    status OK: the event was found on the first read. WARN: it took retries, was not found (fail-closed or
    all_dates pass-through), or no row for this update exists (the row is from an earlier update, or none).
    rows_read = reads needed.
    """
    base = {"run_id": run_id, "table_id": None, "layer": "guard", "rows_written": None, "rows_rejected": None,
            "started_at": started_at, "ended_at": ended_at}
    if not guard or (expected_update_id and guard.get("update_id") != expected_update_id):
        return {**base, "status": "WARN", "rows_read": None,
                "error": f"no guard_reads row for update {expected_update_id or '<unknown>'}"}
    reads, found = int(guard.get("reads") or 0), bool(guard.get("event_found"))
    status = "OK" if found and reads == 1 else "WARN"
    detail = f"update {guard.get('update_id')}: create_update event {'found' if found else 'NOT found'} after {reads} read(s)"
    if guard.get("note"):
        detail += f"; {guard['note']}"
    return {**base, "status": status, "rows_read": reads, "error": None if status == "OK" else detail}


def source_pairs_df(src_df, key_col: str, watermark_col: str, floor: datetime.date):
    """Distinct (k, d) pairs in the source: business key as string, watermark as a date."""
    from pyspark.sql import functions as F

    return (
        src_df.where(F.col(watermark_col) >= F.lit(floor))
        .select(
            F.coalesce(F.col(key_col).cast("string"), F.lit(NULL_KEY)).alias("k"),
            F.to_date(F.col(watermark_col)).alias("d"),
        )
        .distinct()
    )


def read_ledger_pairs_df(spark, ledger_table: str, source_table: str):
    """Ledger rows for one source as (k, d). A missing ledger table means nothing has been loaded yet."""
    from pyspark.sql import functions as F

    try:
        df = spark.read.table(ledger_table)
    except Exception:  # AnalysisException: ledger not created yet (first ever run)
        return spark.createDataFrame([], "k STRING, d DATE")
    return df.where(F.col("table_name") == source_table).select(
        F.col("business_key").alias("k"), F.col("snapshot_date").alias("d")
    )


# --------------------------------------------------------------------------
# Per-date fingerprints
#
# Comparing every (business_key, date) pair of a large source on every run is
# expensive. Instead each side computes, per date, over the DISTINCT
# (business_key, date) pairs: the pair count and the sum of a 60-bit
# md5-derived bigint per pair. Postgres computes the source side server-side
# and returns one row per date; the ledger stores the same numbers. Pairs are
# fetched only for dates whose fingerprint differs. A moved row changes the
# sum even when the count is unchanged.
#
# hash(pair) = int(first 15 hex chars of md5("<key>|<YYYY-MM-DD>"), 16); a NULL
# key is written as NULL_KEY. The sum is kept as an exact integer (Postgres
# numeric, Spark decimal(38,0), Python int) and compared as a string.
# --------------------------------------------------------------------------

FP_HEX_CHARS = 15  # 60 bits: fits a non-negative bigint per pair


def pair_hash(key, day: datetime.date) -> int:
    """Python reference for the per-pair hash (must equal the Postgres and Spark expressions)."""
    digest = hashlib.md5(f"{ledger_key(key)}|{day.isoformat()}".encode("utf-8")).hexdigest()
    return int(digest[:FP_HEX_CHARS], 16)


def date_fingerprints(pairs) -> dict:
    """{date: (distinct pair count, sum of pair hashes as a decimal string)} -- the Python reference."""
    distinct = {(ledger_key(k), d) for k, d in pairs}
    grouped: dict = {}
    for k, d in distinct:
        n, total = grouped.get(d, (0, 0))
        grouped[d] = (n + 1, total + pair_hash(k, d))
    return {d: (n, str(total)) for d, (n, total) in sorted(grouped.items())}


def mismatched_dates(source_fp: dict, ledger_fp: dict) -> list:
    """Dates present on both sides whose fingerprints differ (dates on only one side are handled by the plan)."""
    return sorted(d for d in source_fp if d in ledger_fp and tuple(source_fp[d]) != tuple(ledger_fp[d]))


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def source_fingerprint_sql(schema: str, table: str, key_col: str, watermark_col: str, floor: datetime.date) -> str:
    """Postgres subquery (usable as a JDBC `dbtable`) returning one row per date:
    snapshot_date, row_count (distinct pairs), fp_sum (text)."""
    bits = FP_HEX_CHARS * 4
    return (
        "(SELECT d AS snapshot_date, count(*) AS row_count, sum(h)::text AS fp_sum FROM ("
        f"SELECT d, ('x' || substr(md5(k || '|' || to_char(d, 'YYYY-MM-DD')), 1, {FP_HEX_CHARS}))::bit({bits})::bigint AS h FROM ("
        f"SELECT DISTINCT coalesce({_quote(key_col)}::text, '{NULL_KEY}') AS k, {_quote(watermark_col)}::date AS d "
        f"FROM {_quote(schema)}.{_quote(table)} WHERE {_quote(watermark_col)} >= DATE '{floor.isoformat()}'"
        ") pairs) hashed GROUP BY d) fp"
    )


# The same fingerprint in Spark SQL, over a relation with columns (k STRING, d DATE).
SPARK_FINGERPRINT_SQL = (
    "SELECT d AS snapshot_date, count(*) AS row_count, "
    "cast(sum(cast(cast(conv(substring(md5(concat(k, '|', date_format(d, 'yyyy-MM-dd'))), 1, "
    + str(FP_HEX_CHARS)
    + "), 16, 10) AS bigint) AS decimal(38, 0))) AS string) AS fp_sum "
    "FROM (SELECT DISTINCT k, d FROM {relation}) GROUP BY d"
)


def spark_fingerprint_sql(relation: str) -> str:
    return SPARK_FINGERPRINT_SQL.format(relation=relation)


def fingerprints_df(spark, pairs_df):
    """Per-date fingerprints of a (k, d) DataFrame: columns snapshot_date, row_count, fp_sum."""
    pairs_df.createOrReplaceTempView("_fp_pairs")
    return spark.sql(spark_fingerprint_sql("_fp_pairs"))


def fingerprint_table_name(catalog: str) -> str:
    return f"{catalog}.ledger.bronze_fingerprints"


def read_jdbc_query(spark, conn: PgConn, query: str):
    """Batch-read the result of a subquery `(SELECT ...) alias` over JDBC."""
    return (
        spark.read.format("jdbc")
        .option("url", _jdbc_url(conn))
        .option("user", conn.user)
        .option("password", conn.token)
        .option("dbtable", query)
        .load()
    )


def read_source_fingerprints(spark, conn: PgConn, schema, table, key_col, watermark_col, floor) -> dict:
    df = read_jdbc_query(spark, conn, source_fingerprint_sql(schema, table, key_col, watermark_col, floor))
    return {r["snapshot_date"]: (int(r["row_count"]), str(r["fp_sum"])) for r in df.collect()}


def read_ledger_fingerprints(spark, fingerprint_table: str, source_table: str) -> dict:
    """The ledger's per-date fingerprints for one source ({} if the ledger does not exist yet)."""
    from pyspark.sql import functions as F

    try:
        df = spark.read.table(fingerprint_table)
    except Exception:  # first ever run: nothing loaded
        return {}
    rows = df.where(F.col("table_name") == source_table).collect()
    return {r["snapshot_date"]: (int(r["row_count"]), str(r["fp_sum"])) for r in rows}


def collect_plan_inputs(
    spark, conn: PgConn, table_def: dict, ledger_table: str, fingerprint_table: str,
    rebuild: bool = False, max_pending: int = DEFAULT_MAX_PENDING_KEYS,
):
    """Graph-build inputs for plan_flows: (source dates, ledger dates, pending keys by loaded date).

    1. Postgres returns one fingerprint row per source date (no pairs cross the wire).
    2. Dates missing from the ledger are new: their snapshot flow loads them whole.
    3. Only dates on both sides whose fingerprints differ are examined further: their source
       pairs are fetched and anti-joined with the ledger's pairs; the missing pairs become top-ups.
    With rebuild=True the ledger is ignored and only the source dates are returned.
    """
    from pyspark.sql import functions as F

    schema, table = table_def["dest_schema"], table_def["source_table"]
    key_col, wm_col = table_def["business_key"].strip(), table_def["watermark_col"].strip()
    floor = floor_date(table_def)

    source_fp = read_source_fingerprints(spark, conn, schema, table, key_col, wm_col, floor)
    src_dates = sorted(source_fp)
    if rebuild:
        return src_dates, set(), {}

    ledger_fp = read_ledger_fingerprints(spark, fingerprint_table, table)
    bad = mismatched_dates(source_fp, ledger_fp)
    topups: dict = {}
    if bad:
        in_dates = None
        for d in bad:
            cond = (F.col(wm_col) >= F.lit(d)) & (F.col(wm_col) < F.lit(d + datetime.timedelta(days=1)))
            in_dates = cond if in_dates is None else in_dates | cond
        src_pairs = source_pairs_df(read_jdbc_table(spark, conn, schema, table).where(in_dates), key_col, wm_col, floor)
        ledger_pairs = read_ledger_pairs_df(spark, ledger_table, table).where(F.col("d").isin(bad))
        rows = src_pairs.join(ledger_pairs, ["k", "d"], "left_anti").limit(max_pending + 1).collect()
        enforce_pending_cap(len(rows), max_pending)
        topups = group_pending_by_date((r["k"], r["d"]) for r in rows)
    return src_dates, set(ledger_fp), topups

def ledger_table_name(catalog: str) -> str:
    return f"{catalog}.ledger.bronze_keys"


def ensure_ledger_table(spark, catalog: str) -> str:
    """Create the key ledger (outside the pipeline) if it does not exist yet."""
    name = ledger_table_name(catalog)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ledger")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {name} (table_name STRING, business_key STRING, snapshot_date DATE) "
        "COMMENT 'Distinct (business_key, _snapshot_date) pairs loaded into poc_bronze; maintained by the sync_ledger job task'"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {fingerprint_table_name(catalog)} "
        "(table_name STRING, snapshot_date DATE, row_count BIGINT, fp_sum STRING) "
        "COMMENT 'Per-date fingerprint (distinct pair count, sum of md5-derived bigints) of bronze_keys; maintained by sync_ledger'"
    )
    return name


def bronze_pairs_df(spark, catalog: str, table_def: dict):
    """Distinct (table_name, business_key, snapshot_date) pairs bronze actually holds for one incremental table."""
    from pyspark.sql import functions as F

    table = table_def["source_table"]
    key = table_def["business_key"].strip()
    return (
        spark.table(f"{catalog}.{bronze_table_name(table)}")
        .select(
            F.lit(table).alias("table_name"),
            F.coalesce(F.col(key).cast("string"), F.lit(NULL_KEY)).alias("business_key"),
            F.col("_snapshot_date").alias("snapshot_date"),
        )
        .distinct()
    )


def pg_conn_from_job_secret(scope: str, key: str, host: str, user: str) -> PgConn:
    """PgConn for a job task (spark_python_task): the secret comes back base64-encoded from the SDK."""
    import base64

    from databricks.sdk import WorkspaceClient

    token = base64.b64decode(WorkspaceClient().secrets.get_secret(scope=scope, key=key).value).decode("utf-8")
    return PgConn(host=host, user=user, token=token)


def build_column_list(columns: list[dict]) -> list[str]:
    """Active column names for one table, ordered by source_columns.ordinal.

    `columns` is the list of source_columns rows (as dicts) for a single
    table_id. Rows with is_active=False are dropped. Rows with a NULL
    ordinal sort after every row that has one.
    """
    active = [c for c in columns if c.get("is_active") is None or bool(c.get("is_active"))]
    if not active:
        raise ValueError("No active columns found for table")
    active.sort(key=lambda c: (c["ordinal"] is None, c["ordinal"]))
    return [c["column_name"] for c in active]


def build_dq_predicate(rules: list[dict]) -> str:
    """SQL boolean expression: true only when every HARD rule passes.

    `rules` is the list of data_quality_rules rows (as dicts) for a single
    table_id. Only severity == 'HARD' rows are enforced; SOFT/other
    severities are ignored here (they don't gate Silver/reject routing).
    Returns the literal 'true' when there are no HARD rules for the table.
    """
    hard_exprs = [
        r["rule_expr"].strip()
        for r in rules
        if r.get("severity") == "HARD" and r.get("rule_expr") and r["rule_expr"].strip()
    ]
    if not hard_exprs:
        return "true"
    return " AND ".join(f"({expr})" for expr in hard_exprs)


def has_merge_keys(table_def: dict) -> bool:
    """True when a table_def has both a business_key and a watermark_col --
    what Silver's AUTO CDC merge needs to key and sequence by. Shared by
    silver.py (which tables get a Silver flow) and log_run_audit.py (which
    tables get a 'silver' run_audit row)."""
    return bool(_blank_to_none(table_def.get("business_key"))) and bool(_blank_to_none(table_def.get("watermark_col")))


def build_soft_expectations(rules: list[dict]) -> dict[str, str]:
    """rule_name -> SQL boolean expression for every SOFT rule, ready for
    dp.expect_all(). SOFT rules only record a violation metric in the
    pipeline event log; they never drop or reject rows. Rows with a blank
    rule_expr are skipped. The name is the expectation name in the event log;
    when two SOFT rules of the table share a name, each gets " (rule <rule_id>)"
    appended, so neither silently replaces the other.
    """
    soft = [r for r in rules if r.get("severity") == "SOFT" and r.get("rule_expr") and r["rule_expr"].strip()]
    names = [r["rule_name"] for r in soft]
    return {soft_expectation_name(r, names.count(r["rule_name"]) > 1): r["rule_expr"].strip() for r in soft}


def soft_expectation_name(rule: dict, duplicated: bool = False) -> str:
    return f"{rule['rule_name']} (rule {rule.get('rule_id')})" if duplicated else rule["rule_name"]


def build_dq_reason_expr(rules: list[dict]) -> str:
    """SQL expression producing a comma-joined list of failed HARD rule names.

    Evaluates to NULL literal when there are no HARD rules for the table
    (nothing can fail).
    """
    hard_rules = [
        (r["rule_name"], r["rule_expr"].strip())
        for r in rules
        if r.get("severity") == "HARD" and r.get("rule_expr") and r["rule_expr"].strip()
    ]
    if not hard_rules:
        return "NULL"
    cases = ", ".join(f"CASE WHEN NOT ({expr}) THEN '{name}' END" for name, expr in hard_rules)
    return f"array_join(array_compact(array({cases})), ', ')"
