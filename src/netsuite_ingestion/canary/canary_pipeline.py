"""Guard canary pipeline: a tiny pipeline that runs the SAME full-refresh guard as bronze.py.

It has one streaming table with one `once` flow that appends a single row. At
graph build it reads its own update's `create_update` event from the pipeline
event log and applies metadata.refresh_guard_error, exactly like bronze.py. It
is configured with bronze_rebuild=false and flow_scope=pending_only, so:

  * a normal update completes (the row is loaded once);
  * a full refresh, or a selective refresh naming canary_bronze, is refused
    with the BLOCKED message before any data changes.

run_canary.py triggers those updates on a schedule and asserts both behaviors,
so a platform change that alters how a refresh is reported in the event log is
noticed here instead of silently emptying bronze. Not part of the data flow.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from metadata import DEFAULT_SCOPE, read_create_update_detail, refresh_guard_error

TABLE = "canary_bronze"

_rebuild = (spark.conf.get("bronze_rebuild", "false") or "false").strip().lower() == "true"
_scope = (spark.conf.get("flow_scope", DEFAULT_SCOPE) or DEFAULT_SCOPE).strip()

_create_update, _create_update_note = read_create_update_detail(spark)
_guard_error = refresh_guard_error(
    _create_update,
    [TABLE],
    _rebuild,
    _scope,
    {
        "target": spark.conf.get("bundle_target", None),
        "update_id": spark.conf.get("spark.pipelines.updateId", None),
        "pipeline": spark.conf.get("pipelines.id", "guard_canary"),
        "note": _create_update_note,
    },
)
if _guard_error:
    raise RuntimeError(_guard_error)

dp.create_streaming_table(name=TABLE, comment="Guard canary: one row, loaded once")


@dp.append_flow(target=TABLE, name="canary_once", once=True)
def _canary_once():
    return spark.range(1).select(F.col("id").cast("bigint").alias("id"), F.current_timestamp().alias("_loaded_at"))
