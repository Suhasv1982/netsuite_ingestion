"""Silver layer: MERGE the validated rows into poc_silver.<table>.

Only tables that have both a business_key and a watermark_col get a Silver
table -- per source_table_def, that's every table except netsuite_customers
(FullLoad, no watermark_col, so there's nothing to sequence a merge by).
netsuite_customers stops at bronze + DQ; see self-review notes.

MERGE is implemented as an AUTO CDC flow (SCD Type 1: keyed upsert, no
history) keyed on business_key and sequenced by (watermark_col, _row_hash),
sourced from the DQ layer's "<table>_valid" view. updated_date is a plain
date, so two versions of a key can share it; _row_hash breaks the tie by
content only, so every environment keeps the same version whatever order the
rows were loaded in (owner decision 2026-10-08). The kept version is
deterministic, not necessarily the newer one: the source has no finer
timestamp to tell.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, struct

from metadata import ROW_HASH_COL, has_merge_keys, pg_conn_from_conf, read_table_defs

meta_conn = pg_conn_from_conf(spark, dbutils, "meta")

table_defs = read_table_defs(spark, meta_conn)


def _register_silver_merge(table_def: dict) -> None:
    source_table = table_def["source_table"]
    business_key = table_def["business_key"].strip()
    watermark_col = table_def["watermark_col"].strip()
    target = f"poc_silver.{source_table}"
    valid_view = f"{source_table}_valid"

    dp.create_streaming_table(
        name=target,
        comment=f"Silver: {source_table} merged on {business_key}, sequenced by ({watermark_col}, {ROW_HASH_COL})",
    )
    dp.create_auto_cdc_flow(
        target=target,
        source=valid_view,
        keys=[business_key],
        sequence_by=struct(col(watermark_col), col(ROW_HASH_COL)),
        stored_as_scd_type="1",
    )


for _table_def in table_defs:
    if has_merge_keys(_table_def):
        _register_silver_merge(_table_def)
