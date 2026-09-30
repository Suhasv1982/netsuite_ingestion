"""Gold layer: customer-level views of silver, as materialized views in poc_gold.

  poc_gold.gold_customer_revenue  revenue per customer per month (transactions + transaction_lines)
  poc_gold.gold_customer_status   active memberships and certifications per customer, as of today

Driven by the same metadata as silver: a gold table is defined only when every silver
table it reads is defined (source_table_def rows with business_key + watermark_col),
otherwise it is skipped with a message. The SQL lives in gold_sql.py so tests can run
it without Spark. Both are recomputed on every update (customer_status depends on
current_date).
"""

from pyspark import pipelines as dp

from gold_sql import (
    CUSTOMER_REVENUE,
    CUSTOMER_STATUS,
    GOLD_SCHEMA,
    REVENUE_INPUTS,
    STATUS_INPUTS,
    customer_revenue_sql,
    customer_status_sql,
    missing_inputs,
)
from metadata import has_merge_keys, pg_conn_from_conf, read_table_defs

meta_conn = pg_conn_from_conf(spark, dbutils, "meta")
silver_tables = {td["source_table"] for td in read_table_defs(spark, meta_conn) if has_merge_keys(td)}


def _silver(table: str) -> str:
    return f"poc_silver.{table}"


_missing = missing_inputs(REVENUE_INPUTS, silver_tables)
if _missing:
    print(f"gold: {CUSTOMER_REVENUE} skipped, silver table(s) not defined: {', '.join(_missing)}")
else:

    @dp.materialized_view(
        name=f"{GOLD_SCHEMA}.{CUSTOMER_REVENUE}",
        comment="Revenue per customer per month: sum of transaction line amounts by transaction date (silver)",
    )
    def gold_customer_revenue():
        return spark.sql(customer_revenue_sql(*(_silver(t) for t in REVENUE_INPUTS)))


_missing = missing_inputs(STATUS_INPUTS, silver_tables)
if _missing:
    print(f"gold: {CUSTOMER_STATUS} skipped, silver table(s) not defined: {', '.join(_missing)}")
else:

    @dp.materialized_view(
        name=f"{GOLD_SCHEMA}.{CUSTOMER_STATUS}",
        comment="Active memberships and certifications per customer as of the update date (silver)",
    )
    def gold_customer_status():
        return spark.sql(customer_status_sql(*(_silver(t) for t in STATUS_INPUTS)))
