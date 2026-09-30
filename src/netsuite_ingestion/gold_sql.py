"""Gold-layer queries, as plain SQL built from silver table names.

The pipeline (transformations/gold.py) runs them with spark.sql; the tests run the
same text on DuckDB. So the SQL sticks to what both engines accept: no arrays,
no engine-specific date functions (months are built with make_date/year/month).

Both queries read silver only: silver holds the latest version of each business
key (AUTO CDC, SCD1) and only rows that passed the HARD data-quality rules.
"""

GOLD_SCHEMA = "poc_gold"
CUSTOMER_REVENUE = "gold_customer_revenue"
CUSTOMER_STATUS = "gold_customer_status"

# silver tables each gold table needs (source_table names from source_table_def)
REVENUE_INPUTS = ("netsuite_transactions", "netsuite_transaction_lines")
STATUS_INPUTS = ("netsuite_memberships", "netsuite_certifications")


def customer_revenue_sql(transactions: str, lines: str) -> str:
    """Revenue per customer per calendar month of the transaction date.

    One row per (customer_internal_id, revenue_month). revenue = sum of line
    amounts; a transaction without lines contributes nothing, a line without its
    transaction is not counted (inner join). Transactions without a customer or a
    date are left out.
    """
    return f"""
SELECT
  t.customer_internal_id,
  make_date(year(t.date), month(t.date), 1) AS revenue_month,
  COUNT(DISTINCT t.transaction_internal_id) AS transaction_count,
  COUNT(l.transaction_line_id) AS line_count,
  SUM(l.amount) AS revenue
FROM {transactions} AS t
JOIN {lines} AS l ON l.transaction_internal_id = t.transaction_internal_id
WHERE t.customer_internal_id IS NOT NULL AND t.date IS NOT NULL
GROUP BY t.customer_internal_id, make_date(year(t.date), month(t.date), 1)
"""


def customer_status_sql(memberships: str, certifications: str, as_of: str = "current_date") -> str:
    """Active memberships and certifications per customer, as of `as_of` (a SQL date expression).

    One row per customer that has any membership or certification in silver.
    Active membership: status 'Active' and as_of within [start_date, end_date]
    (a NULL bound is open). Active certification: as_of within
    [certification_start_date, certification_end_date] (NULL bound open).
    """
    return f"""
WITH customers AS (
  SELECT customer_internal_id FROM {memberships} WHERE customer_internal_id IS NOT NULL
  UNION
  SELECT customer_internal_id FROM {certifications} WHERE customer_internal_id IS NOT NULL
),
active_m AS (
  SELECT customer_internal_id, COUNT(*) AS n, MIN(end_date) AS next_end
  FROM {memberships}
  WHERE membership_status = 'Active'
    AND (start_date IS NULL OR start_date <= {as_of})
    AND (end_date IS NULL OR end_date >= {as_of})
  GROUP BY customer_internal_id
),
active_c AS (
  SELECT customer_internal_id, COUNT(*) AS n, MIN(certification_end_date) AS next_end
  FROM {certifications}
  WHERE (certification_start_date IS NULL OR certification_start_date <= {as_of})
    AND (certification_end_date IS NULL OR certification_end_date >= {as_of})
  GROUP BY customer_internal_id
)
SELECT
  k.customer_internal_id,
  COALESCE(m.n, 0) AS active_memberships,
  COALESCE(c.n, 0) AS active_certifications,
  COALESCE(m.n, 0) > 0 AS has_active_membership,
  COALESCE(c.n, 0) > 0 AS has_active_certification,
  m.next_end AS next_membership_end,
  c.next_end AS next_certification_end,
  {as_of} AS as_of_date
FROM customers AS k
LEFT JOIN active_m AS m ON m.customer_internal_id = k.customer_internal_id
LEFT JOIN active_c AS c ON c.customer_internal_id = k.customer_internal_id
"""


def missing_inputs(needed: tuple, silver_tables: set) -> list:
    """The silver tables a gold table needs that the pipeline does not define (empty list = buildable)."""
    return [t for t in needed if t not in silver_tables]
