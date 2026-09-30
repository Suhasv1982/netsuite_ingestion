"""Gold SQL (gold_sql.py) run on DuckDB against small silver-shaped tables.

The pipeline runs the same SQL text with spark.sql, so these tests check the
logic (grouping, joins, activity windows) without Spark. Engine differences are
kept out of the SQL on purpose (see gold_sql.py).
"""

import datetime as dt

import pytest

from gold_sql import (
    REVENUE_INPUTS,
    STATUS_INPUTS,
    customer_revenue_sql,
    customer_status_sql,
    missing_inputs,
)

duckdb = pytest.importorskip("duckdb")

D = dt.date
AS_OF = "DATE '2026-09-30'"


@pytest.fixture()
def db():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE tx (transaction_internal_id INT, tranid VARCHAR, customer_internal_id INT, type VARCHAR, date DATE);
        INSERT INTO tx VALUES
          (1, 'T1', 100, 'Invoice',   DATE '2026-07-11'),
          (2, 'T2', 100, 'Cash Sale', DATE '2026-07-30'),
          (3, 'T3', 100, 'Invoice',   DATE '2026-08-01'),
          (4, 'T4', 200, 'Invoice',   DATE '2026-07-15'),
          (5, 'T5', NULL, 'Invoice',  DATE '2026-07-15'),   -- no customer: left out
          (6, 'T6', 300, 'Invoice',   NULL),                -- no date: left out
          (7, 'T7', 400, 'Invoice',   DATE '2026-07-20');   -- no lines: no revenue row
        CREATE TABLE lines (transaction_line_id VARCHAR, transaction_internal_id INT, amount INT);
        INSERT INTO lines VALUES
          ('1-1', 1, 250), ('1-2', 1, 60),
          ('2-1', 2, 400),
          ('3-1', 3, 175),
          ('4-1', 4, 900),
          ('5-1', 5, 1000),
          ('6-1', 6, 1000),
          ('99-1', 99, 5000);                                 -- orphan line: not counted
        CREATE TABLE mem (membership_internal_id INT, customer_internal_id INT, membership_type VARCHAR,
                          start_date DATE, end_date DATE, membership_status VARCHAR);
        INSERT INTO mem VALUES
          (1, 100, 'Professional', DATE '2026-01-01', DATE '2026-12-31', 'Active'),
          (2, 100, 'Student',      DATE '2026-01-01', NULL,              'Active'),   -- open end
          (3, 200, 'Professional', DATE '2025-01-01', DATE '2026-09-29', 'Active'),   -- ended yesterday
          (4, 200, 'Other',        DATE '2026-01-01', DATE '2026-12-31', 'Suspended'),
          (5, 300, 'Professional', DATE '2026-10-01', DATE '2027-09-30', 'Active'),   -- starts tomorrow
          (6, 500, 'Professional', DATE '2026-09-30', DATE '2026-09-30', 'Active');   -- one-day window, today
        CREATE TABLE cert (certification_internal_id INT, customer_internal_id INT, certification_type VARCHAR,
                           certification_start_date DATE, certification_end_date DATE);
        INSERT INTO cert VALUES
          (1, 100, 'SCP', DATE '2025-06-01', DATE '2027-06-01'),
          (2, 200, 'CP',  DATE '2024-01-01', DATE '2026-01-01'),   -- expired
          (3, 600, 'CP',  NULL,              NULL);                -- open both ends
    """)
    yield con
    con.close()


def _rows(con, sql):
    cur = con.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


class TestCustomerRevenue:
    def test_revenue_per_customer_per_month(self, db):
        got = {
            (r["customer_internal_id"], r["revenue_month"]): (r["transaction_count"], r["line_count"], r["revenue"])
            for r in _rows(db, customer_revenue_sql("tx", "lines"))
        }
        assert got == {
            (100, D(2026, 7, 1)): (2, 3, 710),  # T1 250+60, T2 400
            (100, D(2026, 8, 1)): (1, 1, 175),
            (200, D(2026, 7, 1)): (1, 1, 900),
        }

    def test_leaves_out_rows_without_customer_date_or_lines(self, db):
        customers = {r["customer_internal_id"] for r in _rows(db, customer_revenue_sql("tx", "lines"))}
        assert customers == {100, 200}  # not NULL (T5), 300 (no date), 400 (no lines)

    def test_total_equals_sum_of_matched_lines(self, db):
        total = db.execute(f"SELECT SUM(revenue) FROM ({customer_revenue_sql('tx', 'lines')})").fetchone()[0]
        assert total == 250 + 60 + 400 + 175 + 900


class TestCustomerStatus:
    @pytest.fixture()
    def status(self, db):
        return {r["customer_internal_id"]: r for r in _rows(db, customer_status_sql("mem", "cert", AS_OF))}

    def test_one_row_per_customer_with_any_membership_or_certification(self, status):
        assert set(status) == {100, 200, 300, 500, 600}

    def test_active_counts(self, status):
        counts = {k: (v["active_memberships"], v["active_certifications"]) for k, v in status.items()}
        assert counts == {100: (2, 1), 200: (0, 0), 300: (0, 0), 500: (1, 0), 600: (0, 1)}

    def test_flags_and_next_end(self, status):
        assert status[100]["has_active_membership"] and status[100]["has_active_certification"]
        assert status[100]["next_membership_end"] == D(2026, 12, 31)  # the open-ended one does not count
        assert status[100]["next_certification_end"] == D(2027, 6, 1)
        assert not status[200]["has_active_membership"] and status[200]["next_membership_end"] is None
        assert status[600]["next_certification_end"] is None  # open-ended certification

    def test_as_of_date_is_reported(self, status):
        assert {v["as_of_date"] for v in status.values()} == {D(2026, 9, 30)}

    def test_default_as_of_is_current_date(self):
        assert "current_date AS as_of_date" in customer_status_sql("m", "c")


class TestInputs:
    def test_buildable_when_all_silver_inputs_exist(self):
        assert missing_inputs(REVENUE_INPUTS, set(REVENUE_INPUTS) | set(STATUS_INPUTS)) == []

    def test_reports_the_missing_silver_table(self):
        assert missing_inputs(STATUS_INPUTS, {"netsuite_memberships"}) == ["netsuite_certifications"]
