"""Integration test: the per-date fingerprint is identical in Python, Postgres and Spark.

Skipped unless NETSUITE_INTEGRATION=1 (it needs Databricks CLI access to the
netsuite-sample Lakebase endpoint and a SQL warehouse). Postgres is exercised
through a session-local TEMP table, so nothing is written to the real source.

    NETSUITE_INTEGRATION=1 pytest tests/test_fingerprint_integration.py

The Spark side runs the exact SQL text (metadata.SPARK_FINGERPRINT_SQL) that
sync_ledger / ledger_check use in the pipeline, on a Databricks SQL warehouse.
"""

import datetime as dt
import os
import random
import sys
from pathlib import Path

import pytest

if os.environ.get("NETSUITE_INTEGRATION") != "1":
    pytest.skip("set NETSUITE_INTEGRATION=1 to run the Postgres/Spark fingerprint comparison", allow_module_level=True)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "baseline"))

import collect_metrics as cm  # noqa: E402
import pg_writer  # noqa: E402

from metadata import NULL_KEY, date_fingerprints, source_fingerprint_sql, spark_fingerprint_sql  # noqa: E402

FLOOR = dt.date(1900, 1, 1)
D1, D2, D3 = dt.date(2026, 7, 11), dt.date(2026, 8, 1), dt.date(2026, 8, 22)


def sample_pairs(n_random=5000, seed=7):
    rng = random.Random(seed)
    pairs = [
        ("1", D1), ("2", D1), ("2", D1),               # a duplicate pair counts once
        ("90001_1", D2), ("90001_2", D2),
        (None, D1), (None, D1), (None, D3),              # NULL keys share the sentinel
        ("café-中", D3), ("with|pipe", D3), ("", D3), ("  spaced  ", D2),
    ]
    pairs += [(str(rng.randrange(10**7)), dt.date(2026, 1, 1) + dt.timedelta(days=rng.randrange(30))) for _ in range(n_random)]
    return pairs


def postgres_fingerprints(pairs):
    conn = pg_writer.connect("DEFAULT")
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE fp_check (k text, d date)")
            with cur.copy("COPY fp_check (k, d) FROM STDIN") as cp:
                for k, d in pairs:
                    cp.write_row([k, d])
            sql = "SELECT * FROM " + source_fingerprint_sql("pg_temp", "fp_check", "k", "d", FLOOR)
            cur.execute(sql)
            return {r["snapshot_date"]: (int(r["row_count"]), r["fp_sum"]) for r in cur.fetchall()}
    finally:
        conn.rollback()
        conn.close()


def spark_fingerprints(pairs):
    def lit(k):
        return "CAST(NULL AS STRING)" if k is None else "'" + k.replace("'", "''") + "'"

    values = ", ".join(f"({lit(NULL_KEY if k is None else k)}, DATE'{d.isoformat()}')" for k, d in pairs)
    relation = f"(VALUES {values}) AS t(k, d)"
    rows = cm.sql("DEFAULT", spark_fingerprint_sql(relation))
    return {dt.date.fromisoformat(r["snapshot_date"]): (int(r["row_count"]), r["fp_sum"]) for r in rows}


@pytest.fixture(scope="module")
def pairs():
    return sample_pairs()


def test_python_reference_equals_postgres(pairs):
    assert date_fingerprints(pairs) == postgres_fingerprints(pairs)


def test_python_reference_equals_spark(pairs):
    # NULL keys are passed to Spark as the sentinel string, exactly as bronze_pairs_df / source_pairs_df write them
    assert date_fingerprints(pairs) == spark_fingerprints(pairs)


def test_postgres_equals_spark(pairs):
    assert postgres_fingerprints(pairs) == spark_fingerprints(pairs)


def test_sums_exceed_a_64_bit_integer_and_stay_exact(pairs):
    fp = date_fingerprints(pairs)
    biggest = max(int(total) for _, total in fp.values())
    assert biggest > 2**63  # a bigint accumulator would have overflowed; all three sides use exact wide integers
    assert fp == postgres_fingerprints(pairs) == spark_fingerprints(pairs)


def test_one_row_moves_out_and_one_late_row_moves_in_is_visible_on_every_side():
    ledger = [("1", D1), ("2", D1), ("3", D2)]
    source = [("2", D1), ("9", D1), ("1", D3), ("3", D2)]  # D1 keeps 2 rows: key 1 left, late key 9 arrived
    for compute in (date_fingerprints, postgres_fingerprints, spark_fingerprints):
        led, src = compute(ledger), compute(source)
        assert led[D1][0] == src[D1][0] == 2  # counts are equal ...
        assert led[D1] != src[D1]              # ... fingerprints are not
        assert led[D2] == src[D2]              # the untouched date still matches
