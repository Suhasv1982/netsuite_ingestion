"""Integration test: the row hash and the per-date fingerprint agree across Python, Postgres and Spark.

Skipped unless NETSUITE_INTEGRATION=1 (it needs Databricks CLI access to the
netsuite-sample Lakebase endpoint and a SQL warehouse). Postgres is exercised
through session-local TEMP tables, so nothing is written to the real source.

    NETSUITE_INTEGRATION=1 pytest tests/test_fingerprint_integration.py

* Row hash: Postgres computes it (metadata.row_hash_sql) for every column type
  the source uses (integer, text, date, timestamp, NULL); the test checks it
  equals md5 of the canonical JSON text the Python helper below writes.
* Fingerprints: Postgres runs metadata.source_fingerprint_sql over a temp table
  (k, d, v) hashed over all three columns; Spark runs the exact SQL text
  (metadata.SPARK_FINGERPRINT_SQL) that sync_ledger / ledger_check use over the
  same (k, d, h) entries; the Python reference must match both.
"""

import datetime as dt
import hashlib
import json
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

from metadata import NULL_KEY, date_fingerprints, row_hash_sql, source_fingerprint_sql, spark_fingerprint_sql  # noqa: E402

FLOOR = dt.date(1900, 1, 1)
D1, D2, D3 = dt.date(2026, 7, 11), dt.date(2026, 8, 1), dt.date(2026, 8, 22)


def canonical_md5(values) -> str:
    """md5 of Postgres' json_build_array(...)::text for these values (Python model, used only by this test)."""
    def norm(v):
        if isinstance(v, dt.datetime):
            return v.isoformat()
        if isinstance(v, dt.date):
            return v.isoformat()
        return v
    return hashlib.md5(json.dumps([norm(v) for v in values], ensure_ascii=False).encode("utf-8")).hexdigest()


def sample_rows(n_random=3000, seed=7):
    """(k, d, v) rows: k is the key (may be NULL), d the date, v other content."""
    rng = random.Random(seed)
    rows = [
        ("1", D1, "a"), ("2", D1, "b"), ("2", D1, "b"),       # an exact copy counts once
        ("2", D1, "b2"),                                       # a second version of key 2 on D1 counts too
        ("90001_1", D2, "x"), ("90001_2", D2, "x"),
        (None, D1, "n"), (None, D1, "n"), (None, D3, "n"),   # NULL keys share the sentinel
        ("café-中", D3, "ü"), ("with|pipe", D3, "p|q"), ("", D3, ""), ("  spaced  ", D2, " s "),
    ]
    rows += [(str(rng.randrange(10**7)), dt.date(2026, 1, 1) + dt.timedelta(days=rng.randrange(30)),
              rng.choice("abc")) for _ in range(n_random)]
    return rows


def entries(rows):
    """(key, date, row hash) entries as bronze would hold them: the hash covers k, d and v."""
    return [(NULL_KEY if k is None else k, d, canonical_md5([k, d, v])) for k, d, v in rows]


def postgres_fingerprints(rows):
    conn = pg_writer.connect("DEFAULT")
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE fp_check (k text, d date, v text)")
            with cur.copy("COPY fp_check (k, d, v) FROM STDIN") as cp:
                for row in rows:
                    cp.write_row(list(row))
            cur.execute("SELECT * FROM " + source_fingerprint_sql("pg_temp", "fp_check", "k", "d", FLOOR, ["k", "d", "v"]))
            return {r["snapshot_date"]: (int(r["row_count"]), r["fp_sum"]) for r in cur.fetchall()}
    finally:
        conn.rollback()
        conn.close()


def spark_fingerprints(ents):
    def lit(s):
        return "'" + s.replace("'", "''") + "'"

    values = ", ".join(f"({lit(k)}, DATE'{d.isoformat()}', {lit(h)})" for k, d, h in ents)
    rows = cm.sql("DEFAULT", spark_fingerprint_sql(f"(VALUES {values}) AS t(k, d, h)"))
    return {dt.date.fromisoformat(r["snapshot_date"]): (int(r["row_count"]), r["fp_sum"]) for r in rows}


@pytest.fixture(scope="module")
def rows():
    return sample_rows()


def test_postgres_row_hash_is_the_canonical_json_md5_for_every_source_type():
    typed = [
        (1, "plain", D1, dt.datetime(2026, 10, 2, 0, 0, 0)),
        (2, "café-中 \"quoted\" back\\slash", D2, dt.datetime(2026, 10, 3, 13, 45, 7)),
        (None, None, None, None),
    ]
    conn = pg_writer.connect("DEFAULT")
    try:
        with conn.cursor() as cur:
            cur.execute("SET TimeZone = 'America/New_York'; SET DateStyle = 'SQL, DMY'")  # must not matter
            cur.execute("CREATE TEMP TABLE rh_check (i integer, t text, d date, ts timestamp without time zone)")
            with cur.copy("COPY rh_check (i, t, d, ts) FROM STDIN") as cp:
                for row in typed:
                    cp.write_row(list(row))
            cur.execute(f"SELECT {row_hash_sql(['i', 't', 'd', 'ts'])} AS h FROM rh_check ORDER BY i NULLS LAST")
            got = [r["h"] for r in cur.fetchall()]
    finally:
        conn.rollback()
        conn.close()
    assert got == [canonical_md5(r) for r in typed]


def test_python_reference_equals_postgres(rows):
    assert date_fingerprints(entries(rows)) == postgres_fingerprints(rows)


def test_python_reference_equals_spark(rows):
    assert date_fingerprints(entries(rows)) == spark_fingerprints(entries(rows))


def test_sums_exceed_a_64_bit_integer_and_stay_exact(rows):
    fp = date_fingerprints(entries(rows))
    assert max(int(total) for _, total in fp.values()) > 2**63
    assert fp == postgres_fingerprints(rows) == spark_fingerprints(entries(rows))


def test_a_second_version_on_a_loaded_date_is_visible_on_every_side():
    ledger = [("7", D1, "first")]
    source = [("7", D1, "first"), ("7", D1, "second")]  # incident 2026-10-02: same key, same day
    led_py, src_py = date_fingerprints(entries(ledger)), date_fingerprints(entries(source))
    assert led_py[D1] != src_py[D1]
    assert postgres_fingerprints(source) == src_py == spark_fingerprints(entries(source))
