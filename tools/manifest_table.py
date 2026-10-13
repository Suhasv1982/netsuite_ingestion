"""Generator defect manifests as a Delta table (ground truth for the DQ recommender eval).

One row per injected defect row: which batch (batch_date, seed), which defect, which table and business key.
The daily generator job writes its batch with Spark (netsuite_gen.py --manifest-table). Recomputed manifests
(tools/recompute_manifest.py) and committed baseline manifests are loaded through a SQL warehouse:

    python tools/manifest_table.py --table workspace.generator.manifests --source recomputed \
        --code-version f14ef2d baseline/raw/manifests/manifest_20261002.json ... --profile DEFAULT

Writing a batch replaces that batch's rows (same batch_date, seed and source), so a re-run is idempotent.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

COLUMNS = [
    ("batch_date", "DATE"),
    ("seed", "BIGINT"),
    ("mode", "STRING"),             # init | increment
    ("source", "STRING"),           # job | recomputed | baseline
    ("code_version", "STRING"),     # commit of the generator code that made the batch, when known
    ("defect", "STRING"),
    ("table_name", "STRING"),
    ("business_key", "STRING"),     # NULL for a defect on a row whose key is itself NULL
    ("column_name", "STRING"),
    ("observable_in_bronze", "BOOLEAN"),
    ("detail", "STRING"),           # JSON: the defect entry's other fields, and this row's details if listed
    ("recorded_at", "TIMESTAMP"),
]
SOURCES = ("job", "recomputed", "baseline")


def ddl(table: str) -> str:
    cols = ", ".join(f"{n} {t}" for n, t in COLUMNS)
    return (f"CREATE TABLE IF NOT EXISTS {table} ({cols}) COMMENT 'Generator defect manifests, one row per injected "
            "defect row (tools/manifest_table.py); ground truth for the DQ recommender eval'")


def manifest_rows(manifest: dict, source: str, code_version: str | None = None) -> list[dict]:
    """Flatten a generator manifest into table rows (recorded_at is set by the writer)."""
    if source not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES}")
    batch = manifest.get("batch_date")
    out = []
    for entry in manifest.get("defects", []):
        per_row = {str(r.get("key")): r for r in entry.get("rows", []) if isinstance(r, dict)}
        common = {k: v for k, v in entry.items()
                  if k not in ("defect", "table", "business_keys", "row_count", "rows", "column", "observable_in_bronze")}
        for key in entry.get("business_keys", []):
            detail = {**common, **({"row": per_row[str(key)]} if str(key) in per_row else {})}
            out.append({
                "batch_date": str(batch) if batch else None, "seed": manifest.get("seed"), "mode": manifest.get("mode"),
                "source": source, "code_version": code_version, "defect": entry["defect"],
                "table_name": entry["table"], "business_key": None if key is None else str(key),
                "column_name": entry.get("column"), "observable_in_bronze": entry.get("observable_in_bronze"),
                "detail": json.dumps(detail, default=str, sort_keys=True) if detail else None,
            })
    return out


def _sql_literal(value, sql_type: str) -> str:
    if value is None:
        return f"CAST(NULL AS {sql_type})"
    if sql_type == "BOOLEAN":
        return "true" if value else "false"
    if sql_type == "BIGINT":
        return str(int(value))
    if sql_type == "DATE":
        return f"DATE'{dt.date.fromisoformat(str(value)).isoformat()}'"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def replace_batch_statements(table: str, rows: list[dict], manifest: dict, source: str, chunk: int = 500) -> list[str]:
    """SQL statements that replace one batch's rows: DELETE that (batch_date, seed, source), then INSERT in chunks."""
    batch, seed = manifest.get("batch_date"), manifest.get("seed")
    stmts = [f"DELETE FROM {table} WHERE batch_date = {_sql_literal(batch, 'DATE')} AND seed = "
             f"{_sql_literal(seed, 'BIGINT')} AND source = {_sql_literal(source, 'STRING')}"]
    names = [n for n, _ in COLUMNS]
    for i in range(0, len(rows), chunk):
        values = ", ".join(
            "(" + ", ".join("current_timestamp()" if n == "recorded_at" else _sql_literal(r.get(n), t)
                            for n, t in COLUMNS) + ")"
            for r in rows[i:i + chunk])
        stmts.append(f"INSERT INTO {table} ({', '.join(names)}) VALUES {values}")
    return stmts


def write_with_spark(spark, table: str, manifest: dict, source: str = "job", code_version: str | None = None) -> int:
    """Used inside the generator job (serverless Spark, as the data-generator service principal)."""
    spark.sql(ddl(table))
    rows = manifest_rows(manifest, source, code_version)
    for stmt in replace_batch_statements(table, rows, manifest, source):
        spark.sql(stmt)
    return len(rows)


# -- loading files through a SQL warehouse (owner, locally) ---------------------------------------------


def _warehouse_sql(profile: str, warehouse_id: str, statement: str) -> None:
    import subprocess

    def cli(*args, body=None):
        cmd = ["databricks", *args, "-o", "json", "--profile", profile] + (["--json", json.dumps(body)] if body else [])
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode:
            raise RuntimeError(p.stderr.strip()[:300])
        return json.loads(p.stdout) if p.stdout.strip() else {}

    r = cli("api", "post", "/api/2.0/sql/statements",
            body={"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "50s"})
    while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = cli("api", "get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"statement failed: {json.dumps(r.get('status'))[:300]}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("files", nargs="+", type=Path)
    p.add_argument("--table", required=True)
    p.add_argument("--source", required=True, choices=["recomputed", "baseline"])
    p.add_argument("--code-version", help="generator commit that made these batches")
    p.add_argument("--warehouse-id", required=True)
    p.add_argument("--profile", default="DEFAULT")
    args = p.parse_args(argv)
    _warehouse_sql(args.profile, args.warehouse_id, ddl(args.table))
    for f in args.files:
        manifest = json.loads(f.read_text(encoding="utf-8"))
        if args.source == "recomputed" and not manifest.get("recomputed"):
            print(f"REFUSED {f}: not a verified recomputed manifest (tools/recompute_manifest.py)")
            return 1
        version = args.code_version or (manifest.get("recomputed") or {}).get("code_dir_commit")
        rows = manifest_rows(manifest, args.source, version)
        for stmt in replace_batch_statements(args.table, rows, manifest, args.source):
            _warehouse_sql(args.profile, args.warehouse_id, stmt)
        print(f"loaded {f.name}: batch {manifest.get('batch_date')}, {len(rows)} defect rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
