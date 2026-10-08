"""Recompute the defect manifest of a past daily generator batch, and prove it by reproducing the batch's rows.

    python tools/recompute_manifest.py --code-dir <checkout of the commit that ran the batch> \
        --pre-schema netsuite_hold_202610020500 --post-schema netsuite_hold_202610030500 \
        --seed 685447674728824 --batch-date 2026-10-02 --out manifest_20261002.json --profile DEFAULT

Read-only. The daily generator (tools/netsuite_gen.py --increment) is deterministic given its seed (the job's
run_id), its config and the source state it reads. That state is the batch's pre-batch backup, copied to a
netsuite_hold_<stamp> schema so the daily prune keeps it. This tool runs the generator code of the commit that
ran the batch (--code-dir) in dry-run against --pre-schema, then compares the rows it would write with the rows
the batch really wrote: rows in --post-schema (the next batch's pre-state, or `netsuite` for the latest batch)
minus rows in --pre-schema, as multisets of full rows, per table (customers: inserts plus upserted rows).

The manifest is written (and exit 0) only when every table matches exactly; otherwise the batch has no ground
truth and must be excluded from evaluation (docs/dq_recommender_design_v2.md, section 9).
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import importlib
import json
import sys
from collections import Counter
from pathlib import Path


def norm(v):
    """Comparable form of a value from Postgres or from the generator."""
    if isinstance(v, dt.datetime):
        return v.replace(tzinfo=None).isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return str(v.normalize()) if v == v.to_integral_value() else str(v)
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return None if v is None else str(v)


def row_key(row: dict, cols: list[str]) -> tuple:
    return tuple(norm(row.get(c)) for c in cols)


def multiset_diff(post: list[dict], pre: list[dict], cols: list[str]) -> Counter:
    return Counter(row_key(r, cols) for r in post) - Counter(row_key(r, cols) for r in pre)


def compare(generated: Counter, actual: Counter) -> dict:
    return {"generated": sum(generated.values()), "actual": sum(actual.values()),
            "only_generated": sum((generated - actual).values()), "only_actual": sum((actual - generated).values()),
            "match": generated == actual}


def load_generator(code_dir: Path):
    tools = str(code_dir / "tools")
    for name in ("netsuite_gen", "pg_writer"):
        sys.modules.pop(name, None)
    sys.path.insert(0, tools)
    gen, pgw = importlib.import_module("netsuite_gen"), importlib.import_module("pg_writer")
    assert Path(gen.__file__).resolve().parent == Path(tools).resolve(), "wrong generator code loaded"
    return gen, pgw


def read_state(pgw, conn, schema: str):
    """(live columns, rows per table) of one schema, read with that commit's own reader."""
    pgw.SCHEMA = schema
    pgw._table.__defaults__ = (schema,)  # the default is bound at definition time
    live = pgw.read_live_columns(conn)
    return live, pgw.read_existing(conn, live)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--code-dir", type=Path, required=True)
    p.add_argument("--pre-schema", required=True)
    p.add_argument("--post-schema", required=True)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--batch-date", type=dt.date.fromisoformat, required=True)
    p.add_argument("--config", default="increment_daily.yaml")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--profile", default="DEFAULT")
    args = p.parse_args(argv)

    gen, pgw = load_generator(args.code_dir)
    conn = pgw.connect(args.profile, "cli")
    try:
        conn.read_only = True
        live, existing = read_state(pgw, conn, args.pre_schema)
        _, post = read_state(pgw, conn, args.post_schema)
    finally:
        conn.close()

    # the same call the job makes for --increment (see netsuite_gen.main at that commit), with its job parameters
    region = gen.DRIFT_COLUMN in live.get(gen.DRIFT_TABLE, [])
    cfg = gen.load_config(str(args.code_dir / "tools" / args.config))
    g = gen.Generator(args.seed, pgw.items_from_rows(existing.get(gen.T_LINES, [])))
    result = gen.generate_increment(g, existing, gen.ScaleConfig().scaled(1.0), args.batch_date, cfg,
                                    apply_drift=False, region_present=region, update_spread_days=0,
                                    allow_before_watermark=True)

    report, ok = {}, True
    for table in gen.TABLES:
        cols = [c for c in gen.columns_for(table, region) if c in live.get(table, [])]
        rows = result.data.get(table, []) + (result.upserts.get(table, []) if table == gen.T_CUSTOMERS else [])
        report[table] = compare(Counter(row_key(r, cols) for r in rows),
                                multiset_diff(post.get(table, []), existing.get(table, []), cols))
        ok &= report[table]["match"]
        print(f"{'OK  ' if report[table]['match'] else 'DIFF'} {table}: {report[table]}")

    if not ok:
        print(f"NO GROUND TRUTH for batch {args.batch_date}: the recomputed rows differ from what the batch wrote")
        return 1
    manifest = {**result.manifest, "recomputed": {
        "pre_schema": args.pre_schema, "post_schema": args.post_schema, "code_dir_commit": args.code_dir.name,
        "verified_tables": report, "recomputed_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}}
    args.out.write_text(json.dumps(manifest, indent=2, default=gen._json_default), encoding="utf-8")
    print(f"VERIFIED batch {args.batch_date}: {sum(d['row_count'] for d in manifest['defects'])} defect rows; "
          f"manifest {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
