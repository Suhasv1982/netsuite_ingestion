"""Verify a committed generator manifest against today's source and keep only the defects that still hold.

    python tools/verify_manifest.py baseline/manifest_defects.json --out baseline/raw/manifests/verified_20260711.json \
        --profile DEFAULT

Read-only (the source connection is read only; a disabled endpoint is re-enabled first, like daily_check). The
baseline manifests (07-11 init, 08-22 increment) describe batches written before the daily runs, and the source
changed since (customers are upserted in place; later batches append new versions). A defect counts as ground
truth for the DQ recommender eval only where it still holds today (docs/phase5_dq_recommender_plan.md, step 1).

Per defect type, for each listed business key:
  invalid_enum            a row of that key has a value from netsuite_gen.INVALID_ENUM_VALUES[column]
  end_before_start        a row of that key has end < start
  negative_amount         a row of that key has amount < 0
  amount_mismatch         a row of that key has amount != quantity * rate
  orphan_lines            a row of that key references a transaction_internal_id that no transaction has
  duplicate_business_key  the key has two rows on one updated_date day whose created_date differs
  late_arriving           a row of that key with the listed created_date and updated_date exists
  customer_*              the customer's current row still has the defect (upserts can remove it)
  null_business_key       keys were nulled, so rows cannot be told apart: verified as a count of NULL-key rows
The output keeps each entry with only its verified keys and a `verification` block (total, verified, method).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import daily_check  # noqa: E402  (pg(): read-only connect with endpoint wake-up)
import netsuite_gen as ng  # noqa: E402

EMAIL_OK = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s.]+$")


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def check_entry(conn, entry: dict) -> tuple[list, str]:
    """(verified keys, method) for one manifest entry."""
    defect, table = entry["defect"], entry["table"]
    key = ng.BUSINESS_KEY[table]
    keys = [k for k in entry.get("business_keys", []) if k is not None]
    t = f"netsuite.{q(table)}"

    def keys_where(cond: str, params: tuple = ()) -> list:
        if not keys:
            return []
        rows = conn.execute(f"SELECT DISTINCT {q(key)} FROM {t} WHERE {q(key)} = ANY(%s) AND ({cond})",
                            (keys, *params)).fetchall()
        found = {r[0] for r in rows}
        return [k for k in keys if k in found]

    if defect == "invalid_enum":
        col = entry["column"]
        return keys_where(f"{q(col)} = ANY(%s)", (ng.INVALID_ENUM_VALUES[col],)), f"{col} has an invalid value"
    if defect == "end_before_start":
        start, end = entry["column"].split("/")
        return keys_where(f"{q(end)} < {q(start)}"), f"{end} < {start}"
    if defect == "negative_amount":
        return keys_where("amount < 0"), "amount < 0"
    if defect == "amount_mismatch":
        return keys_where("amount <> quantity * rate"), "amount <> quantity * rate"
    if defect == "orphan_lines":   # qualified on both sides: an unqualified column would bind to the subquery
        rows = conn.execute(
            f"SELECT DISTINCT o.{q(key)} FROM {t} o WHERE o.{q(key)} = ANY(%s) AND NOT EXISTS ("
            "SELECT 1 FROM netsuite.netsuite_transactions x WHERE x.transaction_internal_id = o.transaction_internal_id)",
            (keys,)).fetchall()
        found = {r[0] for r in rows}
        return [k for k in keys if k in found], "no transaction with that transaction_internal_id"
    if defect == "duplicate_business_key":
        rows = conn.execute(
            f"SELECT {q(key)} FROM {t} WHERE {q(key)} = ANY(%s) GROUP BY {q(key)}, updated_date::date "
            "HAVING count(DISTINCT created_date) > 1", (keys,)).fetchall()
        found = {r[0] for r in rows}
        return [k for k in keys if k in found], "two rows on one updated_date day with different created_date"
    if defect == "late_arriving":
        found = []
        for r in entry.get("rows", []):
            hit = conn.execute(f"SELECT 1 FROM {t} WHERE {q(key)} = %s AND created_date::date = %s "
                               "AND updated_date::date = %s LIMIT 1",
                               (r["key"], dt.date.fromisoformat(r["created_date"]),
                                dt.date.fromisoformat(r["updated_date"]))).fetchone()
            if hit:
                found.append(r["key"])
        return found, "row with the listed created_date and updated_date exists"
    if defect == "customer_null_company_name":
        return keys_where("company_name IS NULL"), "current row: company_name IS NULL"
    if defect == "customer_malformed_email":
        rows = conn.execute(f"SELECT {q(key)}, email FROM {t} WHERE {q(key)} = ANY(%s)", (keys,)).fetchall()
        bad = {k for k, e in rows if e is None or not EMAIL_OK.match(e)}
        return [k for k in keys if k in bad], "current row: email is malformed"
    if defect == "customer_updated_before_created":
        return keys_where("updated_date < created_date"), "current row: updated_date < created_date"
    if defect == "null_business_key":
        n = conn.execute(f"SELECT count(*) FROM {t} WHERE {q(key)} IS NULL").fetchone()[0]
        total = len(entry.get("business_keys", []))
        return (list(entry["business_keys"]) if n >= total else []), \
            f"count: {n} NULL-key rows in the table, {total} injected (rows cannot be matched to keys)"
    raise ValueError(f"unknown defect type {defect!r}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("manifest", type=Path)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--profile", default="DEFAULT")
    args = p.parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    out_defects = []
    with daily_check.pg(args.profile, daily_check.SOURCE_ENDPOINT) as conn:
        conn.read_only = True
        for entry in manifest["defects"]:
            verified, method = check_entry(conn, entry)
            total = len(entry.get("business_keys", []))
            print(f"{entry['defect']:<34} {entry['table'].replace('netsuite_', ''):<18} {len(verified):>4}/{total:<4} {method}")
            if verified:
                kept = {**entry, "business_keys": verified, "row_count": len(verified),
                        "verification": {"total": total, "verified": len(verified), "method": method}}
                if "rows" in entry:
                    kept["rows"] = [r for r in entry["rows"] if r.get("key") in set(verified)]
                out_defects.append(kept)
        conn.rollback()
    verified_manifest = {**manifest, "defects": out_defects,
                         "verified": {"source_manifest": args.manifest.name,
                                      "verified_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(verified_manifest, indent=2, default=str), encoding="utf-8")
    print(f"wrote {args.out}: {sum(d['row_count'] for d in out_defects)} verified defect rows in {len(out_defects)} entries")
    return 0


if __name__ == "__main__":
    sys.exit(main())
