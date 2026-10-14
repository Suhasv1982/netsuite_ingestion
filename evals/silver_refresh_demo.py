"""Before / after measurements of the silver-only refresh (plan #39 step 11; design v2 section 12 item 4). Read-only.

    python evals/silver_refresh_demo.py --profile DEFAULT --phase before        # right before the refresh job
    python evals/silver_refresh_demo.py --profile DEFAULT --phase after         # right after it
    python evals/silver_refresh_demo.py --compare --report evals/results/dq_eval_2026-10-14_v2.md

Measured on dev (warehouse queries, metadata reads):
  bronze   rows and a content checksum (sum of xxhash64(_row_hash), as DECIMAL(38,0) so it cannot overflow) per table: must be unchanged
  silver   rows per table, and per active HARD rule the silver rows that violate it
  gold     rows per materialized view
  rejects  rows per table, and per HARD rule the reject rows naming it (reason lists the failed rule names)
The comparison checks: bronze unchanged; for every HARD rule added by an approved proposal, violating rows in silver
before > 0 and after = 0, and present in rejects. It appends a section to the eval report and writes the JSON next
to it (evals/results/silver_refresh_<date>.json).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
OUT = ROOT / "evals" / "results"
GOLD = ("gold_customer_revenue", "gold_customer_status")
RULES_SQL = ("SELECT r.rule_id, d.source_table, r.rule_name, r.rule_expr, r.proposal_id FROM "
             "aidq_metadata.data_quality_rules r JOIN aidq_metadata.source_table_def d USING (table_id) "
             "WHERE r.is_active AND r.severity = 'HARD' ORDER BY r.rule_id")


def lit(v: str) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def measure(sql, rules: list[dict], catalog: str, incremental: list[str], full_load: list[str]) -> dict:
    """Every number of the snapshot (pure apart from `sql`, which runs one statement and returns rows)."""
    out = {"bronze": {}, "silver": {}, "gold": {}, "rejects": {}, "rules": []}
    for t in incremental:
        [b] = sql(f"SELECT count(*) AS n, sum(CAST(xxhash64(_row_hash) AS DECIMAL(38, 0))) AS checksum FROM {catalog}.poc_bronze.{t}")
        out["bronze"][t] = {"rows": int(b["n"]), "checksum": str(b["checksum"])}
        [s] = sql(f"SELECT count(*) AS n FROM {catalog}.poc_silver.{t}")
        out["silver"][t] = int(s["n"])
    for t in full_load:
        [b] = sql(f"SELECT count(*) AS n FROM {catalog}.poc_bronze.{t}")
        out["bronze"][t] = {"rows": int(b["n"]), "checksum": None}
    for g in GOLD:
        [r] = sql(f"SELECT count(*) AS n FROM {catalog}.poc_gold.{g}")
        out["gold"][g] = int(r["n"])
    for r in sql(f"SELECT source_table, count(*) AS n FROM {catalog}.poc_reject.rejected_rows GROUP BY 1"):
        out["rejects"][r["source_table"]] = int(r["n"])
    for rule in rules:
        t = rule["source_table"]
        silver = None
        if t in incremental:
            [v] = sql(f"SELECT count_if(NOT coalesce(({rule['rule_expr']}), false)) AS n FROM {catalog}.poc_silver.{t}")
            silver = int(v["n"])
        [rj] = sql(f"SELECT count(*) AS n FROM {catalog}.poc_reject.rejected_rows WHERE source_table = {lit(t)} AND "
                   f"array_contains(split(reason, ', '), {lit(rule['rule_name'])})")
        out["rules"].append({"rule_id": rule["rule_id"], "table": t, "rule_name": rule["rule_name"],
                             "proposal_id": rule["proposal_id"], "silver_violations": silver,
                             "reject_rows": int(rj["n"])})
    return out


def compare(before: dict, after: dict) -> dict:
    """Verdicts and the before/after table rows."""
    bronze_same = before["bronze"] == after["bronze"]
    by_id = {r["rule_id"]: r for r in before["rules"]}
    rules = []
    for r in after["rules"]:
        b = by_id.get(r["rule_id"], {})
        new = r["proposal_id"] is not None
        ok = None
        if new and r["silver_violations"] is not None:
            ok = (b.get("silver_violations") or 0) > 0 and r["silver_violations"] == 0 and r["reject_rows"] > 0
        rules.append({**r, "silver_before": b.get("silver_violations"), "rejects_before": b.get("reject_rows"),
                      "from_proposal": new, "moved": ok})
    moved = [r for r in rules if r["from_proposal"] and r["silver_violations"] is not None]
    return {"bronze_unchanged": bronze_same, "rules": rules,
            "violating_rows_moved": bool(moved) and all(r["moved"] for r in moved)}


def render(before: dict, after: dict, c: dict) -> str:
    ok = lambda b: "PASS" if b else "FAIL"  # noqa: E731
    lines = [f"## Silver-only refresh (done-when item 4), {after['date']}", "",
             f"Before: {before['taken_at']}. After: {after['taken_at']}. Job `silver_refresh` (refresh_credentials, then a "
             "full refresh of the four silver tables with rejects and gold recomputed; bronze not in the selection).", "",
             f"- Bronze unchanged (rows and content checksum per table): **{ok(c['bronze_unchanged'])}**",
             f"- Existing violating rows of the new HARD rules moved from silver to rejects: "
             f"**{ok(c['violating_rows_moved'])}**", "",
             "| Layer | Table | Before | After |", "|---|---|---|---|"]
    for t, v in after["bronze"].items():
        lines.append(f"| bronze | {t} | {before['bronze'][t]['rows']} | {v['rows']} |")
    for t, v in after["silver"].items():
        lines.append(f"| silver | {t} | {before['silver'][t]} | {v} |")
    for g, v in after["gold"].items():
        lines.append(f"| gold | {g} | {before['gold'].get(g)} | {v} |")
    for t in sorted(set(before["rejects"]) | set(after["rejects"])):
        lines.append(f"| rejects | {t} | {before['rejects'].get(t, 0)} | {after['rejects'].get(t, 0)} |")
    lines += ["", "| HARD rule | Table | From proposal | Silver violations before | after | Reject rows before | after | Moved |",
              "|---|---|---|---|---|---|---|---|"]
    for r in c["rules"]:
        lines.append(f"| {r['rule_id']} {r['rule_name']} | {r['table']} | {r['proposal_id'] or '-'} | "
                     f"{r['silver_before'] if r['silver_before'] is not None else '-'} | "
                     f"{r['silver_violations'] if r['silver_violations'] is not None else '-'} | {r['rejects_before']} | "
                     f"{r['reject_rows']} | {'-' if r['moved'] is None else ok(r['moved'])} |")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", default=None)
    ap.add_argument("--phase", choices=["before", "after"])
    ap.add_argument("--compare", action="store_true")
    ap.add_argument("--report", help="eval report to append the section to (with --compare)")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    OUT.mkdir(parents=True, exist_ok=True)
    if args.phase:
        from aidq_mcp.config import env_config
        from aidq_mcp.reads import Reads

        reads, cfg = Reads(args.profile), env_config("dev")
        rules = reads.pg_query(cfg.metadata_endpoint, RULES_SQL)
        snap = measure(reads.sql, rules, cfg.catalog, list(cfg.business_keys), list(cfg.full_load_tables))
        now = dt.datetime.now(dt.timezone.utc)
        snap.update({"phase": args.phase, "taken_at": now.isoformat(timespec="seconds"), "date": now.date().isoformat()})
        path = OUT / f"silver_refresh_{args.phase}.json"
        path.write_text(json.dumps(snap, indent=2), encoding="utf-8")
        print(f"wrote {path.relative_to(ROOT)}")
        print(json.dumps({k: snap[k] for k in ("silver", "gold", "rejects", "rules")}, indent=1))
        return 0
    if args.compare:
        before = json.loads((OUT / "silver_refresh_before.json").read_text(encoding="utf-8"))
        after = json.loads((OUT / "silver_refresh_after.json").read_text(encoding="utf-8"))
        c = compare(before, after)
        md = render(before, after, c)
        print(md)
        (OUT / f"silver_refresh_{after['date']}.json").write_text(
            json.dumps({"before": before, "after": after, "comparison": c}, indent=2), encoding="utf-8")
        if args.report:
            with open(args.report, "a", encoding="utf-8") as f:
                f.write("\n" + md)
        return 0 if c["bronze_unchanged"] and c["violating_rows_moved"] else 1
    ap.error("--phase or --compare")
    return 2


if __name__ == "__main__":
    sys.exit(main())
