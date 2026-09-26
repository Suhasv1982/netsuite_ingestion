"""Classify every injected defect row by what the pipeline did with it.

Inputs: a generator manifest (which rows carry which defect) and a snapshot
from collect_metrics.py (which business keys reached silver, which were
rejected, and whether the run failed). Pure functions, no I/O apart from main().

Outcomes for a defect row
-------------------------
rejected            a HARD DQ rule sent it to poc_reject.rejected_rows
passed_to_silver    it reached poc_silver unchanged by the pipeline
silently_absorbed   no error, but it is in neither silver nor rejects: merged
                    away by the AUTO CDC key/sequence merge, or skipped
caused_failure      the run failed and the error names this table
unresolved          the run failed and the error does not name this table
bronze_only         customers: FullLoad, no DQ rules, no silver table
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

OUTCOMES = ["rejected", "passed_to_silver", "silently_absorbed", "caused_failure", "unresolved", "bronze_only"]
SILVER_TABLES = ["netsuite_memberships", "netsuite_certifications", "netsuite_transactions", "netsuite_transaction_lines"]


def _silver_index(snapshot: dict, table: str):
    entry = (snapshot.get("row_state") or {}).get("silver", {}).get(table)
    if not entry or "rows" not in entry:
        return None
    by_key: dict = defaultdict(list)
    null_rows = 0
    for key, created, updated in entry["rows"]:
        if key is None:
            null_rows += 1
        else:
            by_key[str(key)].append((created, updated))
    return by_key, null_rows


def _reject_index(snapshot: dict, table: str):
    entry = (snapshot.get("row_state") or {}).get("rejected", {}).get(table)
    if entry is None or isinstance(entry, dict):
        return None
    by_key: dict = defaultdict(list)
    null_rows = 0
    for reason, key in entry:
        if key is None:
            null_rows += 1
        else:
            by_key[str(key)].append(reason)
    return by_key, null_rows


def failure_tables(snapshot: dict) -> set[str]:
    """Silver/incremental tables named in the run's error events (empty if the run succeeded)."""
    if (snapshot.get("pipeline") or {}).get("state") == "COMPLETED" and (snapshot.get("job") or {}).get("state") == "SUCCESS":
        return set()
    text = json.dumps((snapshot.get("pipeline") or {}).get("error_events", [])) + str((snapshot.get("job") or {}).get("message"))
    return {t for t in SILVER_TABLES if t in text}


def run_failed(snapshot: dict) -> bool:
    return not ((snapshot.get("pipeline") or {}).get("state") == "COMPLETED" and (snapshot.get("job") or {}).get("state") == "SUCCESS")


def classify_entry(entry: dict, snapshot: dict) -> dict:
    """Return {outcome: row_count} for one manifest defect entry."""
    table, n, defect = entry["table"], entry["row_count"], entry["defect"]
    counts: Counter = Counter()

    if table == "netsuite_customers":
        counts["bronze_only"] = n
        return dict(counts)

    silver, rejects = _silver_index(snapshot, table), _reject_index(snapshot, table)
    failed, failed_tables = run_failed(snapshot), failure_tables(snapshot)

    def unresolved(k=1):
        counts["caused_failure" if table in failed_tables else "unresolved" if failed else "silently_absorbed"] += k

    if silver is None or rejects is None:
        counts["unresolved"] = n
        return dict(counts)
    silver_by_key, silver_nulls = silver
    reject_by_key, reject_nulls = rejects

    if defect == "null_business_key":
        passed = min(n, silver_nulls)
        rejected = min(n - passed, reject_nulls)
        counts["passed_to_silver"], counts["rejected"] = passed, rejected
        unresolved(n - passed - rejected)
    elif defect == "duplicate_business_key":
        for key in map(str, entry["business_keys"]):
            in_silver = len(silver_by_key.get(key, []))
            if key in reject_by_key:
                counts["rejected"] += 1
            elif in_silver >= 2:
                counts["passed_to_silver"] += 1  # both copies survived
            elif in_silver == 1:
                counts["silently_absorbed"] += 1  # the pair collapsed to one row
            else:
                unresolved()
    else:
        for key in map(str, entry["business_keys"]):
            if key in reject_by_key:
                counts["rejected"] += 1
            elif key in silver_by_key:
                counts["passed_to_silver"] += 1
            else:
                unresolved()
    return dict(counts)


def classify_manifest(manifest: dict, snapshot: dict) -> list[dict]:
    out = []
    for entry in manifest["defects"]:
        outcomes = classify_entry(entry, snapshot)
        assert sum(outcomes.values()) == entry["row_count"], (entry["defect"], entry["table"], outcomes)
        out.append({
            "defect": entry["defect"], "table": entry["table"], "column": entry.get("column"),
            "rows": entry["row_count"], "outcomes": outcomes,
            "observable_in_bronze": entry.get("observable_in_bronze", True),
        })
    return out


def aggregate(classified: list[dict], by: str) -> dict:
    """by = 'defect' or 'table' -> {name: {outcome: rows, 'rows': total}}"""
    agg: dict = defaultdict(lambda: Counter())
    for row in classified:
        for outcome, n in row["outcomes"].items():
            agg[row[by]][outcome] += n
        agg[row[by]]["rows"] += row["rows"]
    return {k: dict(v) for k, v in agg.items()}


def main(argv=None):
    manifest_path, snapshot_path, out_path = (Path(p) for p in (argv or sys.argv[1:4]))
    manifest, snapshot = json.loads(manifest_path.read_text()), json.loads(snapshot_path.read_text())
    classified = classify_manifest(manifest, snapshot)
    result = {"rows": classified, "by_defect": aggregate(classified, "defect"), "by_table": aggregate(classified, "table")}
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
