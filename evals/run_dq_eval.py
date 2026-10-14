"""DQ recommender eval (plan #39 step 9; design v2 section 9): live dry runs on dev bronze, scored against the
generator manifests (workspace.generator.manifests).

    python evals/run_dq_eval.py --profile DEFAULT [--runs 3] [--model databricks-...]

Each run is the real agent (python -m aidq_dq_agent's graph) as a DRY RUN: the no-nagging checks read metadata,
nothing is inserted. Every proposal it would submit is then validated in-process with the validator's own checks
(static parse, dry run on bronze, NULL check; src/aidq_dq_agent/validator.py) and scored with warehouse queries
that join bronze to the manifests. Read-only throughout: no proposal, incident or validation result is written.

Targets (design v2 section 9):
  recall        every row-expressible (table, defect) with ground truth found in >= 2 of 3 runs. "Found": a
                VALIDATED rule that fails >= 80% of that defect's manifest rows (keys; NULL-key rows by count)
  precision     >= 80% of proposals target a real defect (fail >= 50% of some manifest defect on their table)
  false rejects 0 per VALIDATED rule: failing rows whose key is in no row-expressible manifest of that table
                (reported per rule; bronze also holds unmanifested defects, e.g. the 10-01 future dates)
  unsafe        0 VALIDATED rules that fail the independent re-check (static parse and no NULL results)
  coverage      duplicates, orphans and late rows each reported as DQ_NOT_EXPRESSIBLE at least once
  repeatability >= 70% mean pairwise overlap (Jaccard) of the runs' rule sets, by (table, matched defect)

The report goes to evals/results/dq_eval_<date>.md and .json.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import itertools
import json
import pathlib
import sys
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

MANIFESTS = "workspace.generator.manifests"
# (table, defect) targets a row rule can catch, with ground truth that is observable in bronze. Excluded:
# customer_updated_before_created (observable_in_bronze false) and future dates (no manifest; design v2 section 9).
ROW_TARGETS = [
    ("netsuite_transaction_lines", "negative_amount"), ("netsuite_transaction_lines", "amount_mismatch"),
    ("netsuite_transaction_lines", "null_business_key"),
    ("netsuite_transactions", "invalid_enum"), ("netsuite_transactions", "null_business_key"),
    ("netsuite_memberships", "invalid_enum"), ("netsuite_memberships", "end_before_start"),
    ("netsuite_memberships", "null_business_key"),
    ("netsuite_certifications", "invalid_enum"), ("netsuite_certifications", "end_before_start"),
    ("netsuite_certifications", "null_business_key"),
    ("netsuite_customers", "customer_malformed_email"), ("netsuite_customers", "customer_null_company_name"),
]
NOT_EXPRESSIBLE = {"duplicate_business_key": "duplicate_business_key", "orphan_lines": "orphan_records",
                   "late_arriving": "late_arriving_rows"}       # manifest defect -> finding_type
FOUND_RECALL, PRECISE_RECALL = 0.8, 0.5
TARGETS = {"recall_runs": 2, "precision": 0.8, "false_rejects": 0, "unsafe": 0, "repeatability": 0.7}


# -- pure scoring -------------------------------------------------------------------------------------------------


def rule_recall(score: dict, manifest_totals: dict[str, int]) -> dict[str, float]:
    """defect -> share of its manifest rows this rule fails. `score`: {"caught": {defect: keys failing},
    "failing_null_key_rows": n}; null_business_key is matched by count (its rows have no key to join on)."""
    out = {}
    for defect, total in manifest_totals.items():
        if not total:
            continue
        caught = score.get("failing_null_key_rows", 0) if defect == "null_business_key" else score.get("caught", {}).get(defect, 0)
        out[defect] = min(1.0, caught / total)
    return out


def recheck(expr: str, counts: dict | None) -> bool:
    """An independent safety re-check of a VALIDATED rule, not using the validator's parser: no statement
    separator or comment, no subquery keyword, and no NULL result on bronze."""
    low = f" {expr.lower()} "
    if any(t in low for t in (";", "--", "/*", "*/")) or any(f" {w} " in low or f"({w} " in low
                                                            for w in ("select", "insert", "update", "delete", "drop")):
        return False
    return counts is not None and int(counts.get("nulls") or 0) == 0


def best_target(recall: dict[str, float]) -> tuple[str | None, float]:
    if not recall:
        return None, 0.0
    defect = max(recall, key=lambda d: (recall[d], d))
    return defect, recall[defect]


def rule_set(run: dict) -> set[tuple]:
    """A run's VALIDATED rules, by (table, matched defect) or, unmatched, (table, normalized expression)."""
    out = set()
    for p in run["proposals"]:
        if p.get("status") != "VALIDATED":
            continue
        out.add((p["table"], p["matched_defect"]) if p.get("matched_defect") and p["best_recall"] >= PRECISE_RECALL
                else (p["table"], " ".join(p["rule_expr"].lower().split())))
    return out


def jaccard(a: set, b: set) -> float:
    return 1.0 if not a and not b else len(a & b) / len(a | b)


def summarize(runs: list[dict], ground_truth: dict) -> dict:
    """Metrics and target verdicts over scored runs. `ground_truth`: {"row": {(table, defect): n}, "not_expressible":
    [defect, ...]} (keys as "table|defect" strings in JSON)."""
    found_in = {t: 0 for t in ground_truth["row"]}
    for run in runs:
        found = {(p["table"], d) for p in run["proposals"] if p.get("status") == "VALIDATED"
                 for d, r in p.get("recall", {}).items() if r >= FOUND_RECALL}
        for t in found_in:
            found_in[t] += t in found
    proposals = [p for run in runs for p in run["proposals"]]
    precise = [p for p in proposals if p.get("best_recall", 0) >= PRECISE_RECALL]
    validated = [p for p in proposals if p.get("status") == "VALIDATED"]
    false_rejects = {f"{p['table']}: {p['rule_expr']}": p.get("unexplained_rows", 0) for p in validated
                     if p.get("unexplained_rows", 0) > 0}
    unsafe = [p for p in validated if not p.get("recheck_ok", False)]
    reported = {f.get("finding_type") for run in runs for f in run["findings"]}
    coverage = {d: NOT_EXPRESSIBLE[d] in reported for d in ground_truth["not_expressible"]}
    sets = [rule_set(r) for r in runs]
    pairs = list(itertools.combinations(sets, 2))
    repeat = sum(jaccard(a, b) for a, b in pairs) / len(pairs) if pairs else None
    precision = len(precise) / len(proposals) if proposals else None
    m = {
        "runs": len(runs), "proposals": len(proposals), "validated": len(validated),
        "recall_runs": {f"{t}|{d}": n for (t, d), n in found_in.items()},
        "precision": precision, "false_reject_rules": false_rejects, "unsafe_validated": len(unsafe),
        "not_expressible_coverage": coverage, "repeatability": repeat,
    }
    need = min(TARGETS["recall_runs"], len(runs))
    m["verdicts"] = {
        "recall": all(n >= need for n in found_in.values()),
        "precision": precision is not None and precision >= TARGETS["precision"],
        "false_rejects": not false_rejects,
        "unsafe": not unsafe,
        "coverage": all(coverage.values()),
        "repeatability": repeat is not None and repeat >= TARGETS["repeatability"],
    }
    return m


def render(m: dict, runs: list[dict], meta: dict) -> str:
    v = m["verdicts"]
    ok = lambda b: "PASS" if b else "FAIL"  # noqa: E731
    lines = [f"# DQ recommender eval, dev, {meta['date']}", "",
             f"{m['runs']} live dry runs; model {meta['model']}; prompt {meta['prompt_version']}; ground truth "
             f"{MANIFESTS}. Nothing was written.", "",
             "| Metric | Result | Target | Verdict |", "|---|---|---|---|"]
    found = sum(n >= min(TARGETS["recall_runs"], m["runs"]) for n in m["recall_runs"].values())
    lines.append(f"| Recall (types found in >= 2 runs) | {found} of {len(m['recall_runs'])} | all | {ok(v['recall'])} |")
    prec = "-" if m["precision"] is None else f"{m['precision']:.0%}"
    lines.append(f"| Precision | {prec} ({m['proposals']} proposals) | >= 80% | {ok(v['precision'])} |")
    lines.append(f"| False rejects | {len(m['false_reject_rules'])} rule(s) with unexplained rows | 0 | {ok(v['false_rejects'])} |")
    lines.append(f"| Unsafe rules VALIDATED | {m['unsafe_validated']} | 0 | {ok(v['unsafe'])} |")
    cov = ", ".join(f"{d} {'yes' if c else 'no'}" for d, c in m["not_expressible_coverage"].items())
    lines.append(f"| DQ_NOT_EXPRESSIBLE coverage | {cov} | each | {ok(v['coverage'])} |")
    rep = "-" if m["repeatability"] is None else f"{m['repeatability']:.0%}"
    lines.append(f"| Repeatability | {rep} | >= 70% | {ok(v['repeatability'])} |")
    lines += ["", "## Recall per type (runs in which a VALIDATED rule failed >= 80% of the manifest rows)", ""]
    lines += [f"- {k.replace('|', ': ')}: {n} of {m['runs']}" for k, n in m["recall_runs"].items()]
    if m["false_reject_rules"]:
        lines += ["", "## Rules with failing rows outside the manifests", ""]
        lines += [f"- {k}: {n} rows" for k, n in m["false_reject_rules"].items()]
    for i, run in enumerate(runs, 1):
        lines += ["", f"## Run {i} ({run.get('model')}, {run.get('tokens')} tokens, exit {run.get('exit_code')})", ""]
        for p in run["proposals"]:
            lines.append(f"- {p['status']} {p['table']} [{p['severity']}, {p.get('defect_type')}] `{p['rule_expr']}`"
                         f" -> best {p.get('matched_defect') or '-'} {p.get('best_recall', 0):.0%}, unexplained "
                         f"{p.get('unexplained_rows', '-')}" + (f"; {p['problems']}" if p.get("problems") else ""))
        for f in run["findings"]:
            lines.append(f"- finding {f.get('table')} {f.get('finding_type')}")
        if run.get("error"):
            lines.append(f"- error: {run['error']}")
    return "\n".join(lines) + "\n"


# -- SQL (expressions are inserted only after validator.static_check passed: it is the security boundary) -----------


def score_sql(relation: str, key: str, expr: str, table: str, defects: list[str]) -> tuple[str, str]:
    lits = ", ".join("'" + d.replace("'", "''") + "'" for d in defects) or "''"
    base = (f"WITH m AS (SELECT defect, business_key FROM {MANIFESTS} WHERE table_name = '{table}' AND defect IN ({lits}) "
            f"AND business_key IS NOT NULL), b AS (SELECT CAST({key} AS STRING) AS k, NOT coalesce(({expr}), false) "
            f"AS fails FROM {relation})")
    totals = (base + " SELECT count_if(fails) AS failing_rows, count_if(fails AND k IS NULL) AS failing_null_key_rows, "
              "count_if(fails AND k IS NOT NULL AND k NOT IN (SELECT business_key FROM m)) AS unexplained_rows FROM b")
    caught = (base + " SELECT m.defect, count(DISTINCT CASE WHEN b.fails THEN m.business_key END) AS caught "
              "FROM m LEFT JOIN b ON b.k = m.business_key GROUP BY m.defect")
    return totals, caught


GROUND_TRUTH_SQL = (f"SELECT table_name, defect, count(*) AS n FROM {MANIFESTS} WHERE observable_in_bronze "
                    "GROUP BY table_name, defect")


# -- live ---------------------------------------------------------------------------------------------------------


def load_ground_truth(sql) -> dict:
    rows = {(r["table_name"], r["defect"]): int(r["n"]) for r in sql(GROUND_TRUTH_SQL)}
    return {"row": {t: rows[t] for t in ROW_TARGETS if rows.get(t)},
            "not_expressible": sorted({d for (_, d) in rows if d in NOT_EXPRESSIBLE})}


def score_proposal(p: dict, ctx: dict) -> dict:
    """Validate one proposal with the validator's checks and score it against the manifests."""
    from aidq_dq_agent import validator

    meta = ctx["tables"][p["table"]]
    static = validator.static_check(p["rule_expr"], meta["columns"])
    counts = None
    if static.ok:
        try:
            counts = ctx["sql"](validator.dry_run_sql(meta["relation"], p["rule_expr"]))[0]
        except Exception as exc:  # noqa: BLE001
            static.problems.append(f"dry run failed: {str(exc)[:200]}")
            static.ok = False
    result = validator.judge(static, counts, meta["threshold"], ctx["spark_version"])
    out = {**p, "status": result["status"], "problems": result.get("problems") or [], "counts": counts}
    if not static.ok:
        return out
    totals_sql, caught_sql = score_sql(meta["relation"], meta["key"], p["rule_expr"], p["table"], meta["defects"])
    [t] = ctx["sql"](totals_sql)
    caught = {r["defect"]: int(r["caught"] or 0) for r in ctx["sql"](caught_sql)}
    score = {"caught": caught, "failing_null_key_rows": int(t["failing_null_key_rows"] or 0)}
    recall = rule_recall(score, meta["manifest_totals"])
    defect, best = best_target(recall)
    unexplained = int(t["unexplained_rows"] or 0)
    if "null_business_key" not in meta["manifest_totals"]:
        unexplained += score["failing_null_key_rows"]
    out.update({"recall": recall, "matched_defect": defect, "best_recall": best, "failing_rows": int(t["failing_rows"] or 0),
                "unexplained_rows": unexplained, "recheck_ok": recheck(p["rule_expr"], counts)})
    return out


async def one_run(args, ctx: dict) -> dict:
    from aidq_agent.dbx import DatabricksChat
    from aidq_agent.toolbox import McpToolBox
    from aidq_dq_agent import VERSION, graph
    from aidq_dq_agent.__main__ import cli_connect
    from aidq_dq_agent.recommend import RoutedRecommender
    from aidq_dq_agent.write_tools import WriteTools
    from aidq_llm.router import Router
    from aidq_mcp.config import env_config
    from aidq_mcp.reads import Reads
    from aidq_mcp.server import DQ_TOOLS

    trace = uuid.uuid4().hex
    cfg = env_config("dev")
    wt = WriteTools(cli_connect(Reads(args.profile), cfg.metadata_endpoint), f"dq_agent@{VERSION}-eval", trace, dry_run=True)
    router = Router(DatabricksChat(args.profile), "aidq_dq_eval", pinned_model=args.model, trace_id=trace)
    async with McpToolBox(args.profile, only=DQ_TOOLS) as tb:
        state = await graph.run(tb, RoutedRecommender(router), wt, list(cfg.tables), trace_id=trace)
    rec = state.get("recommendation")
    submitted = state.get("submitted") or []
    proposals = [{k: s["item"].get(k) for k in ("table", "rule_name", "rule_expr", "severity", "defect_type")}
                 for s in submitted if s["kind"] == "proposal" and s["result"].get("status") == "ok"]
    findings = [{k: s["item"].get(k) for k in ("table", "finding_type")} for s in submitted
                if s["kind"] == "finding" and s["result"].get("status") in ("ok", "skipped")]
    return {"trace_id": trace, "exit_code": state.get("exit_code"), "model": rec.model if rec else None,
            "tokens": sum((rec.usage or {}).values()) if rec else 0, "error": rec.error if rec else "no profiles",
            "proposals": [score_proposal(p, ctx) for p in proposals], "findings": findings,
            "skipped": [s for s in submitted if s["result"].get("status") not in ("ok",)]}


def context(profile: str) -> dict:
    from aidq_mcp.config import env_config
    from aidq_mcp.reads import Reads

    reads, cfg = Reads(profile), env_config("dev")
    gt = load_ground_truth(reads.sql)
    keys = {**cfg.business_keys, **{t: "customer_internal_id" for t in cfg.full_load_tables}}
    tables = {}
    for t in cfg.tables:
        [d] = reads.pg_query(cfg.metadata_endpoint, "SELECT table_id, discard_threshold FROM aidq_metadata.source_table_def "
                             "WHERE source_table = %s", (t,))
        cols = [r["column_name"] for r in reads.pg_query(
            cfg.metadata_endpoint, "SELECT column_name FROM aidq_metadata.source_columns WHERE table_id = %s AND "
            "coalesce(is_active, true)", (d["table_id"],))]
        totals = {defect: n for (tt, defect), n in gt["row"].items() if tt == t}
        tables[t] = {"columns": cols, "key": keys[t], "relation": f"{cfg.catalog}.poc_bronze.{t}",
                     "threshold": None if d["discard_threshold"] is None else float(d["discard_threshold"]),
                     "manifest_totals": totals, "defects": sorted(totals)}
    return {"sql": reads.sql, "tables": tables, "ground_truth": gt,
            "spark_version": reads.sql("SELECT version() AS v")[0]["v"]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", required=True)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--model", default=None, help="pin a model (default: routed, task rule_generation)")
    ap.add_argument("--out", default=str(ROOT / "evals" / "results"))
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    from aidq_dq_agent.recommend import PROMPT_VERSION
    from aidq_llm.router import Router

    ctx = context(args.profile)
    runs = []
    for i in range(args.runs):
        run = asyncio.run(one_run(args, ctx))
        print(f"run {i + 1}: {len(run['proposals'])} proposals, {len(run['findings'])} findings, exit {run['exit_code']}",
              flush=True)
        runs.append(run)
    m = summarize(runs, ctx["ground_truth"])
    model = args.model or Router(None, "x").route("rule_generation").primary
    meta = {"date": dt.date.today().isoformat(), "model": model, "prompt_version": PROMPT_VERSION}
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    md = render(m, runs, meta)
    (out / f"dq_eval_{meta['date']}.md").write_text(md, encoding="utf-8")
    (out / f"dq_eval_{meta['date']}.json").write_text(json.dumps(
        {"meta": meta, "metrics": m, "runs": runs,
         "ground_truth": {"row": {f"{t}|{d}": n for (t, d), n in ctx["ground_truth"]["row"].items()},
                          "not_expressible": ctx["ground_truth"]["not_expressible"]}}, indent=2, default=str),
        encoding="utf-8")
    print(md)
    return 0 if all(m["verdicts"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
