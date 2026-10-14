"""DQ recommender eval (plan #39 step 9; design v2 section 9): live dry runs on dev bronze, scored against the
generator manifests (workspace.generator.manifests).

    python evals/run_dq_eval.py --profile DEFAULT [--runs 3] [--model databricks-...]

Each run is the real agent (python -m aidq_dq_agent's graph) as a DRY RUN: the no-nagging checks read metadata,
nothing is inserted. Every proposal it would submit is then validated in-process with the validator's own checks
(static parse, dry run on bronze, NULL check; src/aidq_dq_agent/validator.py) and scored with warehouse queries
that join bronze to the manifests. Read-only throughout: no proposal, incident or validation result is written.

Targets (design v2 section 9):
  recall        every row-expressible (table, defect) with ground truth found in >= 2 of 3 runs. "Found": a
                VALIDATED rule that fails >= 80% of that defect's manifest rows (keys; NULL-key rows by count).
                A defect an existing active rule already catches the same way is "covered": proposing it again
                would be nagging (design v2 section 5), so it is reported but not required
  precision     >= 80% of proposals target a real defect (fail >= 50% of some manifest defect on their table)
  false rejects 0 per VALIDATED rule: failing rows whose key is in no manifest of that table, counted only
                for keys whose first failing version is in a batch with known ground truth (below)
  unsafe        0 VALIDATED rules that fail the independent re-check (static parse and no NULL results)
  coverage      duplicates, orphans and late rows each reported as DQ_NOT_EXPRESSIBLE at least once
  repeatability >= 70% mean pairwise overlap (Jaccard) of the runs' rule sets, by (table, matched defect)

Known ground truth (design v2 section 9: "evaluate only on batches with known ground truth"): the batch dates that
have a manifest, minus PARTIAL_BATCHES. Bronze also holds rows from writes without a manifest (the 09-14 increment,
the 10-01 run, the 10-09..10-13 batches); a defect those writes introduced is neither a hit nor a false reject. A
key is attributed to the first snapshot in which the rule fails it, because the generator re-emits existing keys
with their latest values: a defect injected by an unmanifested write reappears in later, manifested snapshots.
Recall counts only manifest rows of known batches; rows outside the window are reported as "unknown ground truth".

The report goes to evals/results/dq_eval_<date>_<prompt version>.md and .json.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import itertools
import json
import os
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
# Batch dates that have a manifest but also hold rows from another, unmanifested write: the 10-01 run left rows
# dated 10-02 and 10-03 (plan #39 section 1, item 6). Checked 2026-10-14: the 'N/A' types first seen on 10-02 are
# not among the values the 10-02 batch injected.
PARTIAL_BATCHES = ("2026-10-02", "2026-10-03")
# Full-load tables (customers) hold only the current row, and later batches update customers: a defect the 07-11
# manifest recorded may have been overwritten since (2 of the 5 NULL company names on 2026-10-14). For these, a
# manifest row is ground truth only while the current row still shows the defect, by the check the manifest's own
# verification names ("current row: company_name IS NULL", "current row: email is malformed").
STILL_PRESENT = {
    "customer_null_company_name": "company_name IS NULL",
    "customer_malformed_email": "email IS NOT NULL AND NOT email rlike '{email}'",
}
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
    [defect, ...], "covered": {(table, defect): "rule ..."}} (keys as "table|defect" strings in JSON)."""
    covered = ground_truth.get("covered") or {}
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
        "checklist_complete_runs": sum(1 for r in runs if r.get("exit_code") not in (1, 4)),
        "unknown_ground_truth_rows": {f"{p['table']}: {p['rule_expr']}": p["unknown_rows"] for p in validated
                                      if p.get("unknown_rows")},
        "recall_runs": {f"{t}|{d}": n for (t, d), n in found_in.items()},
        "covered_by_existing_rule": {f"{t}|{d}": r for (t, d), r in covered.items() if (t, d) in found_in},
        "precision": precision, "false_reject_rules": false_rejects, "unsafe_validated": len(unsafe),
        "not_expressible_coverage": coverage, "repeatability": repeat,
    }
    need = min(TARGETS["recall_runs"], len(runs))
    m["verdicts"] = {
        "recall": all(n >= need or t in covered for t, n in found_in.items()),
        "precision": precision is not None and precision >= TARGETS["precision"],
        "false_rejects": not false_rejects,
        "unsafe": not unsafe,
        "coverage": all(coverage.values()),
        "repeatability": repeat is not None and repeat >= TARGETS["repeatability"],
    }
    return m


SCORING_RULES = [
    "Ground truth: workspace.generator.manifests, only batches with known ground truth: the manifested batch dates "
    "minus 10-02 and 10-03, which also hold rows the 10-01 run wrote without a manifest. Rows from unmanifested writes "
    "(09-14 increment, 10-01 run, 10-09..10-13) are neither hits nor false rejects; they are listed as unknown ground "
    "truth.",
    "Attribution: a key belongs to the first snapshot in which the rule fails it (the generator re-emits keys with "
    "their latest values, so a defect carries into later snapshots).",
    "Customers (full load, current state only): a manifest row counts while the current row still shows its defect.",
    "Existing-rule coverage: a defect type an active rule already fails on >= 80% of its manifest rows is covered; "
    "proposing it again would be nagging (design v2 section 5), so it is reported but not required for recall.",
    "Found: a VALIDATED rule fails >= 80% of a type's manifest rows (NULL-key rows by count). Precise: a proposal "
    "fails >= 50% of some manifest type on its table, whatever its validation status.",
    "NULL counts as a failure (like dq.py's HARD rules: coalesce((expr), false)).",
]


def imprecise_reason(p: dict) -> str:
    """Why a proposal does not count as precise."""
    if p.get("status") == "VALIDATION_FAILED" and not p.get("recall"):
        return "validation failed before scoring: " + "; ".join(p.get("problems") or [])
    if not p.get("failing_rows"):
        return "fails no bronze rows"
    best = p.get("matched_defect")
    known = p.get("failing_rows", 0) - p.get("unknown_rows", 0)
    if best is None or p.get("best_recall", 0) == 0:
        return (f"no manifest defect type covers this check ({p.get('failing_rows')} failing rows, "
                f"{p.get('unknown_rows', 0)} of them from batches without a manifest)")
    return (f"best match {best} at {p['best_recall']:.0%} of its manifest rows (needs 50%); "
            f"{known} failing rows in known batches")


def render(m: dict, runs: list[dict], meta: dict) -> str:
    v = m["verdicts"]
    ok = lambda b: "PASS" if b else "FAIL"  # noqa: E731
    lines = [f"# DQ recommender eval, dev, {meta['date']}", "",
             f"{m['runs']} live dry runs; model {meta['model']}; prompt {meta['prompt_version']}; ground truth "
             f"{MANIFESTS}, batches {', '.join(meta.get('known_batches') or [])}. Nothing was written.",
             f"Runs with a complete candidate checklist: {m.get('checklist_complete_runs')} of {m['runs']}.", "",
             "| Metric | Result | Target | Verdict |", "|---|---|---|---|"]
    covered = m.get("covered_by_existing_rule") or {}
    need = min(TARGETS["recall_runs"], m["runs"])
    found = sum(n >= need for k, n in m["recall_runs"].items() if k not in covered)
    lines.append(f"| Recall (types found in >= 2 runs) | {found} of {len(m['recall_runs']) - len(covered)} not yet covered "
                 f"(+ {len(covered)} covered by existing rules) | all | {ok(v['recall'])} |")
    prec = "-" if m["precision"] is None else f"{m['precision']:.0%}"
    lines.append(f"| Precision | {prec} ({m['proposals']} proposals) | >= 80% | {ok(v['precision'])} |")
    lines.append(f"| False rejects | {len(m['false_reject_rules'])} rule(s) with unexplained rows | 0 | {ok(v['false_rejects'])} |")
    lines.append(f"| Unsafe rules VALIDATED | {m['unsafe_validated']} | 0 | {ok(v['unsafe'])} |")
    cov = ", ".join(f"{d} {'yes' if c else 'no'}" for d, c in m["not_expressible_coverage"].items())
    lines.append(f"| DQ_NOT_EXPRESSIBLE coverage | {cov} | each | {ok(v['coverage'])} |")
    rep = "-" if m["repeatability"] is None else f"{m['repeatability']:.0%}"
    lines.append(f"| Repeatability | {rep} | >= 70% | {ok(v['repeatability'])} |")
    lines += ["", "## Recall per type (runs in which a VALIDATED rule failed >= 80% of the manifest rows)", ""]
    lines += [f"- {k.replace('|', ': ')}: {n} of {m['runs']}" + (f" (covered by existing {covered[k]})" if k in covered else "")
              for k, n in m["recall_runs"].items()]
    if m["false_reject_rules"]:
        lines += ["", "## Rules with failing rows outside the manifests", ""]
        lines += [f"- {k}: {n} rows" for k, n in m["false_reject_rules"].items()]
    imprecise = [(i, p) for i, run in enumerate(runs, 1) for p in run["proposals"]
                 if p.get("best_recall", 0) < PRECISE_RECALL]
    if imprecise:
        lines += ["", f"## Proposals counted as imprecise ({len(imprecise)})", ""]
        lines += [f"- run {i}: {p['table']} [{p.get('candidate_id')}] `{p['rule_expr']}`: {imprecise_reason(p)}"
                  for i, p in imprecise]
    lines += ["", "## Scoring rules", ""] + [f"- {r}" for r in SCORING_RULES]
    if m.get("unknown_ground_truth_rows"):
        lines += ["", "## Failing rows with unknown ground truth (first failing in a batch without a manifest)", ""]
        lines += [f"- {k}: {n} rows" for k, n in m["unknown_ground_truth_rows"].items()]
    for i, run in enumerate(runs, 1):
        lines += ["", f"## Run {i} ({run.get('model')}, {run.get('tokens')} tokens, exit {run.get('exit_code')})", ""]
        for p in run["proposals"]:
            lines.append(f"- {p['status']} {p['table']} [{p['severity']}, {p.get('defect_type')}, "
                         f"{p.get('candidate_id')}] `{p['rule_expr']}`"
                         f" -> best {p.get('matched_defect') or '-'} {p.get('best_recall', 0):.0%}, unexplained "
                         f"{p.get('unexplained_rows', '-')}" + (f"; {p['problems']}" if p.get("problems") else ""))
        for f in run["findings"]:
            lines.append(f"- finding {f.get('table')} {f.get('finding_type')}")
        if run.get("dismissed") is not None:
            lines.append(f"- {len(run['dismissed'])} candidate(s) dismissed")
        if run.get("error"):
            lines.append(f"- error: {run['error']}")
    return "\n".join(lines) + "\n"


# -- SQL (expressions are inserted only after validator.static_check passed: it is the security boundary) -----------


def _lits(values) -> str:
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values) or "''"


def score_sql(relation: str, key: str, expr: str, table: str, defects: list[str], known: list[str],
              snapshots: bool = True) -> tuple[str, str]:
    """(totals, caught). m_known: the target defects' manifest keys from known batches (recall). m_any: every
    manifest key of the table, any batch (a failing row whose key is in it is explained). first: the first
    snapshot in which the rule fails a key; only keys first failing in a known batch can be false rejects.
    `snapshots` False (a full-load table: bronze holds the current state, no _snapshot_date): a row's batch is
    unknown, so a failing row outside the manifests is unknown ground truth, never a false reject."""
    day = "cast(_snapshot_date AS string)" if snapshots else "CAST(NULL AS STRING)"
    present = "" if snapshots else f" AND {still_present_filter(relation, key, defects)}"
    base = (f"WITH m_known AS (SELECT defect, business_key FROM {MANIFESTS} WHERE table_name = '{table}' AND defect IN "
            f"({_lits(defects)}) AND business_key IS NOT NULL AND cast(batch_date AS string) IN ({_lits(known)}){present}), "
            f"m_any AS (SELECT DISTINCT business_key FROM {MANIFESTS} WHERE table_name = '{table}' AND business_key IS "
            f"NOT NULL), b AS (SELECT CAST({key} AS STRING) AS k, {day} AS d, "
            f"NOT coalesce(({expr}), false) AS fails FROM {relation}), "
            "first AS (SELECT k, min(d) AS fd FROM b WHERE fails AND k IS NOT NULL GROUP BY k)")
    totals = (base + " SELECT count_if(b.fails) AS failing_rows, "
              f"count_if(b.fails AND b.k IS NULL AND b.d IN ({_lits(known)})) AS failing_null_key_rows, "
              f"count_if(b.fails AND b.k IS NOT NULL AND b.k NOT IN (SELECT business_key FROM m_any) AND f.fd IN "
              f"({_lits(known)})) AS unexplained_rows, "
              f"count_if(b.fails AND (b.k IS NULL AND NOT coalesce(b.d IN ({_lits(known)}), false) OR b.k IS NOT NULL "
              f"AND b.k NOT IN (SELECT business_key FROM m_any) AND NOT coalesce(f.fd IN ({_lits(known)}), false))) "
              "AS unknown_rows "
              "FROM b LEFT JOIN first f ON b.k = f.k")
    caught = (base + " SELECT m.defect, count(DISTINCT CASE WHEN b.fails THEN m.business_key END) AS caught "
              "FROM m_known m LEFT JOIN b ON b.k = m.business_key GROUP BY m.defect")
    return totals, caught


def still_present_filter(relation: str, key: str, defects: list[str]) -> str:
    """For a full-load table: keep a manifest row only while its current row still shows its defect."""
    from aidq_mcp.dq_tools import EMAIL_PATTERN

    arms = " ".join(f"WHEN '{d}' THEN business_key IN (SELECT CAST({key} AS STRING) FROM {relation} WHERE "
                    f"{STILL_PRESENT[d].format(email=EMAIL_PATTERN)})" for d in defects if d in STILL_PRESENT)
    return f"(CASE defect {arms} ELSE true END)" if arms else "true"


def full_load_totals_sql(table: str, relation: str, key: str, defects: list[str], known: list[str]) -> str:
    return (f"SELECT defect, count(*) AS n FROM {MANIFESTS} WHERE table_name = '{table}' AND observable_in_bronze AND "
            f"defect IN ({_lits(defects)}) AND cast(batch_date AS string) IN ({_lits(known)}) AND "
            f"{still_present_filter(relation, key, defects)} GROUP BY defect")


def known_batches(batch_dates: list[str]) -> list[str]:
    return sorted(d for d in {str(x) for x in batch_dates} if d not in PARTIAL_BATCHES)


def ground_truth_sql(known: list[str]) -> str:
    return (f"SELECT table_name, defect, count(*) AS n FROM {MANIFESTS} WHERE observable_in_bronze AND "
            f"cast(batch_date AS string) IN ({_lits(known)}) GROUP BY table_name, defect")


# -- live ---------------------------------------------------------------------------------------------------------


def load_ground_truth(sql) -> dict:
    known = known_batches([r["d"] for r in sql(f"SELECT DISTINCT cast(batch_date AS string) AS d FROM {MANIFESTS}")])
    rows = {(r["table_name"], r["defect"]): int(r["n"]) for r in sql(ground_truth_sql(known))}
    return {"row": {t: rows[t] for t in ROW_TARGETS if rows.get(t)}, "known_batches": known,
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
    totals_sql, caught_sql = score_sql(meta["relation"], meta["key"], p["rule_expr"], p["table"], meta["defects"],
                                       ctx["ground_truth"]["known_batches"], meta["snapshots"])
    [t] = ctx["sql"](totals_sql)
    caught = {r["defect"]: int(r["caught"] or 0) for r in ctx["sql"](caught_sql)}
    score = {"caught": caught, "failing_null_key_rows": int(t["failing_null_key_rows"] or 0)}
    recall = rule_recall(score, meta["manifest_totals"])
    defect, best = best_target(recall)
    unexplained = int(t["unexplained_rows"] or 0)
    if "null_business_key" not in meta["manifest_totals"]:
        unexplained += score["failing_null_key_rows"]
    out.update({"recall": recall, "matched_defect": defect, "best_recall": best, "failing_rows": int(t["failing_rows"] or 0),
                "unexplained_rows": unexplained, "unknown_rows": int(t["unknown_rows"] or 0), "recheck_ok": recheck(p["rule_expr"], counts)})
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
    proposals = [{k: s["item"].get(k) for k in ("table", "rule_name", "rule_expr", "severity", "defect_type", "candidate_id")}
                 for s in submitted if s["kind"] == "proposal" and s["result"].get("status") == "ok"]
    findings = [{k: s["item"].get(k) for k in ("table", "finding_type")} for s in submitted
                if s["kind"] == "finding" and s["result"].get("status") in ("ok", "skipped")]
    return {"trace_id": trace, "exit_code": state.get("exit_code"), "model": rec.model if rec else None,
            "tokens": sum((rec.usage or {}).values()) if rec else 0, "error": rec.error if rec else "no profiles",
            "proposals": [score_proposal(p, ctx) for p in proposals], "findings": findings,
            "skipped": [s for s in submitted if s["result"].get("status") not in ("ok",)],
            "dismissed": ((rec.result or {}).get("dismissed") if rec else None),
            "unanswered": rec.unanswered if rec else []}


ACTIVE_RULES_SQL = ("SELECT d.source_table, r.rule_id, r.rule_name, r.rule_expr FROM aidq_metadata.data_quality_rules r "
                    "JOIN aidq_metadata.source_table_def d USING (table_id) WHERE r.is_active ORDER BY r.rule_id")


def covered_targets(rules: list[dict], tables: dict, sql, known: list[str]) -> dict:
    """(table, defect) -> "rule <id> <name>" for each target an active rule already fails on >= FOUND_RECALL of its
    manifest rows. Existing rules are live configuration (applied through governance), scored as they are."""
    out = {}
    for r in rules:
        meta = tables.get(r["source_table"])
        if not meta or not meta["defects"]:
            continue
        totals_sql, caught_sql = score_sql(meta["relation"], meta["key"], r["rule_expr"], r["source_table"],
                                           meta["defects"], known, meta["snapshots"])
        [t] = sql(totals_sql)
        caught = {x["defect"]: int(x["caught"] or 0) for x in sql(caught_sql)}
        recall = rule_recall({"caught": caught, "failing_null_key_rows": int(t["failing_null_key_rows"] or 0)},
                             meta["manifest_totals"])
        for d, rc in recall.items():
            if rc >= FOUND_RECALL:
                out.setdefault((r["source_table"], d), f"rule {r['rule_id']} {r['rule_name']}")
    return out


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
        if t in cfg.full_load_tables and totals:
            totals = {r["defect"]: int(r["n"]) for r in reads.sql(full_load_totals_sql(
                t, f"{cfg.catalog}.poc_bronze.{t}", keys[t], sorted(totals), gt["known_batches"]))}
            gt["row"].update({(t, d): n for d, n in totals.items()})
        tables[t] = {"columns": cols, "key": keys[t], "relation": f"{cfg.catalog}.poc_bronze.{t}",
                     "threshold": None if d["discard_threshold"] is None else float(d["discard_threshold"]),
                     "manifest_totals": totals, "defects": sorted(totals), "snapshots": t not in cfg.full_load_tables}
    gt["covered"] = covered_targets(reads.pg_query(cfg.metadata_endpoint, ACTIVE_RULES_SQL), tables, reads.sql,
                                    gt["known_batches"])
    return {"sql": reads.sql, "tables": tables, "ground_truth": gt,
            "spark_version": reads.sql("SELECT version() AS v")[0]["v"]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", default=os.environ.get("AIDQ_PROFILE"),
                    help="Databricks CLI profile (or AIDQ_PROFILE); omit in CI to use the DATABRICKS_* variables")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--model", default=None, help="pin a model (default: routed, task rule_generation)")
    ap.add_argument("--out", default=str(ROOT / "evals" / "results"))
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    if not args.profile and not os.environ.get("DATABRICKS_HOST"):
        ap.error("--profile, AIDQ_PROFILE or DATABRICKS_* variables are required (no profile is chosen automatically)")
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
    meta = {"date": dt.date.today().isoformat(), "model": model, "prompt_version": PROMPT_VERSION,
            "known_batches": ctx["ground_truth"]["known_batches"]}
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    md = render(m, runs, meta)
    (out / f"dq_eval_{meta['date']}_{meta['prompt_version'].rsplit('_', 1)[-1]}.md").write_text(md, encoding="utf-8")
    (out / f"dq_eval_{meta['date']}_{meta['prompt_version'].rsplit('_', 1)[-1]}.json").write_text(json.dumps(
        {"meta": meta, "metrics": m, "runs": runs,
         "ground_truth": {"row": {f"{t}|{d}": n for (t, d), n in ctx["ground_truth"]["row"].items()},
                          "not_expressible": ctx["ground_truth"]["not_expressible"],
                          "known_batches": ctx["ground_truth"]["known_batches"],
                          "covered": {f"{t}|{d}": r for (t, d), r in ctx["ground_truth"]["covered"].items()}}},
        indent=2, default=str),
        encoding="utf-8")
    print(md)
    return 0 if all(m["verdicts"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
