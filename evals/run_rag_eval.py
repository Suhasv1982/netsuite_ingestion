"""RAG + lineage eval (plan #70 step 6; design section 4.7). Read-only, through the real MCP tools on dev.

    python evals/run_rag_eval.py --profile DEFAULT

25 fixed questions with known answers:
  * 10 impact / upstream questions: the tool's gold and rule nodes must equal the expected set EXACTLY (100%).
    Expected gold nodes are written by hand from the gold SQL (src/netsuite_ingestion/gold_sql.py); expected rules
    are every rule whose expression names the column (a plain word match, independent of the graph's sqlglot path).
  * 10 history questions: the expected document must be among search_metadata's top 5 (recall@5 >= 0.9); two also
    check explain_history.
  * 5 "no answer" questions (off-topic): search_metadata must return no hit above MIN_SCORE (it must not invent).
  * Every hit carries a citation (id and source).
The report also prints the score distribution of relevant vs off-topic searches, to calibrate MIN_SCORE.
Report: evals/results/rag_eval_<date>.md and .json.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

REV, STA = "gold:gold_customer_revenue", "gold:gold_customer_status"
IMPACT = [  # (object, direction, expected gold nodes) ; rules are added from the metadata (word match)
    ("netsuite_transaction_lines.amount", "down", {REV, f"{REV}.revenue"}),
    ("netsuite_transactions.date", "down", {REV, f"{REV}.revenue_month"}),
    ("netsuite_memberships.membership_status", "down", {STA}),
    ("netsuite_memberships.end_date", "down", {STA, f"{STA}.next_membership_end"}),
    ("netsuite_certifications.certification_end_date", "down", {STA, f"{STA}.next_certification_end"}),
    ("netsuite_customers.email", "down", set()),
    ("netsuite_transactions.type", "down", set()),
    ("netsuite_transaction_lines.transaction_internal_id", "down", {REV}),
    ("netsuite_memberships.customer_internal_id", "down", {STA, f"{STA}.customer_internal_id"}),
]
UPSTREAM = ("gold_customer_revenue.revenue", {
    "source:netsuite_transaction_lines.amount", "source:netsuite_transaction_lines.transaction_internal_id",
    "source:netsuite_transaction_lines.transaction_line_id", "source:netsuite_transactions.customer_internal_id",
    "source:netsuite_transactions.date", "source:netsuite_transactions.transaction_internal_id"})
HISTORY = [  # (question, acceptable document ids or source substrings)
    ("Why was the rule restricting transaction type to Cash Sale and Invoice added?", {"rule:10", "proposal:20"}),
    ("Which proposal added the check that the membership business key is not null?", {"rule:11", "proposal:14"}),
    ("What happened in the incident about the stale OAuth token and the expired credential?", {"incident:6"}),
    ("Why was the rule about membership start dates in the future rejected?", {"proposal:15"}),
    ("Proposal to add the custbody_region column to transactions", {"proposal:48"}),
    ("Rule that certification end date must not be before the start date, and its severity", {"rule:12", "proposal:16"}),
    ("How does the bronze row hash guard fail closed when the column set changes?", {"src:docs/"}),
    ("What does the watchdog do when a scheduled dev run is missed?", {"src:docs/watchdog_plan.md"}),
    ("Which table is loaded in full every run instead of incrementally?", {"table:netsuite_customers"}),
    ("Manual pipeline runs must go through the job, never directly", {"src:docs/STATUS.md"}),
]
NO_ANSWER = ["What is the capital of France?", "Who won the 2018 football world cup?",
             "How do I bake sourdough bread at home?", "What is the current stock price of Apple?",
             "Translate 'good morning' into Japanese."]


def rule_nodes(rules: list[dict], obj: str) -> set[str]:
    table, _, col = obj.partition(".")
    return {f"rule:{r['rule_id']}" for r in rules
            if r["source_table"] == table and re.search(rf"\b{re.escape(col)}\b", r["rule_expr"] or "")}


def matches(hit: dict, wanted: set[str]) -> bool:
    return any(hit["id"] == w or (w.startswith("src:") and (hit.get("source") or "").startswith(w[4:])) for w in wanted)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", required=True)
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    from aidq_mcp import knowledge_tools as kt
    from aidq_mcp.config import env_config
    from aidq_mcp.reads import Reads
    from aidq_mcp.server import Tools

    reads, cfg = Reads(args.profile), env_config("dev")
    tools = Tools(reads)
    rules = reads.pg_query(cfg.metadata_endpoint, "SELECT r.rule_id, d.source_table, r.rule_expr FROM "
                           "aidq_metadata.data_quality_rules r JOIN aidq_metadata.source_table_def d USING (table_id)")
    rows, checks = [], {}
    for obj, _, gold in IMPACT:
        out = tools.get_impact("dev", obj)
        got = {n["node"] for n in (out.get("nodes") or {}).get("items", []) if n["node"].startswith(("gold:", "rule:"))}
        want = gold | rule_nodes(rules, obj)
        ok = out["status"] == "ok" and got == want and all(n.get("path") for n in out["nodes"]["items"])
        rows.append({"kind": "impact", "q": obj, "ok": ok, "expected": sorted(want), "got": sorted(got)})
    out = tools.get_upstream("dev", UPSTREAM[0])
    got = {n["node"] for n in out["nodes"]["items"] if n["node"].startswith("source:") and "." in n["node"]}
    rows.append({"kind": "upstream", "q": UPSTREAM[0], "ok": got == UPSTREAM[1], "expected": sorted(UPSTREAM[1]),
                 "got": sorted(got)})
    impact_ok = sum(r["ok"] for r in rows)
    checks["impact / upstream exact (100%)"] = impact_ok == len(rows)

    saved, kt.MIN_SCORE = kt.MIN_SCORE, -1.0          # read every score, then judge at the configured floor
    relevant_scores, offtopic_scores, history_hits, citations_ok = [], [], 0, True
    for q, wanted in HISTORY:
        out = tools.search_metadata("dev", q, 5)
        assert out["status"] == "ok", out          # a failing search must fail the eval, not look like "no hits"
        hits = out.get("hits", [])
        citations_ok &= all(h.get("id") and h.get("source") for h in hits)
        rank = next((i for i, h in enumerate(hits) if matches(h, wanted)), None)
        if rank is not None:
            relevant_scores.append(hits[rank]["score"])
        hit = rank is not None and hits[rank]["score"] >= saved
        history_hits += hit
        rows.append({"kind": "history", "q": q, "ok": hit, "rank": rank, "top": [(h["id"], h["score"]) for h in hits]})
    for q in NO_ANSWER:
        hits = tools.search_metadata("dev", q, 5).get("hits", [])
        top = max((h["score"] for h in hits), default=0.0)
        offtopic_scores.append(top)
        rows.append({"kind": "no_answer", "q": q, "ok": top < saved, "top_score": top})
    kt.MIN_SCORE = saved
    recall = history_hits / len(HISTORY)
    checks["history recall@5 >= 0.9"] = recall >= 0.9
    checks["no-answer questions find nothing above MIN_SCORE"] = all(r["ok"] for r in rows if r["kind"] == "no_answer")
    checks["a citation (id, source) on every hit"] = citations_ok
    h1 = tools.explain_history("dev", "rule:10")
    checks["explain_history(rule:10) shows proposal 20"] = any(e["id"] == 20 for e in h1.get("events", {}).get("items", []))
    h2 = tools.explain_history("dev", "netsuite_transactions")
    # incident 11: the DQ recommender's duplicate-keys finding on transactions (incident 6 has no table link)
    checks["explain_history(netsuite_transactions) shows incident 11"] = any(
        e["kind"] == "incident" and e["id"] == 11 for e in h2.get("events", {}).get("items", []))

    date = dt.date.today().isoformat()
    ok = all(checks.values())
    lines = [f"# RAG + lineage eval, dev, {date}", "",
             f"Real MCP tools on dev; MIN_SCORE {saved}. Impact {impact_ok}/{len(IMPACT) + 1}, history recall@5 "
             f"{recall:.0%}.", "", "| Check | Result |", "|---|---|"]
    lines += [f"| {k} | {'PASS' if v else 'FAIL'} |" for k, v in checks.items()]
    lines += ["", f"**Overall: {'PASS' if ok else 'FAIL'}**", "",
              f"Score calibration: relevant hits {sorted(relevant_scores)}; best off-topic hit per question "
              f"{sorted(offtopic_scores)}.", "", "## Questions", ""]
    for r in rows:
        detail = (f"expected {r['expected']}, got {r['got']}" if r["kind"] in ("impact", "upstream")
                  else f"rank {r.get('rank')}, top {r.get('top')}" if r["kind"] == "history"
                  else f"best score {r.get('top_score')}")
        lines.append(f"- [{'PASS' if r['ok'] else 'FAIL'}] {r['kind']}: {r['q']} ({detail})")
    md = "\n".join(lines) + "\n"
    out_dir = ROOT / "evals" / "results"
    (out_dir / f"rag_eval_{date}.md").write_text(md, encoding="utf-8")
    (out_dir / f"rag_eval_{date}.json").write_text(json.dumps({"checks": checks, "rows": rows, "min_score": saved,
                                                               "relevant_scores": relevant_scores,
                                                               "offtopic_scores": offtopic_scores}, indent=2,
                                                              default=str), encoding="utf-8")
    print(md)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
