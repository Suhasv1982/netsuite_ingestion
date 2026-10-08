"""Evaluate the monitor + RCA agent on recorded cases (evals/agent_cases.yaml) with free workspace models.

    PYTHONPATH=src python evals/run_agent_eval.py --profile DEFAULT [--runs 3] [--model M] [--judge J]

Each run replays a case's tool outputs (no workspace reads, nothing written), then grades:
  deterministic: incident written iff expected; categories allowed; RCA produced; expected tools called (reported)
  judge (a different model than the agent): each fixture must_identify item stated, each must_not_claim item claimed
Writes evals/results/<date>_<model>.md and .json. Exit 1 if a target in docs/phase4_agent_plan.md section 7 is missed.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aidq_agent.dbx import DEFAULT_MODEL, DatabricksChat, DatabricksInvestigator, _text  # noqa: E402
from aidq_agent.graph import run  # noqa: E402
from aidq_agent.store import DryRunStore  # noqa: E402
from aidq_agent.toolbox import ReplayToolBox  # noqa: E402

JUDGE_MODEL = "databricks-qwen35-122b-a10b"
JUDGE_PROMPT = """You grade an incident report written by a monitoring agent against a checklist.

Report:
<report>
{report}
</report>

Checklist. For each item answer "yes" or "no":
- identify items: does the report state this (in any wording)?
- claim items: does the report assert this as a fact? Mentioning it as ruled out, unverified or merely possible is "no".

{items}

Reply with JSON only: {{"items": [{{"id": "<id>", "verdict": "yes" or "no", "quote": "<shortest supporting span from the report, or empty>"}}]}}"""


def report_text(inc: dict) -> str:
    rca = inc["evidence"].get("rca") or {}
    parts = [f"Category: {inc['category']}", f"Summary: {inc['summary']}"]
    parts += [f"Evidence [{e['tool']}]: {e['finding']}" for e in rca.get("evidence", [])]
    parts += [f"Ruled out: {x}" for x in rca.get("not_the_cause", [])]
    parts += [f"Not verified: {x}" for x in rca.get("open_questions", [])]
    if inc.get("suggested_fix"):
        parts.append(f"Next step: {inc['suggested_fix']}")
    return "\n".join(parts)


def parse_json(text: str) -> dict | None:
    m = re.search(r"\{.*\}", text or "", re.S)
    try:
        return json.loads(m.group(0)) if m else None
    except ValueError:
        return None


def grade_items(grading: dict) -> list[dict]:
    items = [{"id": f"identify_{i}", "kind": "identify", "text": t} for i, t in enumerate(grading["must_identify"], 1)]
    items += [{"id": f"claim_{i}", "kind": "claim", "text": t} for i, t in enumerate(grading["must_not_claim"], 1)]
    return items


def apply_verdicts(items: list[dict], got: dict | None) -> list[dict] | None:
    if not got or not isinstance(got.get("items"), list):
        return None
    verdicts = {v.get("id"): v for v in got["items"] if isinstance(v, dict)}
    out = []
    for it in items:
        v = verdicts.get(it["id"], {})
        verdict = str(v.get("verdict", "")).lower() or "missing"
        ok = verdict == "yes" if it["kind"] == "identify" else verdict == "no"
        out.append({**it, "verdict": verdict, "pass": ok, "quote": v.get("quote", "")})
    return out


async def judge(chat: DatabricksChat, model: str, report: str, grading: dict) -> list[dict]:
    items = grade_items(grading)
    listing = "\n".join(f"- {it['id']} ({it['kind']}): {it['text']}" for it in items)
    for _ in range(2):
        resp = await chat.complete(model, [{"role": "user", "content": JUDGE_PROMPT.format(report=report, items=listing)}],
                                   max_tokens=12000)   # reasoning models spend thousands of tokens before answering
        graded = apply_verdicts(items, parse_json(_text(resp["choices"][0]["message"])))
        if graded:
            return graded
    return [{**it, "verdict": "judge_failed", "pass": False, "quote": ""} for it in items]


async def run_case(case: dict, r: int, chat: DatabricksChat, model: str, judge_model: str) -> dict:
    cassette = json.loads((ROOT / "evals" / "cassettes" / f"{case['cassette']}.json").read_text(encoding="utf-8"))
    now = dt.datetime.fromisoformat(case["now"])
    # a case recorded before a design change replays with the design notes of its time (evals/design_notes/)
    notes = ROOT / case["design_notes"] if case.get("design_notes") else None
    state = await run(ReplayToolBox(cassette), DatabricksInvestigator(chat, model, design_notes=notes), DryRunStore(),
                      now=lambda: now, trace_id=f"eval-{case['id']}-{r}")
    incidents = state.get("incidents") or []
    checks = {"incident_as_expected": bool(incidents) == case["expect_incident"]}
    result = {"case": case["id"], "run": r, "incidents": len(incidents), "checks": checks, "judge": [],
              "reports": [report_text(i) for i in incidents],
              "tool_calls": [c["tool"] for i in incidents for c in i["evidence"]["tool_calls"]],
              "errors": [i["evidence"]["error"] for i in incidents if i["evidence"]["error"]],
              "usage": [i["evidence"]["usage"] for i in incidents]}
    if case["expect_incident"] and incidents:
        checks["category_allowed"] = all(i["category"] in case["categories"] for i in incidents)
        checks["rca_produced"] = all(i["evidence"]["rca"] for i in incidents)
        fixture = yaml.safe_load((ROOT / "evals" / "incidents" / f"{case['fixture']}.yaml").read_text(encoding="utf-8"))
        expected = set(fixture["grading"]["tools_expected"])
        result["tools_expected_hit"] = f"{len(expected & set(result['tool_calls']))}/{len(expected)}"
        if checks["rca_produced"]:
            result["judge"] = await judge(chat, judge_model, "\n\n".join(result["reports"]), fixture["grading"])
    return result


def summarize(results: list[dict], cases: list[dict], runs: int) -> tuple[str, bool]:
    lines, ok_all = [], True
    for case in cases:
        rs = [r for r in results if r["case"] == case["id"]]
        lines += [f"## {case['id']}", f"cassette `{case['cassette']}`, expect incident: {case['expect_incident']}", ""]
        for k in sorted({k for r in rs for k in r["checks"]}):
            n = sum(1 for r in rs if r["checks"].get(k))
            lines.append(f"- {k}: {n}/{runs}")
            ok_all &= n == runs
        if case["expect_incident"]:
            lines.append(f"- expected tools called: {', '.join(r.get('tools_expected_hit', '-') for r in rs)}")
            for item in next((r["judge"] for r in rs if r["judge"]), []):
                per = [next((j for j in r["judge"] if j["id"] == item["id"]), None) for r in rs]
                n = sum(1 for j in per if j and j["pass"])
                claim = item["kind"] == "claim"
                need = runs if claim else -(-2 * runs // 3)
                lines.append(f"- {item['id']} ({'must not claim' if claim else 'must identify'}): {n}/{runs}  {item['text']}")
                ok_all &= n >= need
            if not any(r["judge"] for r in rs):
                ok_all = False
        errs = [e for r in rs for e in r["errors"]]
        if errs:
            lines.append(f"- errors: {errs}")
        lines.append("")
    return "\n".join(lines), ok_all


async def amain(args) -> int:
    cases = yaml.safe_load((ROOT / "evals" / "agent_cases.yaml").read_text(encoding="utf-8"))["cases"]
    if args.cases:
        cases = [c for c in cases if c["id"] in args.cases]
    chat = DatabricksChat(args.profile)
    results = []
    for case in cases:
        for r in range(1, args.runs + 1):
            res = await run_case(case, r, chat, args.model, args.judge)
            print(f"{case['id']} run {r}: incidents={res['incidents']} checks={res['checks']} "
                  f"judge={[(j['id'], j['pass']) for j in res['judge']]}", file=sys.stderr)
            results.append(res)
    body, ok = summarize(results, cases, args.runs)
    stamp = dt.date.today().isoformat()
    head = (f"# Agent eval {stamp}\n\nAgent model `{args.model}`, judge `{args.judge}` (both served free on the "
            f"workspace), {args.runs} runs per case, recorded tool outputs. Targets (plan section 7): healthy day "
            f"no incident every run; incidents every run on deterministic checks and on each must-not-claim item, "
            f"at least 2/3 on each must-identify item. **Overall: {'PASS' if ok else 'FAIL'}**\n\n")
    out = ROOT / "evals" / "results" / f"{stamp}_{args.model}"
    out.with_suffix(".md").write_text(head + body, encoding="utf-8")
    out.with_suffix(".json").write_text(json.dumps(results, indent=1, default=str), encoding="utf-8")
    print(head + body)
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--judge", default=JUDGE_MODEL)
    ap.add_argument("--cases", nargs="*")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
