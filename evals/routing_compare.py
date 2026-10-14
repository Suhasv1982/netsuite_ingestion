"""Compare candidate models with the routed primary for a task, paired on the same recorded inputs.

    PYTHONPATH=src python evals/routing_compare.py --profile DEFAULT --task root_cause \
        --candidates databricks-gpt-oss-20b databricks-qwen3-next-80b-a3b-instruct [--runs 10] [--cases ...]

Every case of evals/agent_cases.yaml (or --cases) runs --runs times with the baseline (the task's routed primary)
and with each candidate, with the fixed eval judge. Writes evals/routing_report.md (and .json): per candidate the
decision (src/aidq_llm/compare.py rules), then per item the pass counts, the discordant pairs and the sign test.
Nothing changes routing.yml: moving a task is a separate, reviewed change that cites this report.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "evals"))

import yaml  # noqa: E402

from aidq_agent.dbx import DatabricksChat  # noqa: E402
from aidq_llm import compare  # noqa: E402
from aidq_llm.router import load_routes  # noqa: E402
from run_agent_eval import JUDGE_MODEL, run_case  # noqa: E402


CRASH_RETRIES = 2
CRASH_PAUSE_S = 60


async def run_model(model: str, cases: list[dict], runs: int, chat, judge: str) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for case in cases:
        for r in range(1, runs + 1):
            errors = []
            for attempt in range(1 + CRASH_RETRIES):   # an infrastructure crash is retried; a model answer never is
                try:
                    res = await run_case(case, r, chat, model, judge)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{type(exc).__name__}: {str(exc)[:300]}")
                    print(f"{model} {case['id']} run {r}: CRASH (attempt {attempt + 1}): {errors[-1]}", file=sys.stderr)
                    await asyncio.sleep(CRASH_PAUSE_S)
                    continue
                res["checks"] = {"run_completed": True, **res["checks"]}
                res["crash_retries"] = errors
                break
            else:   # still crashing: recorded, and its pair is excluded from the comparison (compare.valid_pairs)
                res = {"case": case["id"], "run": r, "checks": {"run_completed": False}, "judge": [],
                       "errors": errors, "usage": []}
            out.setdefault(case["id"], []).append(res)
            print(f"{model} {case['id']} run {r}: checks={res['checks']}", file=sys.stderr)
    return out


def usage_of(results: dict[str, list[dict]]) -> tuple[int, int]:
    tin = sum(u.get("input_tokens", 0) for rs in results.values() for r in rs for u in r.get("usage") or [])
    tout = sum(u.get("output_tokens", 0) for rs in results.values() for r in rs for u in r.get("usage") or [])
    return tin, tout


def report(task: str, baseline: str, runs: int, base_res: dict, cands: dict[str, dict]) -> str:
    lines = [f"# Routing comparison {dt.date.today().isoformat()}: task `{task}`", "",
             f"Baseline (routed primary) `{baseline}`; {runs} paired runs per case on recorded inputs; judge "
             f"`{JUDGE_MODEL}` (fixed). Rules: src/aidq_llm/compare.py.", ""]
    bt = usage_of(base_res)
    lines.append(f"Baseline tokens: {bt[0]} in / {bt[1]} out")
    for model, res in cands.items():
        stats = compare.pair(base_res, res)
        ok, why = compare.decide(stats)
        skipped = {c: n for c, n in compare.excluded(base_res, res).items() if n}
        t = usage_of(res)
        lines += ["", f"## `{model}`: {'MAY ROUTE' if ok else 'DO NOT ROUTE'}", "",
                  f"Tokens: {t[0]} in / {t[1]} out (baseline {bt[0]} / {bt[1]})", ""]
        lines += [f"- {w}" for w in why] or ["- meets every rule"]
        if skipped:
            lines.append("- pairs excluded after repeated infrastructure crashes (not model failures): "
                         + ", ".join(f"{c} {n}" for c, n in skipped.items()))
        lines += ["", "| case | item | target | baseline | candidate | b | c | p |", "|---|---|---|---|---|---|---|---|"]
        lines += [f"| {s.case} | {s.item} | {s.target}/{s.runs} | {s.baseline_pass} | {s.candidate_pass} | {s.b} | "
                  f"{s.c} | {s.p:.3f} |" for s in stats]
    return "\n".join(lines) + "\n"


async def amain(args) -> int:
    routes = load_routes()
    baseline = routes["tasks"][args.task]["primary"]
    cases = yaml.safe_load((ROOT / "evals" / "agent_cases.yaml").read_text(encoding="utf-8"))["cases"]
    if args.cases:
        cases = [c for c in cases if c["id"] in args.cases]
    chat = DatabricksChat(args.profile)
    base_res = await run_model(baseline, cases, args.runs, chat, JUDGE_MODEL)
    cands = {m: await run_model(m, cases, args.runs, chat, JUDGE_MODEL) for m in args.candidates}
    text = report(args.task, baseline, args.runs, base_res, cands)
    out = ROOT / "evals" / args.out
    out.with_suffix(".md").write_text(text, encoding="utf-8")
    out.with_suffix(".json").write_text(json.dumps({"baseline": base_res, "candidates": cands}, indent=1, default=str),
                                        encoding="utf-8")
    print(text)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--task", default="root_cause")
    ap.add_argument("--candidates", nargs="+", required=True)
    ap.add_argument("--runs", type=int, default=compare.MIN_RUNS)
    ap.add_argument("--cases", nargs="*")
    ap.add_argument("--out", default="routing_report")
    return asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
