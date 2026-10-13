"""Paired model comparison for routing decisions (docs/model_routing_plan.md, owner condition 2026-10-13).

A candidate model and the baseline (the current primary) run the same eval cases on the same recorded inputs,
N runs each, judged by the same fixed judge. Run r of the candidate is paired with run r of the baseline. Per
item (a deterministic check, a must-identify or a must-not-claim item):

* pass counts out of N for both models;
* discordant pairs: b = baseline passed and candidate failed, c = candidate passed and baseline failed;
* an exact two-sided sign test on (b, c): p small means the difference is unlikely to be noise.

A task may move to the candidate only if ALL of these hold:
  1. at least MIN_RUNS runs per case (3 runs swung by chance on 2026-10-13);
  2. every deterministic check and every must-not-claim item passes in every candidate run (N/N);
  3. every must-identify item passes in at least ceil(2N/3) candidate runs (the eval's own target);
  4. no item is worse in the paired comparison: b - c <= 1 for every item.
Rule 4 is deliberately strict: it does not need significance to block, only to show a regression is real. The
report gives b, c and p for every item, so a borderline case is visible to the reviewer.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

MIN_RUNS = 10
MAX_PAIRED_REGRESSION = 1


def sign_test_p(b: int, c: int) -> float:
    """Exact two-sided binomial sign test on the discordant pairs (p = 1 when there are none)."""
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    tail = sum(math.comb(n, i) for i in range(k + 1)) / 2 ** n
    return min(1.0, 2 * tail)


def items_of(result: dict) -> dict[str, tuple[str, bool]]:
    """{item id: (kind, passed)} for one eval run result (run_agent_eval.run_case)."""
    out = {f"check:{k}": ("check", bool(v)) for k, v in (result.get("checks") or {}).items()}
    for j in result.get("judge") or []:
        out[f"{j['kind']}:{j['id']}"] = (j["kind"], bool(j.get("pass")))
    return out


@dataclass
class ItemStat:
    case: str
    item: str
    kind: str                  # check | identify | claim
    runs: int
    baseline_pass: int
    candidate_pass: int
    b: int                     # baseline pass, candidate fail
    c: int                     # candidate pass, baseline fail
    p: float

    @property
    def target(self) -> int:
        return self.runs if self.kind in ("check", "claim") else math.ceil(2 * self.runs / 3)

    @property
    def candidate_ok(self) -> bool:
        return self.candidate_pass >= self.target and self.b - self.c <= MAX_PAIRED_REGRESSION


def pair(baseline: dict[str, list[dict]], candidate: dict[str, list[dict]]) -> list[ItemStat]:
    """`baseline` / `candidate`: {case id: [run results in run order]}. Items missing in a run count as failed."""
    stats = []
    for case, base_runs in baseline.items():
        cand_runs = candidate.get(case, [])
        n = min(len(base_runs), len(cand_runs))
        b_items = [items_of(r) for r in base_runs[:n]]
        c_items = [items_of(r) for r in cand_runs[:n]]
        names = sorted({k for d in b_items + c_items for k in d})
        for name in names:
            kind = next((d[name][0] for d in b_items + c_items if name in d), "check")
            bp = [d.get(name, (kind, False))[1] for d in b_items]
            cp = [d.get(name, (kind, False))[1] for d in c_items]
            b = sum(1 for x, y in zip(bp, cp) if x and not y)
            c = sum(1 for x, y in zip(bp, cp) if y and not x)
            stats.append(ItemStat(case, name, kind, n, sum(bp), sum(cp), b, c, sign_test_p(b, c)))
    return stats


def decide(stats: list[ItemStat]) -> tuple[bool, list[str]]:
    """(may route to the candidate, reasons it may not)."""
    reasons = []
    runs = min((s.runs for s in stats), default=0)
    if runs < MIN_RUNS:
        reasons.append(f"only {runs} paired run(s) per case; at least {MIN_RUNS} are required")
    for s in stats:
        if s.candidate_pass < s.target:
            reasons.append(f"{s.case} {s.item}: candidate {s.candidate_pass}/{s.runs}, target {s.target}")
        if s.b - s.c > MAX_PAIRED_REGRESSION:
            reasons.append(f"{s.case} {s.item}: worse than the baseline in {s.b} paired runs, better in {s.c} "
                           f"(sign test p={s.p:.3f})")
    return not reasons, reasons
