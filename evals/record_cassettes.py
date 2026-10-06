"""Record the aidq_mcp tool results an agent eval case replays (evals/cassettes/<case>.json).

    PYTHONPATH=src python evals/record_cassettes.py --profile DEFAULT

Read-only. Calls the real tools with the clock fixed at each case's time; job runs, workflow runs, pipeline events
and audit rows after that time are filtered out (table health and bronze-vs-source counts are current state:
see each case's note). Ids are replaced with stable placeholders so no environment identifier is committed.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import datetime as dt
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aidq_mcp.config import ENVS  # noqa: E402
from aidq_mcp.reads import Reads  # noqa: E402
from aidq_mcp.server import Tools, build_server  # noqa: E402

UTC = dt.timezone.utc
OUT = ROOT / "evals" / "cassettes"
TABLES = list(ENVS["dev"].tables)

VARIANTS = (
    [("get_recent_job_runs", {"days": d, **({"job": j} if j else {})})
     for d in (1, 2, 3, 5, 7, 14) for j in (None, "generator", "ingestion", "canary")]
    + [("get_table_health", {**({"table": t} if t else {})}) for t in [None] + TABLES]
    + [("get_pipeline_errors", {"hours": h}) for h in (6, 12, 24, 48, 72, 168)]
    + [("compare_bronze_to_source", {**({"table": t} if t else {})}) for t in [None] + TABLES]
    + [("get_recent_deploys", {"days": d}) for d in (1, 2, 3, 5, 7, 14, 30)]
)


class TimeTravelReads(Reads):
    """Reads as of `now`: drops runs, events, deploys and audit rows that happened later."""

    def __init__(self, profile, now: dt.datetime):
        super().__init__(profile)
        self.now = now

    def job_runs(self, job_id, since):
        cut = self.now.timestamp() * 1000
        return [r for r in super().job_runs(job_id, since) if (r.get("start_time") or 0) <= cut]

    def pipeline_events(self, pipeline_id, limit):
        return [e for e in super().pipeline_events(pipeline_id, limit)
                if dt.datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00")) <= self.now]

    def workflow_runs(self, workflow, since):
        return [r for r in super().workflow_runs(workflow, since)
                if dt.datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")) <= self.now]

    def sql(self, statement):
        rows = super().sql(statement)
        if "system.access.audit" in statement:
            cut = self.now.strftime("%Y-%m-%d %H:%M:%S")
            rows = [r for r in rows if r["t"][:19] <= cut]
        return rows


_IDS = re.compile(r"\b\d{10,}\b|\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")


def scrub(obj, mapping: dict):
    """Replace job/run ids (10+ digits) and UUIDs with <id-N>, the same id always mapping to the same placeholder."""
    if isinstance(obj, str):
        return _IDS.sub(lambda m: mapping.setdefault(m.group(0), f"<id-{len(mapping) + 1}>"), obj)
    if isinstance(obj, int) and not isinstance(obj, bool) and obj >= 10**9:
        return mapping.setdefault(str(obj), f"<id-{len(mapping) + 1}>")
    if isinstance(obj, dict):
        return {k: scrub(v, mapping) for k, v in obj.items()}
    if isinstance(obj, list):
        return [scrub(v, mapping) for v in obj]
    return obj


async def tool_defs(tools: Tools) -> list[dict]:
    listed = await build_server(tools).list_tools()
    return [{"name": t.name, "description": t.description or "", "input_schema": t.input_schema} for t in listed]


def record(profile: str, now: dt.datetime) -> dict:
    tools = Tools(TimeTravelReads(profile, now), now=lambda: now)
    calls = []
    for name, args in VARIANTS:
        result = getattr(tools, name)(env="dev", **args)
        calls.append({"tool": name, "arguments": {"env": "dev", **args}, "result": result})
        print(f"  {name} {args}: {result['status']}", file=sys.stderr)
    return {"now": now.isoformat(), "tools": asyncio.run(tool_defs(tools)), "calls": calls}


def pre_fix(cassette: dict) -> dict:
    """The same state as seen before PR #27 taught the check about same-day duplicates: every gap unexplained."""
    c = copy.deepcopy(cassette)
    for rec in c["calls"]:
        if rec["tool"] == "compare_bronze_to_source" and rec["result"].get("status") == "ok":
            r = rec["result"]
            for g in r["gaps"]["items"]:
                g["classification"] = "unexplained"
            if r["gaps"]["items"]:
                r["verdict"] = "unexplained_gaps"
            r["note"] = "per-date gaps between dev bronze and the source"
    return c


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    cases = {
        "2026-10-05_after_daily_run": dt.datetime(2026, 10, 5, 12, 0, tzinfo=UTC),
        "2026-10-06_afternoon": dt.datetime(2026, 10, 6, 14, 45, tzinfo=UTC),
    }
    recorded = {}
    for name, now in cases.items():
        print(f"recording {name} (now {now.isoformat()})", file=sys.stderr)
        recorded[name] = record(args.profile, now)
    recorded["2026-10-05_pre_fix"] = pre_fix(recorded["2026-10-05_after_daily_run"])
    for name, cassette in recorded.items():
        mapping: dict = {}
        cassette = scrub(cassette, mapping)
        cassette["note"] = ("Recorded by evals/record_cassettes.py. Job runs, events, deploys and audit rows are as of "
                            "`now`; table health and bronze-vs-source counts are the state at recording time. Ids "
                            "are placeholders." + (" Gap classifications rewritten to 'unexplained' (the check before "
                                                   "PR #27)." if name.endswith("pre_fix") else ""))
        (OUT / f"{name}.json").write_text(json.dumps(cassette, indent=1, default=str), encoding="utf-8")
        print(f"wrote {name}.json ({len(cassette['calls'])} calls)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
