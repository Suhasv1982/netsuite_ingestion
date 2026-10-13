"""python -m aidq_agent --profile <PROFILE> [--write] [--report-file PATH]

Runs the monitor once against dev. Default is a dry run (nothing written); --write inserts incidents into dev
aidq_metadata.incidents. Exit 1 when a new incident was written (so a CI run fails and notifies), 2 when the
diagnosis could not be written ("metadata unavailable": the report still has all of it), 3 when the run itself
crashed (the report says where), else 0. The report always reaches --report-file.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import traceback

from aidq_mcp.config import env_config
from aidq_mcp.reads import Reads

from .dbx import DatabricksChat, DatabricksInvestigator
from .graph import EXIT_METADATA_UNAVAILABLE, run
from .investigate import AnthropicInvestigator
from .store import DryRunStore, PgIncidentStore
from .toolbox import McpToolBox


EXIT_CRASHED = 3


def publish(report: str, report_file: str | None) -> None:
    print(report)
    if report_file:
        with open(report_file, "a", encoding="utf-8") as f:
            f.write(report + "\n")


async def amain(args) -> int:
    store = PgIncidentStore(Reads(args.profile), env_config("dev").metadata_endpoint) if args.write else DryRunStore()
    try:
        async with McpToolBox(args.profile) as toolbox:
            investigator = (DatabricksInvestigator(DatabricksChat(args.profile), args.model)
                            if args.model.startswith("databricks-") else AnthropicInvestigator(model=args.model))
            state = await run(toolbox, investigator, store)
    except Exception as exc:  # noqa: BLE001 -- a crash still produces a report
        publish("# aidq monitor, dev\n\n**MONITOR CRASHED** before its report: "
                f"{type(exc).__name__}: {exc}\n\n```\n{traceback.format_exc()[-3000:]}\n```", args.report_file)
        return EXIT_CRASHED
    publish(state["report"], args.report_file)
    if state["exit_code"] == EXIT_METADATA_UNAVAILABLE and os.environ.get("GITHUB_ACTIONS"):
        print(f"::error title=metadata unavailable::{state.get('store_error')}")  # names the cause on the run page
    if not args.write and state.get("incidents"):
        print(f"\n(dry run: {len(state['incidents'])} incident(s) not written)")
    return state["exit_code"]


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m aidq_agent")
    ap.add_argument("--profile", default=os.environ.get("AIDQ_PROFILE"), help="Databricks CLI profile (or AIDQ_PROFILE)")
    ap.add_argument("--write", action="store_true", help="insert incidents (default: dry run)")
    ap.add_argument("--model", default="databricks-gpt-oss-120b",
                    help="claude-* (Anthropic API) or databricks-* (a model served on the workspace, e.g. "
                         "databricks-gpt-oss-120b)")
    ap.add_argument("--report-file", help="append the markdown report here (e.g. $GITHUB_STEP_SUMMARY)")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")   # model text may contain non-ASCII (Windows consoles default to cp1252)
    if not args.profile:
        if not os.environ.get("DATABRICKS_HOST"):
            ap.error("--profile, AIDQ_PROFILE or DATABRICKS_* variables are required (no profile is chosen automatically)")
        args.profile = None   # environment auth (CI: ci-dev, OAuth M2M)
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
