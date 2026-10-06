"""python -m aidq_agent --profile <PROFILE> [--write] [--report-file PATH]

Runs the monitor once against dev. Default is a dry run (nothing written); --write inserts incidents into dev
aidq_metadata.incidents. Exit 1 when a new incident was written (so a CI run fails and notifies), else 0.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from aidq_mcp.config import env_config
from aidq_mcp.reads import Reads

from .graph import run
from .investigate import AnthropicInvestigator
from .store import DryRunStore, PgIncidentStore
from .toolbox import McpToolBox


async def amain(args) -> int:
    store = PgIncidentStore(Reads(args.profile), env_config("dev").metadata_endpoint) if args.write else DryRunStore()
    async with McpToolBox(args.profile) as toolbox:
        state = await run(toolbox, AnthropicInvestigator(model=args.model), store)
    print(state["report"])
    if args.report_file:
        with open(args.report_file, "a", encoding="utf-8") as f:
            f.write(state["report"] + "\n")
    if not args.write and state.get("incidents"):
        print(f"\n(dry run: {len(state['incidents'])} incident(s) not written)")
    return state["exit_code"]


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m aidq_agent")
    ap.add_argument("--profile", default=os.environ.get("AIDQ_PROFILE"), help="Databricks CLI profile (or AIDQ_PROFILE)")
    ap.add_argument("--write", action="store_true", help="insert incidents (default: dry run)")
    ap.add_argument("--model", default="claude-opus-5-5")
    ap.add_argument("--report-file", help="append the markdown report here (e.g. $GITHUB_STEP_SUMMARY)")
    args = ap.parse_args()
    if not args.profile:
        ap.error("--profile or AIDQ_PROFILE is required (no profile is ever chosen automatically)")
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
