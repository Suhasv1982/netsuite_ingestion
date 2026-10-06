"""python -m aidq_mcp --profile <PROFILE> [--smoke]

Runs the read-only MCP server on stdio. --smoke calls every tool once against dev and prints a short summary.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from .reads import Reads
from .server import Tools, build_server


def smoke(tools: Tools) -> int:
    calls = {
        "get_table_health": lambda: tools.get_table_health("dev"),
        "get_recent_job_runs": lambda: tools.get_recent_job_runs("dev", 3),
        "get_pipeline_errors": lambda: tools.get_pipeline_errors("dev", 24),
        "compare_bronze_to_source": lambda: tools.compare_bronze_to_source("dev"),
        "get_recent_deploys": lambda: tools.get_recent_deploys("dev", 7),
        "prod is refused": lambda: tools.get_table_health("prod"),
    }
    bad = 0
    for name, call in calls.items():
        out = call()
        size = len(json.dumps(out, default=str))
        print(f"{name:<26} status={out['status']:<12} {size:>6} bytes  {json.dumps(out, default=str)[:160]}")
        bad += out["status"] == "error" and name != "prod is refused"
    return 1 if bad else 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m aidq_mcp")
    ap.add_argument("--profile", default=os.environ.get("AIDQ_PROFILE"), help="Databricks CLI profile (or AIDQ_PROFILE)")
    ap.add_argument("--smoke", action="store_true", help="call every tool once and print a summary")
    args = ap.parse_args()
    if not args.profile:
        if not os.environ.get("DATABRICKS_HOST"):
            ap.error("--profile, AIDQ_PROFILE or DATABRICKS_* variables are required (no profile is chosen automatically)")
        args.profile = None   # environment auth (CI: ci-dev, OAuth M2M)
    tools = Tools(Reads(args.profile))
    if args.smoke:
        return smoke(tools)
    build_server(tools).run("stdio")
    return 0


if __name__ == "__main__":
    sys.exit(main())
