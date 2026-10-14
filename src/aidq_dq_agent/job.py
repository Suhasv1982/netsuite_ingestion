"""The DQ recommender as a Databricks job task (plan #39 step 10; decided 2026-10-08: run_as dq-agent, no OAuth
secret). Same graph as `python -m aidq_dq_agent`, wired for a serverless job:

* identity: the job's run-as principal (dq-agent) through WorkspaceClient(): reads, model calls and writes;
* tools in-process (LocalToolBox over SdkReads): no MCP subprocess and no CLI, which serverless does not have;
* writes through write_tools.sdk_connect (Postgres role aidq_agent: INSERT on config_proposals and incidents only).

Started by src/dq_recommender_job.py. Default is a dry run; --write inserts. Exit codes as python -m aidq_dq_agent
(a non-zero code fails the task, and the report is in the task's output).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import traceback
import uuid


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="dq_recommender_job")
    ap.add_argument("--write", default="false", help='"true": insert proposals and incidents (default: dry run)')
    ap.add_argument("--table", action="append", help="limit to this table (repeatable; default: all dev tables)")
    ap.add_argument("--model", default=None, help="pin a model (default: routed, task rule_generation)")
    args = ap.parse_args(argv)
    write = str(args.write).strip().lower() == "true"
    sys.stdout.reconfigure(encoding="utf-8")

    from aidq_agent.dbx import DatabricksChat
    from aidq_agent.toolbox import LocalToolBox
    from aidq_llm.router import PgCallLogWriter, Router
    from aidq_mcp.config import env_config
    from aidq_mcp.sdk_reads import SdkReads
    from aidq_mcp.server import DQ_TOOLS, Tools

    from . import VERSION
    from .__main__ import AGENT, EXIT_CRASHED
    from .graph import run
    from .recommend import RoutedRecommender
    from .write_tools import WriteTools, sdk_connect

    cfg = env_config("dev")
    unknown = set(args.table or []) - set(cfg.tables)
    if unknown:
        ap.error(f"unknown table(s): {sorted(unknown)}")
    trace_id = uuid.uuid4().hex
    reads = SdkReads()
    connect = sdk_connect(cfg.metadata_endpoint)
    write_tools = WriteTools(connect, f"dq_agent@{VERSION}", trace_id, dry_run=not write)
    router = Router(DatabricksChat(None), AGENT, pinned_model=args.model, trace_id=trace_id)
    print(f"DQ recommender job: {'WRITE' if write else 'dry run'}; trace {trace_id}", flush=True)
    try:
        state = asyncio.run(run(LocalToolBox(Tools(reads), DQ_TOOLS), RoutedRecommender(router), write_tools,
                                args.table or list(cfg.tables), trace_id=trace_id))
    except Exception as exc:  # noqa: BLE001 -- a crash still prints a report
        calls = router.log.flush(PgCallLogWriter(connect) if write else None)
        print(f"**CRASHED** before the report: {type(exc).__name__}: {exc}\n{traceback.format_exc()[-3000:]}")
        print("\n".join(calls))
        return EXIT_CRASHED
    calls = router.log.flush(PgCallLogWriter(connect) if write else None)
    print(state["report"] + "".join(f"\n- {line}" for line in calls))
    return state["exit_code"]
