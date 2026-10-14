"""python -m aidq_dq_agent --profile <PROFILE> [--write] [--table T ...] [--model M] [--report-file PATH]

Runs the DQ rule recommender once against dev (plan #39 step 4). Default is a dry run: the no-nagging checks read
the metadata, nothing is written. --write inserts PENDING proposals and DQ_NOT_EXPRESSIBLE incidents as the
caller: dq-agent in its job (role aidq_agent), the owner locally. Exit 0 when done, 1 when the model gave no
valid recommendation (also after escalation), 2 when an input or a write failed (the report lists it), 3 when the
run itself crashed. The report always reaches --report-file.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import traceback
import uuid

from aidq_agent.toolbox import McpToolBox
from aidq_llm.router import PgCallLogWriter, Router
from aidq_mcp.config import env_config
from aidq_mcp.reads import Reads
from aidq_mcp.server import DQ_TOOLS

from . import VERSION
from .graph import run
from .recommend import RoutedRecommender
from .write_tools import WriteTools

AGENT = "aidq_dq_recommender"
EXIT_CRASHED = 3


def publish(report: str, report_file: str | None) -> None:
    print(report)
    if report_file:
        with open(report_file, "a", encoding="utf-8") as f:
            f.write(report + "\n")


def cli_connect(reads: Reads, endpoint: str):
    """A connect() as the CLI identity (the owner locally, ci-dev in CI). The dq-agent job uses
    write_tools.sdk_connect instead."""
    def connect():
        import psycopg

        ep = reads.cli("postgres", "get-endpoint", endpoint)
        if ep.get("status", {}).get("disabled"):
            raise RuntimeError("metadata endpoint is disabled (the agent never enables it)")
        token = reads.cli("postgres", "generate-database-credential", endpoint)["token"]
        return psycopg.connect(host=ep["status"]["hosts"]["host"], dbname="databricks_postgres",
                               user=reads.current_user(), password=token, sslmode="require", connect_timeout=60)
    return connect


async def amain(args) -> int:
    from aidq_agent.dbx import DatabricksChat

    trace_id = uuid.uuid4().hex
    cfg = env_config("dev")
    tables = args.table or list(cfg.tables)
    connect = cli_connect(Reads(args.profile), cfg.metadata_endpoint)
    write_tools = WriteTools(connect, f"dq_agent@{VERSION}", trace_id, dry_run=not args.write)
    router = Router(DatabricksChat(args.profile), AGENT, pinned_model=args.model, trace_id=trace_id)
    try:
        async with McpToolBox(args.profile, only=DQ_TOOLS) as toolbox:
            state = await run(toolbox, RoutedRecommender(router), write_tools, tables, trace_id=trace_id)
    except Exception as exc:  # noqa: BLE001 -- a crash still produces a report
        calls = router.log.flush(PgCallLogWriter(connect) if args.write else None)
        publish("# aidq DQ recommender, dev\n\n**CRASHED** before its report: "
                f"{type(exc).__name__}: {exc}\n\n```\n{traceback.format_exc()[-3000:]}\n```"
                + "".join(f"\n{line}" for line in calls), args.report_file)
        return EXIT_CRASHED
    calls = router.log.flush(PgCallLogWriter(connect) if args.write else None)
    publish(state["report"] + "".join(f"\n- {line}" for line in calls), args.report_file)
    return state["exit_code"]


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m aidq_dq_agent")
    ap.add_argument("--profile", default=os.environ.get("AIDQ_PROFILE"), help="Databricks CLI profile (or AIDQ_PROFILE)")
    ap.add_argument("--write", action="store_true", help="insert proposals and incidents (default: dry run)")
    ap.add_argument("--table", action="append", help="limit to this table (repeatable; default: all dev tables)")
    ap.add_argument("--model", default=None,
                    help="default: routed by src/aidq_llm/routing.yml (task rule_generation); a databricks-* model "
                         "pins it (no escalation)")
    ap.add_argument("--report-file", help="append the markdown report here")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    if not args.profile:
        if not os.environ.get("DATABRICKS_HOST"):
            ap.error("--profile, AIDQ_PROFILE or DATABRICKS_* variables are required (no profile is chosen automatically)")
        args.profile = None
    unknown = set(args.table or []) - set(env_config("dev").tables)
    if unknown:
        ap.error(f"unknown table(s): {sorted(unknown)}")
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
