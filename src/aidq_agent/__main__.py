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
from aidq_mcp.server import MONITOR_TOOLS
from aidq_mcp.reads import Reads

from aidq_llm.router import PgCallLogWriter, Router

from .dbx import DatabricksChat, RoutedInvestigator
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


def wake_endpoints(profile: str | None) -> list[str]:
    """Re-enable the source and dev metadata endpoints before the read-only tools run (owner decision 2026-10-13;
    the tools themselves still never enable anything). Returns the log lines for the report."""
    from netsuite_ingestion.lakebase_wake import wake_cli

    cfg, lines = env_config("dev"), []
    for endpoint in (cfg.source_endpoint, cfg.metadata_endpoint):
        try:
            wake_cli(endpoint, who="aidq monitor", profile=profile, log=lines.append)
        except Exception as exc:  # noqa: BLE001 -- the tools then report the endpoint as unavailable
            lines.append(f"could not re-enable {endpoint}: {type(exc).__name__}: {exc}")
    return lines


def run_watchdog(args):
    """Trigger today's missed dev runs before the checks (docs/watchdog_plan.md). Never raises: a failure is
    reported in the run summary and the monitor goes on."""
    from .watchdog import CliJobsClient, Watchdog

    enabled = os.environ.get("WATCHDOG_TRIGGER", "on").strip().lower() != "off"
    try:
        return Watchdog(CliJobsClient(Reads(args.profile)), write=args.write, enabled=enabled).act(), None
    except Exception as exc:  # noqa: BLE001
        return None, f"watchdog failed: {type(exc).__name__}: {exc}"


AGENT = "aidq_monitor"


def make_investigator(args, trace_id: str):
    """(investigator, router or None). Default: routed (task root_cause). --model databricks-*: that model pinned
    (still logged, never escalated). --model claude-*: the Anthropic API, not routed."""
    if args.model and args.model.startswith("claude-"):
        return AnthropicInvestigator(model=args.model), None
    router = Router(DatabricksChat(args.profile), AGENT, pinned_model=args.model, trace_id=trace_id)
    return RoutedInvestigator(router), router


def flush_calls(router, store) -> list[str]:
    """Write the LLM call log (aidq_metadata.llm_calls) and prune by retention. Never raises: on failure the
    returned lines carry the call details into the run summary (owner condition 2026-10-13)."""
    if router is None:
        return []
    writer = PgCallLogWriter(store._connect) if isinstance(store, PgIncidentStore) else None
    return router.log.flush(writer)


async def amain(args) -> int:
    import uuid

    trace_id = uuid.uuid4().hex
    woke = wake_endpoints(args.profile)
    watchdog_result, watchdog_error = run_watchdog(args)
    if watchdog_error:
        woke.append(watchdog_error)
    store = PgIncidentStore(Reads(args.profile), env_config("dev").metadata_endpoint) if args.write else DryRunStore()
    investigator, router = make_investigator(args, trace_id)
    try:
        async with McpToolBox(args.profile, only=MONITOR_TOOLS) as toolbox:
            state = await run(toolbox, investigator, store, trace_id=trace_id, watchdog_result=watchdog_result)
    except Exception as exc:  # noqa: BLE001 -- a crash still produces a report
        calls = flush_calls(router, store)
        publish("# aidq monitor, dev\n\n**MONITOR CRASHED** before its report: "
                f"{type(exc).__name__}: {exc}\n\n```\n{traceback.format_exc()[-3000:]}\n```"
                + "".join(f"\n{line}" for line in calls), args.report_file)
        return EXIT_CRASHED
    woke += flush_calls(router, store)
    publish(state["report"] + "".join(f"\n- {line}" for line in woke), args.report_file)
    if state["exit_code"] == EXIT_METADATA_UNAVAILABLE and os.environ.get("GITHUB_ACTIONS"):
        print(f"::error title=metadata unavailable::{state.get('store_error')}")  # names the cause on the run page
    if not args.write and state.get("incidents"):
        print(f"\n(dry run: {len(state['incidents'])} incident(s) not written)")
    return state["exit_code"]


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m aidq_agent")
    ap.add_argument("--profile", default=os.environ.get("AIDQ_PROFILE"), help="Databricks CLI profile (or AIDQ_PROFILE)")
    ap.add_argument("--write", action="store_true", help="insert incidents (default: dry run)")
    ap.add_argument("--model", default=None,
                    help="default: routed by src/aidq_llm/routing.yml (task root_cause). databricks-*: pin that "
                         "workspace model (no escalation); claude-*: the Anthropic API (not routed)")
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
