"""The DQ recommender as a Databricks job task (plan #39 step 10; decided 2026-10-08: run_as dq-agent, no OAuth
secret). Same graph as `python -m aidq_dq_agent`, wired for a serverless job:

* identity: the job's run-as principal (dq-agent) through WorkspaceClient(): reads, model calls and writes;
* tools in-process (LocalToolBox over SdkReads): no MCP subprocess and no CLI, which serverless does not have;
* writes through write_tools.sdk_connect (Postgres role aidq_agent: INSERT on config_proposals and incidents only);
* tracing (step 8): with --trace-experiment, one MLflow trace per run; its id is the run's agent_trace_id.

Started by src/dq_recommender_job.py. Default is a dry run; --write inserts. Exit codes as python -m aidq_dq_agent
(a non-zero code fails the task, and the report is in the task's output).

A serverless python task runs inside an IPython kernel (first runs, 2026-10-14): stdout is the kernel's stream (no
reconfigure()), and an event loop may already be running, where asyncio.run() refuses to start. run_async() runs
the graph on its own loop in a worker thread then.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import sys
import traceback


def run_async(coro):
    """asyncio.run(coro), or, when an event loop is already running in this thread (an IPython kernel), the same on
    a fresh loop in a worker thread."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="dq_recommender_job")
    ap.add_argument("--write", default="false", help='"true": insert proposals and incidents (default: dry run)')
    ap.add_argument("--table", action="append", help="limit to this table (repeatable; default: all dev tables)")
    ap.add_argument("--model", default=None, help="pin a model (default: routed, task rule_generation)")
    ap.add_argument("--trace-experiment", default="",
                    help="MLflow experiment for the run's trace (empty: no tracing; tracing.EXPERIMENT in the job)")
    args = ap.parse_args(argv)
    write = str(args.write).strip().lower() == "true"
    if hasattr(sys.stdout, "reconfigure"):        # an IPython kernel's stream has none (and is UTF-8 already)
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
    from .tracing import MlflowTracer, NullTracer
    from .write_tools import WriteTools, sdk_connect

    cfg = env_config("dev")
    unknown = set(args.table or []) - set(cfg.tables)
    if unknown:
        ap.error(f"unknown table(s): {sorted(unknown)}")
    reads = SdkReads()
    connect = sdk_connect(cfg.metadata_endpoint)
    tables = args.table or list(cfg.tables)
    tracer = NullTracer()
    if args.trace_experiment:
        try:
            warehouse = reads.warehouse_id()
        except Exception:  # noqa: BLE001 -- tracing is optional
            warehouse = None
        tracer = MlflowTracer(args.trace_experiment).configure(warehouse)
    ctx: dict = {}

    async def traced_run():
        inputs = {"write": write, "tables": tables, "pinned_model": args.model}
        with tracer.run("dq_recommender", inputs) as (trace_id, root):
            ctx["trace_id"] = trace_id
            write_tools = WriteTools(connect, f"dq_agent@{VERSION}", trace_id, dry_run=not write)
            router = ctx["router"] = Router(DatabricksChat(None), AGENT, pinned_model=args.model, trace_id=trace_id,
                                            tracer=tracer if tracer.enabled else None)
            print(f"DQ recommender job: {'WRITE' if write else 'dry run'}; trace {trace_id}", flush=True)
            state = await run(LocalToolBox(Tools(reads), DQ_TOOLS), RoutedRecommender(router), write_tools, tables,
                              trace_id=trace_id, tracer=tracer,
                              trace_meta={"trace_experiment_id": tracer.experiment_id} if tracer.enabled else None)
            rows = router.log.rows
            root.set_attributes({"exit_code": state.get("exit_code"), "write": write,
                                 "input_tokens": sum(r.input_tokens or 0 for r in rows),
                                 "output_tokens": sum(r.output_tokens or 0 for r in rows),
                                 "llm_latency_ms": sum(r.latency_ms for r in rows), "llm_calls": len(rows),
                                 "proposals": sum(1 for s in state.get("submitted") or [] if s["kind"] == "proposal"),
                                 "findings": sum(1 for s in state.get("submitted") or [] if s["kind"] == "finding")})
            root.set_outputs({"exit_code": state.get("exit_code"), "report": state.get("report")})
            return state

    try:
        state = run_async(traced_run())
    except Exception as exc:  # noqa: BLE001 -- a crash still prints a report
        router = ctx.get("router")
        calls = router.log.flush(PgCallLogWriter(connect) if write else None) if router else []
        print(f"**CRASHED** before the report: {type(exc).__name__}: {exc}\n{traceback.format_exc()[-3000:]}")
        print("\n".join(calls + ([tracer.error] if tracer.error else [])))
        return EXIT_CRASHED
    calls = ctx["router"].log.flush(PgCallLogWriter(connect) if write else None)
    trace_line = (f"MLflow trace {ctx['trace_id']} (experiment {tracer.experiment_id})" if tracer.enabled
                  else tracer.error or "no tracing")
    print(state["report"] + "".join(f"\n- {line}" for line in calls + [trace_line]))
    return state["exit_code"]
