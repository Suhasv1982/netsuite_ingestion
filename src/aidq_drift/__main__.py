"""python -m aidq_drift --profile <P> [--write] [--source-endpoint E] [--trace-experiment X]   (local)
   src/schema_drift_job.py --write true|false [--trace-experiment X]                         (the job, as dq-agent)

Runs the schema drift agent once against dev. Default is a dry run locally (the no-nagging checks read the metadata,
nothing is written); the job writes. --source-endpoint points detection at another source branch (the eval's dev
branch of netsuite-sample). Exit 0 when done (also when there is no drift), 1 when the model gave no valid review,
2 when a write failed (the report lists it), 3 when the run crashed.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import os
import sys
import traceback

AGENT = "aidq_schema_drift"
EXIT_CRASHED = 3


def run_async(coro):
    """asyncio.run, or on a fresh loop in a worker thread inside a running loop (a job's IPython kernel)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def execute(*, sdk: bool, profile: str | None, write: bool, source_endpoint: str | None, trace_experiment: str,
            model: str | None, out=print) -> int:
    from aidq_agent.dbx import DatabricksChat
    from aidq_dq_agent.tracing import MlflowTracer, NullTracer
    from aidq_dq_agent.write_tools import WriteTools, sdk_connect
    from aidq_llm.router import PgCallLogWriter, Router
    from aidq_mcp.config import env_config

    from . import VERSION
    from .agent import ModelReviewer, run

    cfg = env_config("dev")
    if sdk:
        from aidq_mcp.sdk_reads import SdkReads

        reads = SdkReads()
        connect = sdk_connect(cfg.metadata_endpoint)
    else:
        from aidq_dq_agent.__main__ import cli_connect
        from aidq_mcp.reads import Reads

        reads = Reads(profile)
        connect = cli_connect(reads, cfg.metadata_endpoint)
    tracer = NullTracer()
    if trace_experiment:
        try:
            wh = reads.warehouse_id()
        except Exception:  # noqa: BLE001
            wh = None
        tracer = MlflowTracer(trace_experiment, "databricks" if sdk or not profile else f"databricks://{profile}").configure(wh)
    ctx: dict = {}

    async def go():
        with tracer.run("schema_drift", {"write": write}) as (trace_id, root):
            wt = WriteTools(connect, f"schema_drift@{VERSION}", trace_id, dry_run=not write)
            router = ctx["router"] = Router(DatabricksChat(profile), AGENT, pinned_model=model, trace_id=trace_id,
                                            tracer=tracer if tracer.enabled else None)
            state = await run(reads.pg_query, source_endpoint or cfg.source_endpoint, cfg.metadata_endpoint,
                              ModelReviewer(router), wt, trace_id,
                              meta={"trace_experiment_id": tracer.experiment_id} if tracer.enabled else None,
                              tracer=tracer)
            root.set_outputs({"exit_code": state["exit_code"], "report": state["report"]})
            return state

    try:
        state = run_async(go())
    except Exception as exc:  # noqa: BLE001 -- a crash still prints a report
        out(f"**CRASHED**: {type(exc).__name__}: {exc}\n{traceback.format_exc()[-3000:]}")
        return EXIT_CRASHED
    router = ctx.get("router")
    calls = router.log.flush(PgCallLogWriter(connect) if write else None) if router else []
    out(state["report"] + "".join(f"\n- {line}" for line in calls + ([tracer.error] if tracer.error else [])))
    return state["exit_code"]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m aidq_drift")
    ap.add_argument("--profile", default=os.environ.get("AIDQ_PROFILE"))
    ap.add_argument("--write", action="store_true", help="insert proposals and incidents (default: dry run)")
    ap.add_argument("--source-endpoint", default=None, help="another source branch's endpoint (eval)")
    ap.add_argument("--trace-experiment", default="", help="MLflow experiment (empty: no tracing)")
    ap.add_argument("--model", default=None)
    args = ap.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if not args.profile and not os.environ.get("DATABRICKS_HOST"):
        ap.error("--profile, AIDQ_PROFILE or DATABRICKS_* variables are required")
    return execute(sdk=False, profile=args.profile, write=args.write, source_endpoint=args.source_endpoint,
                   trace_experiment=args.trace_experiment, model=args.model)


if __name__ == "__main__":
    sys.exit(main())
