"""Job task of the dev-only `silver_refresh` job: a full refresh of the SILVER tables only (plan #39 step 11,
design v2 section 12 item 4: rows already in bronze that violate new HARD rules leave silver; rejects are a batch
table recomputed from all of bronze on every run, so they already hold them).

The job runs refresh_credentials first (the rule since incident 6: manual pipeline runs go through the job), then
this task starts one pipeline update and waits for it: full_refresh_selection = the silver tables, refresh_selection
= rejects and gold (a normal recompute, so they reflect the refreshed silver). A selective update runs only the named
datasets: bronze is not named, so it is neither read from the source nor rewritten, and the bronze guard is not
involved. Run as the job's identity (WorkspaceClient()).

    tools/silver_refresh.py --pipeline-id <id> --tables poc_silver.a,poc_silver.b --also-refresh poc_reject.x,poc_gold.y
"""

from __future__ import annotations

import argparse
import sys
import time

TERMINAL = ("COMPLETED", "FAILED", "CANCELED")


def check_tables(tables: list[str], also: list[str]) -> tuple[list[str], list[str]]:
    """Only silver may be fully refreshed; only rejects and gold may be recomputed alongside. Never bronze."""
    bad = [t for t in tables if not t.startswith("poc_silver.")]
    bad += [t for t in also if not t.startswith(("poc_reject.", "poc_gold."))]
    if not tables or bad:
        raise SystemExit(f"full refresh: poc_silver.* only; also-refresh: poc_reject.* / poc_gold.* only; got "
                         f"{tables} / {also}")
    return tables, also


def refresh(w, pipeline_id: str, tables: list[str], also: list[str], poll_s: int = 20, timeout_s: int = 3600,
            log=print) -> str:
    update_id = w.pipelines.start_update(pipeline_id, full_refresh_selection=tables,
                                         refresh_selection=also or None).update_id
    log(f"started update {update_id}: full refresh of {', '.join(tables)}; recompute {', '.join(also) or '-'}")
    deadline = time.monotonic() + timeout_s
    while True:
        state = str(w.pipelines.get_update(pipeline_id, update_id).update.state.value)
        if state in TERMINAL:
            log(f"update {update_id}: {state}")
            return state
        if time.monotonic() > deadline:
            log(f"update {update_id}: still {state} after {timeout_s} s")
            return "TIMEOUT"
        time.sleep(poll_s)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--pipeline-id", required=True)
    ap.add_argument("--tables", required=True, help="comma-separated poc_silver.<table> names (full refresh)")
    ap.add_argument("--also-refresh", default="", help="comma-separated poc_reject.* / poc_gold.* (normal recompute)")
    args = ap.parse_args(argv)
    split = lambda v: [t.strip() for t in v.split(",") if t.strip()]  # noqa: E731
    tables, also = check_tables(split(args.tables), split(args.also_refresh))
    from databricks.sdk import WorkspaceClient

    return 0 if refresh(WorkspaceClient(), args.pipeline_id, tables, also) == "COMPLETED" else 1


if __name__ == "__main__":
    # only a failure exits: in a job's IPython kernel even sys.exit(0) marks the run FAILED
    _code = main()
    if _code:
        sys.exit(_code)
