"""Job task: run the guard canary and assert the full-refresh guard still works.

Triggers four updates of the tiny canary pipeline (canary_pipeline.py):

  1. a normal update            -> must COMPLETE (the guard must not block normal runs,
                                   and the event log must still be readable)
  2. a full refresh             -> must FAIL with the BLOCKED message, data untouched
  3. a selective full refresh   -> must FAIL with the BLOCKED message, data untouched
     of the canary table
  4. a normal update again      -> must COMPLETE (the guard must not break recovery)

If any expectation fails the task raises, so the weekly job fails and alerts. A refresh stopped by the guard's
fail-closed path (event not visible yet) is a WARNING, not a failure. Serverless jobs retry a failed task, so a
first-attempt failure can still end as a passing run; check the task attempts if in doubt.
A failure here means the platform changed how a refresh is reported in the
pipeline event log (the `create_update` event's `full_refresh` /
`full_refresh_selection` fields) or event_log() access changed: the guard in
bronze.py can then no longer be trusted, so fix it before the next full
refresh. Run by hand with: databricks bundle run guard_canary_check -t <target>.
"""

import argparse
import time

TABLE = "canary_bronze"


def evaluate_canary(runs: dict) -> tuple[list[str], list[str]]:
    """Pure check of the four runs. Each value: {"state", "error", "rows"}. Returns (failures, warnings).

    A refresh is correctly stopped in two ways: the guard recognises the full refresh and says so
    (BLOCKED ... bronze_rebuild=true) -- healthy; or the update's create_update event was not visible in the
    event log yet and the guard failed closed (BLOCKED ... could not be read) -- data is safe but detection
    did not run, so that is reported as a WARNING (frequent warnings mean event-log visibility degraded).
    """
    failures, warnings = [], []

    def expect_completed(name):
        r = runs.get(name)
        if r is None or r["state"] != "COMPLETED":
            failures.append(
                f"{name}: expected COMPLETED but got {None if r is None else r['state']}"
                + (f" ({r['error'][:300]})" if r and r.get("error") else "")
                + " -- the guard blocked a normal update, or the event log can no longer be read"
            )

    def expect_blocked(name):
        r = runs.get(name)
        if r is None:
            failures.append(f"{name}: did not run")
            return
        error = r.get("error") or ""
        if r["state"] != "FAILED":
            failures.append(f"{name}: expected FAILED (blocked by the guard) but got {r['state']} -- THE GUARD DID NOT STOP A FULL REFRESH")
        elif "BLOCKED" in error and "bronze_rebuild=true" in error:
            pass  # the guard recognised the refresh
        elif "BLOCKED" in error and "could not be read" in error:
            warnings.append(f"{name}: stopped by the guard's fail-closed path (refresh mode could not be read from the event log); data is safe but detection did not run")
        else:
            failures.append(f"{name}: failed, but not with the guard's BLOCKED message: {error[:300]}")
        if r.get("rows") != 1:
            failures.append(f"{name}: canary table has {r.get('rows')} rows after the blocked refresh, expected 1 (data changed)")

    expect_completed("normal_1")
    expect_blocked("full_refresh")
    expect_blocked("selective_refresh")
    expect_completed("normal_2")
    return failures, warnings


def _wait(w, pipeline_id: str, update_id: str, timeout: int = 1500) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = w.pipelines.get_update(pipeline_id, update_id).update.state.value
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            return state
        time.sleep(15)
    return "TIMEOUT"


def _error_text(w, pipeline_id: str, update_id: str) -> str:
    parts = []
    for ev in w.pipelines.list_pipeline_events(pipeline_id=pipeline_id, max_results=100):
        if getattr(ev.origin, "update_id", None) != update_id:
            continue
        if ev.error and ev.error.exceptions:
            parts += [e.message or "" for e in ev.error.exceptions]
        elif ev.level and ev.level.value in ("ERROR", "WARN") and ev.message:
            parts.append(ev.message)
    return " | ".join(parts)


def main() -> None:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", default="canary")
    args = parser.parse_args()

    w = WorkspaceClient()
    spark = SparkSession.builder.getOrCreate()
    table = f"{args.catalog}.{args.schema}.{TABLE}"

    def rows():
        try:
            return spark.table(table).count()
        except Exception:
            return None

    def run(**kwargs) -> dict:
        update_id = w.pipelines.start_update(pipeline_id=args.pipeline_id, **kwargs).update_id
        state = _wait(w, args.pipeline_id, update_id)
        return {"state": state, "error": "" if state == "COMPLETED" else _error_text(w, args.pipeline_id, update_id), "rows": rows()}

    runs = {"normal_1": run()}
    runs["full_refresh"] = run(full_refresh=True)
    runs["selective_refresh"] = run(full_refresh_selection=[TABLE])
    runs["normal_2"] = run()
    for name, r in runs.items():
        print(f"{name}: {r['state']} rows={r['rows']} {r['error'][:160]}")

    failures, warnings = evaluate_canary(runs)
    for w_ in warnings:
        print("WARNING:", w_)
    if failures:
        raise AssertionError("GUARD CANARY FAILED:\n- " + "\n- ".join(failures))
    print("guard canary OK: normal updates run, full and selective refreshes are blocked, data untouched")


if __name__ == "__main__":
    main()
