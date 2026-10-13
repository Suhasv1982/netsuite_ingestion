# Plan: the watchdog triggers missed runs

**Plan only** (owner request 2026-10-13). No code until the plan is approved. Dev only.

## Goal

The monitor already runs daily on GitHub as the external watchdog (it keeps running when Databricks schedules stop: 10-06, 10-11..13). When today's scheduled generator or dev ingestion run did not start, the monitor triggers it through the Jobs API, waits for the result, and writes an incident saying what it did.

## Bug this PR must fix first: a multi-day outage is never reported

`aidq_mcp.logic` (`missed_schedules`) treats every miss that comes before the first scheduled (PERIODIC) run in the 3-day window as `unconfirmed` ("possibly before the schedule existed"). On 2026-10-13 the scheduler had skipped 10-11, 10-12 and 10-13, and the only runs were manual, so all misses were unconfirmed. The monitor's dry run reported "Healthy: no signals".

Fix: a miss is confirmed when the schedule demonstrably existed at that fire time. Two sources:
- the job was created before it (`created_time` from the Jobs API)
- `get_recent_deploys` shows no schedule change between that fire time and now (or, without audit access, the job was created before the window)

Misses stay `unconfirmed` only for a job created inside the window. Unit tests cover the 10-11..10-13 shape: no PERIODIC run in the window, manual runs only.

## Behaviour

1. **When.** At the start of each monitor run, after the Lakebase endpoints are woken (#42) and before the data checks. Only for **today's** fire times (UTC). There is no backfill of earlier days (owner, 2026-10-13): the generator's batch date is the run's start date, so a late run today makes today's batch.
2. **What counts as missed.** A fire time today, older than its grace period (30 min), with no run of that job started since the fire time (any trigger, so a manual run counts), and no run active now. The rule is the same as `missed_schedules`, except that a later manual run also counts as "ran".
3. **Order.**
   - The generator first. Run-now, then wait until it finishes (timeout 20 min).
   - Then dev ingestion (run-now, wait, timeout 45 min), even if the generator failed: ingestion is idempotent and loads whatever the source holds.
   - If only ingestion missed, only it is triggered.
4. **Idempotent by design.**
   - The generator skips a batch that already exists for its date (`--skip-if-batch-exists`).
   - Ingestion uses `once` flows and the key ledger.
   - The watchdog also triggers each job at most once per day: it checks for its own earlier trigger, marked by the run's `job_parameters` / run name.
5. **Never** prod (the dev jobs only, found by name). Never when a run is active. Never in a dry run (`--write` off: it only reports what it would trigger). Kill switch: repository variable `WATCHDOG_TRIGGER=off`.
6. **Incident.**
   - One ORCHESTRATION incident per day of misses, not two. The triage group for the missed schedules gets the watchdog's actions as deterministic facts ("watchdog triggered generator run X at 13:05 UTC: SUCCESS"), and the incident summary starts with "Watchdog triggered ...".
   - The model's RCA stays as it is: why the scheduler did not fire, with the schedule facts from #41.
   - If the incident store is down, the report keeps everything (#41).
7. **If the workspace refuses runs** (`DENY_NEW_AND_EXISTING_RESOURCES` / `INACTIVE`), the incident says "could not trigger: workspace inactive; press Start in the console". The run fails, so GitHub emails.
8. **Exit codes** (from #41): 1 when a new incident was written, which includes every day the watchdog acted. A healthy day exits 0.

## Identity and rights

The monitor's identity, **ci-dev** (GitHub, OAuth M2M).
- It owns the `[dev ci_dev]` jobs, so it may run them.
- The generator job runs as data-generator (job-level `run_as`), which a run-now keeps.
- No new grant is needed. Endpoint waking needs the Lakebase permission from #42 / STATUS.

## Workflow change (ask first: `.github/workflows/`)

`monitor-dev.yml` `timeout-minutes` goes from 30 to 75, because waiting for the triggered runs takes up to about 65 min. Nothing else changes: same identity, same secrets, same concurrency group.

## Known limits

- GitHub starts the scheduled monitor hours late (12:30-15:30 UTC observed), so a missed 05:00 run is caught in the afternoon. That is the same day, so it is still today's batch.
- An extra earlier cron would catch it sooner; that is optional and asked separately.
- If GitHub itself does not run the monitor, nothing triggers. GitHub disables schedules after 60 days without repository activity.

## Tests

- **Unit**, with a fake Jobs client: what counts as missed (grace period, a manual run counts, an active run blocks), order and waiting, at most one trigger per day, dry run, kill switch, workspace inactive, and the incident text.
- **Eval:** a case built from the 10-11 state (both jobs missed today) with a fake trigger client. The incident must say what was triggered and its result, and must not claim the schedules were paused.
- **Dev:** one real run on a day the scheduler skipped, or a simulated miss: pause the dev schedules for one cycle with your OK, then unpause.
