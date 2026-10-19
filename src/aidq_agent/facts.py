"""Deterministic facts step, before the model: what the tool results establish, as `fact (...)` lines.

The model gets these lines as verified context and must not contradict them. Each comes from a tool result or the
monitor's own start-up, never from inference. Why: twice the model diagnosed a failure from a truncated traceback
(incident 6, an expired credential, called "transient"; incident 30, a row-hash guard block 34 minutes after an
approved column change, called a code bug with a rollback recommended). The facts are:

* error markers: known causes found in the FULL error text (aidq_mcp.logic.ERROR_MARKERS), such as the guard's
  BLOCKED line, with what they mean; a guard block is tied to the applied column change that explains it
* recovery: for each job with a failed run, what the runs after it did (still failing, or recovered)
* credential age and who started each failed update (incident 6)
* schedules when a run was missed (incident 1)
* Lakebase endpoint state, and endpoints the monitor had to re-enable before its checks
* recent deploys
* config changes applied to the metadata recently (config_change_log via get_platform_state)
"""

from __future__ import annotations

import datetime as dt
from typing import Iterable

MAX_CHANGES = 10
MAX_DEPLOYS = 5


def _items(block) -> list:
    return block.get("items", []) if isinstance(block, dict) else []


def _ok(results: dict, tool: str) -> dict:
    r = results.get(tool) or {}
    return r if r.get("status") == "ok" else {}


def _ts(value) -> dt.datetime | None:
    try:
        t = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def facts_from(results: dict[str, dict], startup_log: Iterable[str] = ()) -> list[str]:
    return (marker_facts(results) + recovery_facts(results) + credential_facts(results) + schedule_facts(results)
            + endpoint_facts(results) + startup_facts(startup_log) + deploy_facts(results)
            + config_change_facts(results))


def marker_facts(results: dict[str, dict]) -> list[str]:
    """One line per (marker, line) found in failed pipeline events and FAILED run_audit rows, with when it occurred
    and what it means; for a row-hash guard block, the applied change that requires the rebuild, if any."""
    found: dict[tuple, dict] = {}
    errs = _ok(results, "get_pipeline_errors")
    hits = [(m, e.get("time"), e.get("update_id")) for p in errs.get("pipelines") or []
            for e in _items(p.get("events")) if e.get("level") == "ERROR" for m in e.get("markers") or []]
    hits += [(m, a.get("started_at"), None) for a in _items(errs.get("failed_run_audit")) for m in a.get("markers") or []]
    for m, when, update in hits:
        f = found.setdefault((m["marker"], m["line"]), {**m, "times": [], "updates": set()})
        if when:
            f["times"].append(str(when))
        if update:
            f["updates"].add(str(update)[:8])
    out = []
    for f in found.values():
        times = sorted(f["times"])
        span = f"{times[0]} to {times[-1]}" if len(times) > 1 else (times[0] if times else "in the window")
        who = f"{len(f['updates'])} failed pipeline update(s)" if f["updates"] else "failed run_audit row(s)"
        line = (f"fact (from get_pipeline_errors, full error text): {who}, {span}, stopped with: \"{f['line']}\". "
                f"Meaning: {f['meaning']}. Category: {f['category']}.")
        if f["marker"] == "row_hash_guard":
            line += " " + _guard_cause(results, times[0] if times else None)
        out.append(line)
    return out


def _guard_cause(results: dict, first_failure: str | None) -> str:
    changes = [c for c in _items(_ok(results, "get_platform_state").get("applied_config_changes"))
               if c.get("requires_bronze_rebuild")]
    first = _ts(first_failure) if first_failure else None
    before = [c for c in changes if first is None or ((t := _ts(c.get("changed_at"))) and t <= first)]
    if not before:
        return "No applied change requiring a rebuild is listed in the window (get_platform_state)."
    c = max(before, key=lambda x: str(x.get("changed_at")))
    return (f"It follows the config change applied at {c.get('changed_at')}: {c.get('operation')} "
            f"{c.get('source_table')}.{c.get('column_name')} (proposal {c.get('proposal_id')}, "
            f"{c.get('proposal_type')}{_approved(c)}), which requires a bronze rebuild: the guard failed closed as "
            "designed.")


def _approved(c: dict) -> str:
    return ", approved and applied" if c.get("proposal_status") in ("APPROVED", "APPLIED") else ""


FAILED = {"FAILED", "SUCCESS_WITH_FAILURES", "TIMEDOUT", "CANCELED", "INTERNAL_ERROR"}


def recovery_facts(results: dict[str, dict]) -> list[str]:
    """For each job with a failed run in the window: the runs that started after its last failure and their results,
    so a failure that has already been fixed is not reported as ongoing (incident 30: the model told the owner to
    rebuild bronze, which had been done hours before)."""
    out = []
    for j in _ok(results, "get_recent_job_runs").get("jobs") or []:
        runs = sorted(_items(j.get("runs")), key=lambda r: str(r.get("start")))
        failed = [r for r in runs if r.get("result_state") in FAILED]
        if not failed:
            continue
        last = failed[-1]
        after = [r for r in runs if str(r.get("start")) > str(last.get("start"))]
        done = [r for r in after if r.get("result_state")]
        head = (f"fact (from get_recent_job_runs): {j.get('job')}: last failed run started {last.get('start')} "
                f"({last.get('result_state')}); ")
        if not after:
            out.append(head + "no run has started since, so it is not known whether the problem is fixed")
        elif done and all(r.get("result_state") == "SUCCESS" for r in done):
            out.append(head + f"all {len(done)} finished run(s) since succeeded ("
                       + ", ".join(f"{r.get('start')} {r.get('trigger')}" for r in done)
                       + "): the failure is not ongoing")
        else:
            out.append(head + "runs since: " + ", ".join(f"{r.get('start')} {r.get('result_state') or 'running'}"
                                                         for r in after))
    return out


def credential_facts(results: dict[str, dict]) -> list[str]:
    """For each failed ingestion pipeline update: who started it and how old the stored database credential was, as
    deterministic facts (get_pipeline_errors.failed_updates). Incident 6 (2026-10-14): a manual update started on the
    pipeline, not through the job, skipped refresh_credentials and failed on an expired token; the model, seeing only
    a truncated traceback, called it a transient environment error."""
    out = []
    for p in (results.get("get_pipeline_errors", {}) or {}).get("pipelines") or []:
        fails = p.get("failed_updates")
        if not isinstance(fails, list):
            continue
        for f in fails:
            who = ("through the job (refresh_credentials ran first)" if f.get("started_by_job")
                   else f"by {f.get('cause') or 'unknown'}, not through the job, so refresh_credentials did not run first")
            if f.get("credential_age_minutes") is None:
                age = "no successful credential refresh found in the 2 days before it"
            else:
                h, m = divmod(f["credential_age_minutes"], 60)
                age = (f"the stored database credential was last refreshed {f['last_credential_refresh']}, {h} h {m} min "
                       f"before ({'EXPIRED' if f.get('credential_expired') else 'still valid'}: credentials last about "
                       f"60 min)")
            out.append(f"fact (from get_pipeline_errors): pipeline update {str(f.get('update_id'))[:8]} failed at "
                       f"{f.get('created')}; started {who}; {age}")
    return out


def schedule_facts(results: dict[str, dict]) -> list[str]:
    """When a scheduled run was missed: each job's schedule as the tools report it (pause state, cron) and its last
    config change, as deterministic facts. The model must not have to infer them: in evals and in incident 1 it
    left out the pause state or guessed the schedules "may have been paused" although they were UNPAUSED."""
    jobs = (results.get("get_recent_job_runs", {}) or {}).get("jobs") or []
    missed = [j for j in jobs if j.get("missed_schedules")]
    if not missed:
        return []
    states = []
    for j in jobs:
        sched = j.get("schedule") or {}
        if sched:
            states.append(f"{j.get('job')} {sched.get('pause_status', 'unknown')} (cron {sched.get('cron')} "
                          f"{sched.get('timezone', '')})".rstrip())
    facts = ["fact (from get_recent_job_runs): schedules as configured now: " + "; ".join(states)]
    deploys = results.get("get_recent_deploys", {}) or {}
    changes = deploys.get("config_changes")
    if isinstance(changes, dict) and "unavailable" in changes:
        facts.append("fact: config-change history is unavailable to this identity (no audit access), so a schedule "
                     "change cannot be seen directly; GitHub deploy runs are listed by get_recent_deploys")
    else:
        names = {j.get("job") for j in missed}
        last: dict[str, dict] = {}
        for c in _items(changes):
            job = str(c.get("resource", "")).removeprefix("job:")
            if job in names and (job not in last or str(c.get("time")) > str(last[job].get("time"))):
                last[job] = c
        for job in sorted(names):
            c = last.get(job)
            facts.append(f"fact (from get_recent_deploys): last config change to {job}: "
                         + (f"{c.get('time')} {c.get('action')} by {c.get('actor')}" if c else "none in the window")
                         + "; no pause or unpause action appears for it")
    return facts


def endpoint_facts(results: dict[str, dict]) -> list[str]:
    eps = _ok(results, "get_platform_state").get("endpoints") or []
    parts = []
    for e in eps:
        if "unavailable" in e:
            parts.append(f"{e.get('endpoint')} unknown ({e['unavailable']})")
        else:
            parts.append(f"{e.get('endpoint')} {'DISABLED' if e.get('disabled') else 'enabled'}"
                         + (f" ({e['state']})" if e.get("state") else ""))
    return ["fact (from get_platform_state): Lakebase endpoints now: " + "; ".join(parts)] if parts else []


def startup_facts(startup_log: Iterable[str]) -> list[str]:
    """The monitor re-enables disabled endpoints before its checks (owner decision 2026-10-13); the tools then see
    them enabled, so say that they were not."""
    return [f"fact (monitor start-up): {line}" for line in startup_log if "ENABLED Lakebase endpoint" in line]


def deploy_facts(results: dict[str, dict]) -> list[str]:
    deploys = _items(_ok(results, "get_recent_deploys").get("deploys"))
    if not deploys:
        return []
    latest = sorted(deploys, key=lambda d: str(d.get("created")), reverse=True)[:MAX_DEPLOYS]
    return ["fact (from get_recent_deploys): latest deploy workflow runs: " + "; ".join(
        f"{d.get('created')} {d.get('workflow')} {d.get('conclusion') or d.get('status')} {d.get('sha')} "
        f"\"{d.get('title')}\"" for d in latest)]


def config_change_facts(results: dict[str, dict]) -> list[str]:
    changes = _items(_ok(results, "get_platform_state").get("applied_config_changes"))
    if not changes:
        return []
    out = []
    for c in sorted(changes, key=lambda x: str(x.get("changed_at")), reverse=True)[:MAX_CHANGES]:
        if c.get("column_name"):
            what = f"column {c.get('source_table')}.{c['column_name']}"
        elif c.get("rule_name"):
            what = f"rule '{c['rule_name']}' ({c.get('severity')}) on {c.get('source_table')}"
        else:
            what = c.get("source_table") or "a row"
        out.append(f"{c.get('changed_at')} {c.get('operation')} {c.get('table_name')}: {what}"
                   + (f" (proposal {c['proposal_id']} {c.get('proposal_type')}{_approved(c)})"
                      if c.get("proposal_id") else "")
                   + ("; requires a bronze rebuild (normal runs are BLOCKED by the row-hash guard until then)"
                      if c.get("requires_bronze_rebuild") else ""))
    more = f" (+{len(changes) - MAX_CHANGES} more)" if len(changes) > MAX_CHANGES else ""
    return ["fact (from get_platform_state): config changes applied recently, newest first: " + " | ".join(out) + more]
