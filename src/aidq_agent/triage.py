"""Deterministic triage: turn the five tool results into signals, grouped into investigations. No model involved.

A signal is something a person should look at. Every bronze-vs-source gap is one. Context, not signals: business
keys with several same-day versions (loaded since bronze keys rows by content) and the guard WARN with 2 event-log
reads. Each group has a stable fingerprint, so the same problem found again
the next day maps to the same OPEN incident (migration 005: one OPEN incident per fingerprint).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

TOOLS = ("get_recent_job_runs", "get_table_health", "get_pipeline_errors", "compare_bronze_to_source",
         "get_recent_deploys")
NOT_FAILED = {"SUCCESS", None}


@dataclass(frozen=True)
class Signal:
    kind: str          # missed_schedule, run_failed, pipeline_error, audit_failed, reject_threshold, bronze_gap, tool_unavailable, tool_error
    category: str      # aidq_metadata.incidents category
    key: str           # what makes this signal distinct (job + fire time, table + date, ...)
    detail: str        # one line for people and for the model


@dataclass
class Group:
    category: str
    signals: list[Signal] = field(default_factory=list)

    @property
    def fingerprint(self) -> str:
        keys = "|".join(sorted(f"{s.kind}:{s.key}" for s in self.signals))
        return f"{self.category.lower()}:{hashlib.sha256(keys.encode()).hexdigest()[:16]}"


def _items(block) -> list:
    return block.get("items", []) if isinstance(block, dict) else []


def signals_from(results: dict[str, dict]) -> list[Signal]:
    out: list[Signal] = []
    for tool in TOOLS:
        r = results.get(tool)
        if r is None:
            continue
        if r.get("status") == "unavailable":
            out.append(Signal("tool_unavailable", "SOURCE_UNAVAILABLE", tool, f"{tool}: {r.get('reason')}"))
        elif r.get("status") == "error":
            out.append(Signal("tool_error", "OTHER", tool, f"{tool}: {r.get('error')}"))

    runs = results.get("get_recent_job_runs", {})
    for job in runs.get("jobs", []) if runs.get("status") == "ok" else []:
        for fire in job.get("missed_schedules") or []:
            out.append(Signal("missed_schedule", "ORCHESTRATION", f"{job['job']}@{fire}",
                              f"{job['name']}: no run started for the scheduled time {fire} "
                              f"(schedule {(job.get('schedule') or {}).get('pause_status')})"))
        for run in _items(job.get("runs")):
            if run.get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED") and \
                    run.get("result_state") not in NOT_FAILED:
                out.append(Signal("run_failed", "OTHER", f"{job['job']}#{run.get('run_id')}",
                                  f"{job['name']} run {run.get('run_id')} at {run.get('start')}: "
                                  f"{run.get('result_state')} {run.get('state_message') or ''}".strip()))

    errs = results.get("get_pipeline_errors", {})
    if errs.get("status") == "ok":
        for p in errs.get("pipelines", []):
            for e in _items(p.get("events")):
                if e.get("level") == "ERROR":
                    out.append(Signal("pipeline_error", "OTHER", f"{p['pipeline']}:{e.get('update_id') or e.get('time')}",
                                      f"{p['name']} {e.get('time')}: {e.get('message')} {e.get('exception') or ''}".strip()))
        for a in _items(errs.get("failed_run_audit")):
            out.append(Signal("audit_failed", "OTHER", f"{a.get('run_id')}:{a.get('source_table')}:{a.get('layer')}",
                              f"run_audit FAILED {a.get('source_table')}/{a.get('layer')} run {a.get('run_id')}: "
                              f"{a.get('error')}"))

    health = results.get("get_table_health", {})
    if health.get("status") == "ok":
        for t in health.get("tables", []):
            if t.get("threshold_breached"):
                out.append(Signal("reject_threshold", "REJECT_THRESHOLD", f"{t['source_table']}:{t['layer']}:{t.get('run_id')}",
                                  f"{t['source_table']}/{t['layer']}: reject rate {t.get('reject_rate')} > "
                                  f"threshold {t.get('discard_threshold')}"))

    cmp_ = results.get("compare_bronze_to_source", {})
    if cmp_.get("status") == "ok":
        for g in _items(cmp_.get("gaps")):
            out.append(Signal("bronze_gap", "DATA_COMPLETENESS", f"{g['table']}@{g['date']}",
                              f"{g['table']} {g['date']}: source {g['source']}, bronze {g['bronze']} "
                              f"(missing {g['missing']}, same-day versions {g.get('same_day_extra', 0)})"))
    return out


def watchdog_signals(result) -> list[Signal]:
    """The watchdog's due jobs as ORCHESTRATION signals (grouped with the missed schedules, so one incident per
    day says both why the scheduler did not fire and what the watchdog did)."""
    if result is None:
        return []
    return [Signal("self_healed", "ORCHESTRATION", f"watchdog:{a.job}",
                   f"watchdog: {a.job} due ({a.reason}); "
                   + (f"triggered run {a.run_id}: {a.result}" if a.triggered else (a.result or "not triggered")))
            for a in result.actions if a.due]


def group(signals: list[Signal]) -> list[Group]:
    """One investigation per category: e.g. both 10-06 missed runs are one ORCHESTRATION incident."""
    groups: dict[str, Group] = {}
    for s in signals:
        groups.setdefault(s.category, Group(s.category)).signals.append(s)
    return sorted(groups.values(), key=lambda g: g.category)


def context_notes(results: dict[str, dict]) -> list[str]:
    """Known, non-signal facts worth telling the model (so it does not rediscover them as causes)."""
    notes = []
    cmp_ = results.get("compare_bronze_to_source", {})
    versions = _items(cmp_.get("same_day_versions"))
    if versions:
        notes.append("info (not a signal): business keys with several versions on the same day, all loaded into "
                     "bronze, silver keeps one per key: "
                     + ", ".join(f"{v['table']}@{v['date']} ({v['keys_with_several_versions']})" for v in versions))
    guard = (results.get("get_table_health", {}) or {}).get("latest_guard") or {}
    if guard.get("status") == "WARN" and guard.get("event_log_reads") == 2:
        notes.append("guard WARN with 2 event-log reads is the normal baseline (not a signal)")
    notes += schedule_facts(results)
    notes += credential_facts(results)
    return notes


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
