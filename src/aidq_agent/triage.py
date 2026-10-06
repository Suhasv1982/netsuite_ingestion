"""Deterministic triage: turn the five tool results into signals, grouped into investigations. No model involved.

A signal is something a person should look at. Known defects (same-day duplicate versions, guard WARN with 2
event-log reads) are context, not signals. Each group has a stable fingerprint, so the same problem found again
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
            if g.get("classification") != "known_defect_same_day_duplicates":
                out.append(Signal("bronze_gap", "DATA_COMPLETENESS", f"{g['table']}@{g['date']}",
                                  f"{g['table']} {g['date']}: source {g['source']}, bronze {g['bronze']} "
                                  f"(missing {g['missing']}, same-day duplicates {g['same_day_extra']})"))
    return out


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
    known = [g for g in _items(cmp_.get("gaps")) if g.get("classification") == "known_defect_same_day_duplicates"]
    if known:
        notes.append("known defect (not a signal): same-day duplicate versions on "
                     + ", ".join(sorted({str(g["date"]) for g in known})))
    guard = (results.get("get_table_health", {}) or {}).get("latest_guard") or {}
    if guard.get("status") == "WARN" and guard.get("event_log_reads") == 2:
        notes.append("guard WARN with 2 event-log reads is the normal baseline (not a signal)")
    return notes
