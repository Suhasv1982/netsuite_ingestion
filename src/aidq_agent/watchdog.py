"""The watchdog: trigger today's missed dev generator / dev ingestion run, wait, and report (docs/watchdog_plan.md).

Runs in the GitHub monitor (the external watchdog) after the Lakebase endpoints are woken and before the data
checks. Rules (owner checklist 2026-10-13):
  * a job is due only when its cron fired today (UTC) more than GRACE ago, its schedule is UNPAUSED, it has no
    run started today and none active;
  * generator first; dev ingestion only after today's generator run ended SUCCESS;
  * at most one trigger per job per day, never a retry (a failed or timed-out run is an outcome, not a reason
    to try again); run-now carries an idempotency token per job and day;
  * only the two allowlisted dev jobs, never anything else (never prod);
  * dry run (write off) and the kill switch (WATCHDOG_TRIGGER=off) trigger nothing and say what they would do.
"""

from __future__ import annotations

import datetime as dt
import time
from dataclasses import asdict, dataclass, field
from typing import Callable, Protocol

from aidq_mcp import logic

UTC = dt.timezone.utc
ALLOWLIST = {"generator": "[dev ci_dev] netsuite_daily_generator",
             "ingestion": "[dev ci_dev] netsuite_ingestion_daily"}
ORDER = ("generator", "ingestion")
REQUIRED_PREFIX = "[dev ci_dev] "
GRACE = dt.timedelta(minutes=30)
WAIT = {"generator": dt.timedelta(minutes=20), "ingestion": dt.timedelta(minutes=45)}
ACTIVE = {"PENDING", "QUEUED", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"}


class JobsClient(Protocol):
    def job_id(self, name: str) -> int | None: ...
    def job(self, job_id: int) -> dict: ...                        # {"settings": {...}, "created_time": ms}
    def runs_since(self, job_id: int, since: dt.datetime) -> list[dict]: ...
    def run_now(self, job_id: int, idempotency_token: str) -> int: ...
    def run_state(self, run_id: int) -> dict: ...                 # {"life_cycle_state", "result_state", ...}


@dataclass
class Action:
    job: str
    name: str
    due: bool = False
    reason: str = ""                  # why it was (not) triggered
    triggered: bool = False
    run_id: int | None = None
    triggered_at: str | None = None
    result: str | None = None         # SUCCESS / FAILED / ... / TIMEOUT / REFUSED
    duration_s: int | None = None
    detail: str | None = None

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class WatchdogResult:
    enabled: bool
    write: bool
    actions: list[Action] = field(default_factory=list)

    @property
    def triggered(self) -> list[Action]:
        return [a for a in self.actions if a.triggered]

    @property
    def healed(self) -> bool:
        return bool(self.triggered) and all(a.result == "SUCCESS" for a in self.triggered)

    def label(self) -> str | None:
        """[self-healed] when every triggered run succeeded, [self-heal failed] when something was due but did
        not end SUCCESS (failed, timed out, refused, or not triggered after a failed generator), else None."""
        due = [a for a in self.actions if a.due]
        if not due or not self.write or not self.enabled:
            return None
        return "[self-healed]" if all(a.triggered and a.result == "SUCCESS" for a in due) else "[self-heal failed]"


def _started(r: dict) -> dt.datetime | None:
    return logic.from_epoch_ms(r.get("start_time"))


def check_allowed(alias: str, name: str) -> None:
    if ALLOWLIST.get(alias) != name or not name.startswith(REQUIRED_PREFIX):
        raise PermissionError(f"watchdog refuses job {name!r}: only {sorted(ALLOWLIST.values())}")


def is_due(settings: dict, runs: list[dict], now: dt.datetime) -> tuple[bool, str]:
    """(due, reason) for one job, from its settings and its runs since the start of today (UTC)."""
    sched = settings.get("schedule") or {}
    if not sched.get("quartz_cron_expression"):
        return False, "no schedule"
    if sched.get("pause_status") == "PAUSED":
        return False, "schedule PAUSED (respected)"
    if sched.get("timezone_id", "UTC") != "UTC":
        return False, "only UTC schedules are handled"
    midnight = dt.datetime.combine(now.date(), dt.time.min, UTC)
    fires = [f for f in logic.cron_fire_times(sched["quartz_cron_expression"], midnight, now) if f + GRACE <= now]
    if not fires:
        return False, "not yet due today"
    if any(r.get("state", {}).get("life_cycle_state") in ACTIVE for r in runs):
        return False, "a run is active"
    today = [r for r in runs if (_started(r) or midnight) >= midnight]
    if today:
        return False, f"already ran today ({len(today)} run(s))"
    return True, f"no run since the {fires[-1]:%H:%M} UTC fire time"


class Watchdog:
    def __init__(self, client: JobsClient, now: Callable[[], dt.datetime] = lambda: dt.datetime.now(UTC),
                 write: bool = False, enabled: bool = True, sleep: Callable[[float], None] = time.sleep,
                 poll_s: float = 30):
        self.client, self.now, self.write, self.enabled, self.sleep, self.poll_s = client, now, write, enabled, sleep, poll_s

    def act(self) -> WatchdogResult:
        out = WatchdogResult(enabled=self.enabled, write=self.write)
        today = self.now().date()
        generator_ok: bool | None = None
        for alias in ORDER:
            name = ALLOWLIST[alias]
            check_allowed(alias, name)
            a = Action(job=alias, name=name)
            out.actions.append(a)
            jid = self.client.job_id(name)
            if jid is None:
                a.reason = "job not found"
                continue
            midnight = dt.datetime.combine(today, dt.time.min, UTC)
            runs = self.client.runs_since(jid, midnight)
            a.due, a.reason = is_due(self.client.job(jid).get("settings", {}), runs, self.now())
            if alias == "generator":
                last = max(runs, key=lambda r: r.get("start_time") or 0, default=None)
                generator_ok = (last or {}).get("state", {}).get("result_state") == "SUCCESS" if last else None
            if not a.due:
                continue
            if alias == "ingestion" and generator_ok is not True:
                a.reason += "; not triggered: today's generator run did not end SUCCESS"
                continue
            if not self.enabled or not self.write:
                a.reason += "; would trigger (" + ("kill switch WATCHDOG_TRIGGER=off" if not self.enabled else "dry run") + ")"
                continue
            self._trigger_and_wait(a, jid, alias, today)
            if alias == "generator":
                generator_ok = a.result == "SUCCESS"
        return out

    def _trigger_and_wait(self, a: Action, jid: int, alias: str, today: dt.date) -> None:
        start = self.now()
        a.triggered_at = start.isoformat(timespec="seconds")
        try:
            a.run_id = self.client.run_now(jid, idempotency_token=f"watchdog-{alias}-{today.isoformat()}")
        except Exception as exc:  # noqa: BLE001 -- refused by the platform: an outcome, never a retry
            text = str(exc)
            a.result = "REFUSED"
            a.detail = ("workspace inactive: press Start in the console" if "INACTIVE" in text
                        or "DENY_NEW_AND_EXISTING_RESOURCES" in text else text[:300])
            return
        a.triggered = True
        deadline = start + WAIT[alias]
        while True:
            state = self.client.run_state(a.run_id)
            if state.get("life_cycle_state") not in ACTIVE:
                a.result = state.get("result_state") or state.get("life_cycle_state")
                a.detail = (state.get("state_message") or None) and logic.truncate(state.get("state_message"))
                break
            if self.now() >= deadline:
                a.result = "TIMEOUT"
                a.detail = f"still {state.get('life_cycle_state')} after {WAIT[alias]}; not retried"
                break
            self.sleep(self.poll_s)
        a.duration_s = int((self.now() - start).total_seconds())


class CliJobsClient:
    """Jobs API through the Databricks CLI (aidq_mcp.reads.Reads.cli): the monitor's identity (ci-dev in CI)."""

    def __init__(self, reads):
        self.reads = reads

    def job_id(self, name: str) -> int | None:
        return self.reads.job_id(name)

    def job(self, job_id: int) -> dict:
        return self.reads.cli("jobs", "get", str(job_id))

    def runs_since(self, job_id: int, since: dt.datetime) -> list[dict]:
        return self.reads.job_runs(job_id, since)

    def run_now(self, job_id: int, idempotency_token: str) -> int:
        r = self.reads.cli("jobs", "run-now", "--no-wait", body={"job_id": job_id, "idempotency_token": idempotency_token})
        return int(r["run_id"])

    def run_state(self, run_id: int) -> dict:
        return self.reads.cli("jobs", "get-run", str(run_id)).get("state", {})


def facts(result: WatchdogResult | None) -> list[str]:
    """Deterministic lines for the model and the report."""
    if result is None:
        return []
    lines = []
    for a in result.actions:
        if a.triggered:
            lines.append(f"fact (watchdog): triggered {a.job} at {a.triggered_at} UTC (run {a.run_id}): {a.result}"
                         + (f" after {a.duration_s} s" if a.duration_s is not None else "")
                         + (f" ({a.detail})" if a.detail else ""))
        elif a.result == "REFUSED":
            lines.append(f"fact (watchdog): could not trigger {a.job}: {a.detail}")
        else:
            lines.append(f"fact (watchdog): {a.job} not triggered: {a.reason}")
    return lines
