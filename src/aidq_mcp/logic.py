"""Pure functions behind the tools: no I/O, unit-tested in tests/test_aidq_mcp_logic.py."""

from __future__ import annotations

import datetime as dt
import re
from typing import Any, Iterable

UTC = dt.timezone.utc

# -- bronze vs source ---------------------------------------------------------

def date_gaps(source: dict[tuple, int], bronze: dict[tuple, int], same_day_extra: dict[tuple, int]) -> list[dict]:
    """Per (table, date) where bronze and the source differ (date None for a full-load table). Every gap is a gap:
    since the bronze ledger keys rows by content (key, date, row hash; fix for incident 2026-10-02), a second version
    of a key on a loaded date is loaded too, so same-day versions no longer explain a shortfall. `same_day_extra`
    (rows minus distinct keys on that date) is reported for context. Same rule as tools/daily_check.py."""
    out = []
    for t, d in sorted(set(source) | set(bronze), key=lambda k: (k[0], str(k[1]))):
        s, b, extra = source.get((t, d), 0), bronze.get((t, d), 0), same_day_extra.get((t, d), 0)
        if s != b:
            out.append({"table": t, "date": d, "source": s, "bronze": b, "missing": s - b, "same_day_extra": extra})
    return out


def same_day_versions(rows: Iterable[dict], max_keys: int = 20) -> list[dict]:
    """Info, not a problem: business keys with several source rows on one updated_date day, per (table, date).
    `rows` are {table, d, k}, one row per such key. At most `max_keys` keys are listed per date."""
    grouped: dict[tuple, list] = {}
    for r in rows:
        grouped.setdefault((r["table"], r["d"]), []).append(str(r["k"]))
    return [{"table": t, "date": d, "keys_with_several_versions": len(ks), "keys": sorted(ks)[:max_keys]}
            for (t, d), ks in sorted(grouped.items(), key=lambda x: (x[0][0], str(x[0][1])))]


# -- schedules ----------------------------------------------------------------

def _quartz_field(spec: str, lo: int, hi: int) -> list[int]:
    if spec in ("*", "?"):
        return list(range(lo, hi + 1))
    values = []
    for part in spec.split(","):
        if not part.isdigit() or not lo <= int(part) <= hi:
            raise ValueError(f"unsupported cron field {spec!r}")
        values.append(int(part))
    return sorted(values)


def cron_fire_times(quartz: str, start: dt.datetime, end: dt.datetime) -> list[dt.datetime]:
    """Fire times in [start, end) of a Quartz cron (UTC) with fixed second/minute/hour lists and every day
    ("0 30 5 * * ?"). Anything richer raises ValueError: better no answer than a wrong missed-run report."""
    fields = quartz.split()
    if len(fields) not in (6, 7) or any(f not in ("*", "?") for f in fields[3:6]) or (len(fields) == 7 and fields[6] != "*"):
        raise ValueError(f"unsupported cron {quartz!r} (only fixed times every day)")
    secs, mins, hours = _quartz_field(fields[0], 0, 59), _quartz_field(fields[1], 0, 59), _quartz_field(fields[2], 0, 23)
    out, day = [], start.astimezone(UTC).date()
    while dt.datetime.combine(day, dt.time.min, UTC) < end:
        for h in hours:
            for m in mins:
                for s in secs:
                    t = dt.datetime.combine(day, dt.time(h, m, s), UTC)
                    if start <= t < end:
                        out.append(t)
        day += dt.timedelta(days=1)
    return out


def missed_schedules(quartz: str, paused: bool, run_starts: Iterable[dt.datetime], start: dt.datetime,
                     end: dt.datetime, first_scheduled_run: dt.datetime | None = None,
                     tolerance: dt.timedelta = dt.timedelta(minutes=15),
                     schedule_known_since: dt.datetime | None = None) -> dict:
    """Scheduled fire times in the window with no run started within `tolerance` after them (incident 2026-10-06:
    an unpaused schedule that never triggered). A paused schedule misses nothing; a fire time whose tolerance has
    not passed yet at `end` is not counted.

    Current settings cannot say when the schedule was added or unpaused, so a miss counts as `confirmed` only
    after proof the schedule was active: `first_scheduled_run` (the earliest scheduled run seen) or
    `schedule_known_since` (the current schedule has been in place since then: the later of the job's creation
    and the last deploy that could have changed it). Earlier ones are `unconfirmed`. Without
    `schedule_known_since`, a multi-day outage with only manual runs in the window was never confirmed
    (2026-10-11..13)."""
    if paused:
        return {"confirmed": [], "unconfirmed": []}
    starts = sorted(run_starts)
    missed = [f for f in cron_fire_times(quartz, start, end)
              if f + tolerance <= end and not any(f <= s <= f + tolerance for s in starts)]
    def proven(f: dt.datetime) -> bool:
        return ((first_scheduled_run is not None and f > first_scheduled_run)
                or (schedule_known_since is not None and f > schedule_known_since))

    return {"confirmed": [f for f in missed if proven(f)], "unconfirmed": [f for f in missed if not proven(f)]}


def schedule_known_since(job_created: dt.datetime | None, deploy_times: Iterable[dt.datetime],
                         end: dt.datetime) -> dt.datetime | None:
    """When the job's current schedule was certainly in place: the later of its creation and the last
    successful deploy before `end` (a deploy may change the schedule). None without a deploy history."""
    deploys = [t for t in deploy_times if t <= end]
    if job_created is None or not deploys:
        return None
    return max([job_created, *deploys])


# -- output shaping -----------------------------------------------------------

_URL = re.compile(r"https?://\S+")
_EMAIL = re.compile(r"\b[\w.+-]+@[\w-]+(\.[\w-]+)+\b")
_HOST = re.compile(r"\b[\w-]+(\.[\w-]+)*\.(cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com|"
                   r"database\.[\w-]+\.cloud\.databricks\.com)\b")
_TOKEN = re.compile(r"\b(dapi|dose|eyJ)[\w.-]{16,}\b")


def redact(value: Any) -> Any:
    """Copy of `value` with URLs, emails, workspace/Lakebase hosts and token-like strings masked, recursively."""
    if isinstance(value, str):
        for pattern, mask in ((_URL, "<url>"), (_EMAIL, "<email>"), (_HOST, "<host>"), (_TOKEN, "<token>")):
            value = pattern.sub(mask, value)
        return value
    if isinstance(value, dict):
        return {k: redact(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(v) for v in value]
    return value


def actor_role(actor: str | None, roles: dict[str, str]) -> str:
    """A role name for an audit-log actor: `roles` maps runtime identities (user name, SP application id) to
    roles such as owner or ci-dev; Databricks system actors are 'system'; anyone else 'other'."""
    if not actor:
        return "system"
    if actor in roles:
        return roles[actor]
    if actor.lower() in ("system-user", "system user", "system"):
        return "system"
    return "other"


CREDENTIAL_TTL_MINUTES = 60     # Lakebase database credentials (generate-database-credential) last about an hour
REFRESH_TASK = "refresh_credentials"


def credential_checks(updates: list[dict], refreshes: list[dt.datetime], since: dt.datetime) -> list[dict]:
    """For each FAILED pipeline update since `since`: who started it (JOB_TASK = through the job, whose first task
    refreshes the stored database credentials) and how old the stored credential was then (the last successful
    refresh_credentials task that ended before the update was created). Deterministic; no inference."""
    out = []
    for u in updates:
        created = dt.datetime.fromtimestamp((u.get("creation_time") or 0) / 1000, dt.timezone.utc)
        if u.get("state") != "FAILED" or created < since:
            continue
        before = [r for r in refreshes if r <= created]
        last = max(before) if before else None
        age = int((created - last).total_seconds() // 60) if last else None
        out.append({"update_id": u.get("update_id"), "created": created.isoformat(timespec="seconds"),
                    "cause": u.get("cause"), "started_by_job": u.get("cause") == "JOB_TASK",
                    "last_credential_refresh": last.isoformat(timespec="seconds") if last else None,
                    "credential_age_minutes": age,
                    "credential_expired": None if age is None else age >= CREDENTIAL_TTL_MINUTES})
    return out


# -- known error markers ----------------------------------------------------------

# (marker, pattern, incident category, what it means). Searched in the WHOLE error text: a pipeline failure's text is
# a long traceback whose decisive line is near the end (incident 6: the OAuth rejection; incident 30: the row-hash
# guard's BLOCKED line about 14k characters in), and the tools truncate what they return to ~500 characters.
ERROR_MARKERS: tuple[tuple[str, re.Pattern, str, str], ...] = (
    ("row_hash_guard",
     re.compile(r"BLOCKED: (?:the hashed column set of|no row-hash column set is recorded for)[^\n]*"),
     "SCHEMA_DRIFT",
     "the bronze row-hash guard refused a normal run: the active source_columns of an incremental table differ from "
     "the column set bronze was built with. It fails closed by design until bronze is rebuilt (bronze_rebuild=true); "
     "expected after an approved column change. Not a code bug: the fix is the rebuild, not a code change or rollback"),
    ("full_refresh_guard",
     re.compile(r"BLOCKED: [^\n]*with bronze_rebuild=false[^\n]*"),
     "ORCHESTRATION",
     "the bronze guard refused a full refresh of bronze in a normal run (bronze_rebuild=false) before any data "
     "changed; by design. Not a code bug"),
    ("refresh_mode_unreadable",
     re.compile(r"BLOCKED: [^\n]*\nWHY: its refresh mode could not be read[^\n]*"),
     "OTHER",
     "the bronze guard could not read the update's refresh mode from the event log and stopped before any data "
     "changed; can be transient (the event log is written asynchronously). Not a code bug"),
    ("credential_rejected",
     re.compile(r"[^\n]*OAuth: User is not authorized[^\n]*"),
     "CREDENTIAL_EXPIRED",
     "the source database rejected the stored OAuth credential (expired or invalid)"),
    ("compute_quota",
     re.compile(r"[^\n]*RESOURCE_EXHAUSTED[^\n]*"),
     "COMPUTE_QUOTA",
     "the workspace refused compute (Free Edition quota)"),
)


def error_markers(*texts: str | None) -> list[dict]:
    """The known markers found anywhere in `texts` (full, untruncated error text), each once, in ERROR_MARKERS order:
    {marker, category, meaning, line}. `line` is the matched text, whitespace-collapsed and at most 300 characters."""
    whole = "\n".join(t for t in texts if t)
    out = []
    for name, pattern, category, meaning in ERROR_MARKERS:
        m = pattern.search(whole)
        if m:
            out.append({"marker": name, "category": category, "meaning": meaning,
                        "line": truncate(" ".join(m.group(0).split()), 300)})
    return out


def change_requires_rebuild(change: dict) -> bool:
    """A config change that alters the hashed column set of an incremental table (a source_columns row added,
    removed or changed): normal runs are then blocked by the row-hash guard until bronze is rebuilt. The validator's
    own decision (config_proposals.requires_rebuild) wins when it is set."""
    if change.get("requires_rebuild") is not None:
        return bool(change["requires_rebuild"])
    return change.get("table_name") == "source_columns" and (change.get("load_mode") or "").lower() == "incremental"


def truncate(text: str | None, limit: int = 500) -> str | None:
    if text is None or len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def bounded(items: list, limit: int) -> dict:
    """{'items': first `limit` items, 'total': n, 'truncated': bool}: every list a tool returns goes through this."""
    return {"items": items[:limit], "total": len(items), "truncated": len(items) > limit}


def from_epoch_ms(ms: int | None) -> dt.datetime | None:
    return dt.datetime.fromtimestamp(ms / 1000, UTC) if ms else None
