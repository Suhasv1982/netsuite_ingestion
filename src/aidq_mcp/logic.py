"""Pure functions behind the tools: no I/O, unit-tested in tests/test_aidq_mcp_logic.py."""

from __future__ import annotations

import datetime as dt
import re
from typing import Any, Iterable

UTC = dt.timezone.utc

# -- bronze vs source ---------------------------------------------------------

KNOWN_DEFECT = "known_defect_same_day_duplicates"
UNEXPLAINED = "unexplained"


def classify_date_gaps(source: dict[tuple, int], bronze: dict[tuple, int],
                       same_day_extra: dict[tuple, int]) -> list[dict]:
    """Per (table, date) where bronze and the source differ (date None for a full-load table).

    A date short by exactly the source's same-day duplicate versions on that date (rows minus distinct keys) is a
    known defect: the bronze key ledger tracks distinct (key, date) pairs, so a second version of a key on an
    already-loaded date never reaches bronze (incident 2026-10-02). Same rule as tools/daily_check.py."""
    out = []
    for t, d in sorted(set(source) | set(bronze), key=lambda k: (k[0], str(k[1]))):
        s, b, extra = source.get((t, d), 0), bronze.get((t, d), 0), same_day_extra.get((t, d), 0)
        if s == b:
            continue
        out.append({"table": t, "date": d, "source": s, "bronze": b, "missing": s - b, "same_day_extra": extra,
                    "classification": KNOWN_DEFECT if extra and s - b == extra else UNEXPLAINED})
    return out


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
                     tolerance: dt.timedelta = dt.timedelta(minutes=15)) -> dict:
    """Scheduled fire times in the window with no run started within `tolerance` after them (incident 2026-10-06:
    an unpaused schedule that never triggered). A paused schedule misses nothing; a fire time whose tolerance has
    not passed yet at `end` is not counted.

    Current settings cannot say when the schedule was added or unpaused, so a miss counts as `confirmed` only
    after `first_scheduled_run` (the earliest scheduled run seen, proof the schedule was active). Earlier ones
    are `unconfirmed`: possibly before the schedule existed."""
    if paused:
        return {"confirmed": [], "unconfirmed": []}
    starts = sorted(run_starts)
    missed = [f for f in cron_fire_times(quartz, start, end)
              if f + tolerance <= end and not any(f <= s <= f + tolerance for s in starts)]
    if first_scheduled_run is None:
        return {"confirmed": [], "unconfirmed": missed}
    return {"confirmed": [f for f in missed if f > first_scheduled_run],
            "unconfirmed": [f for f in missed if f < first_scheduled_run]}


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


def truncate(text: str | None, limit: int = 500) -> str | None:
    if text is None or len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def bounded(items: list, limit: int) -> dict:
    """{'items': first `limit` items, 'total': n, 'truncated': bool}: every list a tool returns goes through this."""
    return {"items": items[:limit], "total": len(items), "truncated": len(items) > limit}


def from_epoch_ms(ms: int | None) -> dt.datetime | None:
    return dt.datetime.fromtimestamp(ms / 1000, UTC) if ms else None
