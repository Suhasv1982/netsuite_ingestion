"""Router: agents call a task, the router picks the model, escalates once, and logs every call.

* `routing.yml` maps task -> primary, fallback, max_tokens (and illustrative prices).
* Escalation is deterministic and happens at most once: on an exception from the model client (after the
  client's own HTTP retries: timeout, endpoint error, rate limit) or when the result fails validation (for the
  monitor: no valid submit_rca), the same attempt runs once on the fallback. If that fails too, the failure is
  returned, never retried again.
* A tool-calling loop is one attempt (`run`), so a monitor investigation restarts on the fallback as a whole.
* Every model call becomes a row in an in-memory CallLog. `CallLog.flush` writes them (aidq_metadata.llm_calls)
  and prunes rows older than retention_days; a failure there never raises: it returns report lines that carry
  the call details instead (owner condition 2026-10-13).
"""

from __future__ import annotations

import datetime as dt
import pathlib
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable, TypeVar

import yaml

ROUTES_FILE = pathlib.Path(__file__).with_name("routing.yml")
T = TypeVar("T")


@dataclass(frozen=True)
class Route:
    task: str
    primary: str
    fallback: str | None
    max_tokens: int


def load_routes(path: pathlib.Path = ROUTES_FILE) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@dataclass
class CallRow:
    agent: str
    task: str
    model: str
    escalated: bool
    escalation_reason: str | None
    input_tokens: int | None
    output_tokens: int | None
    latency_ms: int
    est_cost_usd: float | None
    trace_id: str | None
    outcome: str                      # ok | error | schema_invalid | timeout
    error: str | None = None
    called_at: str = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"))

    def line(self) -> str:
        return (f"{self.called_at} {self.task} {self.model}{' (fallback: ' + str(self.escalation_reason) + ')' if self.escalated else ''}"
                f" {self.outcome} {self.input_tokens}/{self.output_tokens} tok {self.latency_ms} ms"
                + (f" error={self.error}" if self.error else ""))


class CallLog:
    def __init__(self, retention_days: int = 90):
        self.rows: list[CallRow] = []
        self.retention_days = retention_days

    def add(self, row: CallRow) -> None:
        self.rows.append(row)

    def flush(self, writer) -> list[str]:
        """Write the rows with `writer` (None: nothing is written, e.g. a dry run) and prune old rows. Never
        raises: on failure the returned report lines list every call instead."""
        if not self.rows:
            return []
        if writer is None:
            return [f"LLM calls (not logged: dry run): {len(self.rows)}"] + [f"  {r.line()}" for r in self.rows]
        try:
            written, pruned = writer.write([asdict(r) for r in self.rows], self.retention_days)
        except Exception as exc:  # noqa: BLE001 -- logging must never crash an agent
            return ([f"LLM call log NOT written ({type(exc).__name__}: {str(exc)[:200]}); the calls were:"]
                    + [f"  {r.line()}" for r in self.rows])
        return [f"LLM calls: {written} logged to llm_calls (pruned {pruned} older than {self.retention_days} days)"]


def _classify(exc: BaseException) -> str:
    text = str(exc).lower()
    return "timeout" if isinstance(exc, TimeoutError) or "timed out" in text or "timeout" in text else "error"


class LoggingChat:
    """Wraps a chat client (`complete(model, messages, tools, max_tokens)`) and logs every call."""

    def __init__(self, chat, log: CallLog, agent: str, task: str, prices: dict, trace_id: str | None,
                 escalated: bool = False, escalation_reason: str | None = None):
        self.chat, self.log, self.agent, self.task, self.prices = chat, log, agent, task, prices
        self.trace_id, self.escalated, self.escalation_reason = trace_id, escalated, escalation_reason

    def _row(self, model, started, outcome, usage=None, error=None) -> CallRow:
        usage = usage or {}
        tin, tout = usage.get("prompt_tokens"), usage.get("completion_tokens")
        price = self.prices.get(model)
        cost = round((tin or 0) * price[0] / 1e6 + (tout or 0) * price[1] / 1e6, 6) if price else None
        return CallRow(self.agent, self.task, model, self.escalated, self.escalation_reason, tin, tout,
                       int((time.monotonic() - started) * 1000), cost, self.trace_id, outcome,
                       None if error is None else str(error)[:500])

    async def complete(self, model: str, messages: list[dict], tools: list[dict] | None = None,
                       max_tokens: int = 4000) -> dict:
        started = time.monotonic()
        try:
            resp = await self.chat.complete(model, messages, tools, max_tokens=max_tokens)
        except Exception as exc:
            self.log.add(self._row(model, started, _classify(exc), error=exc))
            raise
        self.log.add(self._row(model, started, "ok", resp.get("usage")))
        return resp


class Router:
    def __init__(self, chat, agent: str, log: CallLog | None = None, routes: dict | None = None,
                 pinned_model: str | None = None, trace_id: str | None = None):
        """`pinned_model`: use this model for every task, never escalate (evals comparing models, --model)."""
        self.routes = routes or load_routes()
        self.chat, self.agent, self.pinned, self.trace_id = chat, agent, pinned_model, trace_id
        self.log = log or CallLog(self.routes.get("retention_days", 90))
        self.prices = {m: tuple(p) for m, p in (self.routes.get("illustrative_prices_usd_per_1m_tokens") or {}).items()}

    def route(self, task: str) -> Route:
        t = (self.routes.get("tasks") or {}).get(task)
        if t is None:
            raise KeyError(f"unknown task {task!r}; add it to src/aidq_llm/routing.yml")
        if self.pinned:
            return Route(task, self.pinned, None, int(t.get("max_tokens", 4000)))
        return Route(task, t["primary"], t.get("fallback"), int(t.get("max_tokens", 4000)))

    def chat_for(self, task: str, escalated: bool = False, reason: str | None = None) -> LoggingChat:
        return LoggingChat(self.chat, self.log, self.agent, task, self.prices, self.trace_id, escalated, reason)

    async def run(self, task: str, attempt: Callable[[str, LoggingChat], Awaitable[T]],
                  ok: Callable[[T], tuple[bool, str | None]]) -> T:
        """Run `attempt(model, chat)` on the primary; escalate once to the fallback when it raises or `ok(result)`
        is false. Returns the last result; re-raises only if the last attempt raised."""
        route = self.route(task)
        try:
            result = await attempt(route.primary, self.chat_for(task))
            good, why = ok(result)
            if good:
                return result
            # `ok` may classify the failure itself ("error: ...", "timeout: ..."): an endpoint failure that the
            # attempt caught must not be logged as a schema failure
            kind = (why or "").split(":", 1)[0]
            if kind in ("error", "timeout"):
                reason = why
            else:
                reason = f"schema_invalid: {why}" if why else "schema_invalid"
                self._mark_last(route.primary, "schema_invalid", why)
        except Exception as exc:
            if route.fallback is None:
                raise
            reason = f"{_classify(exc)}: {str(exc)[:200]}"
            result = None
        if route.fallback is None:
            return result
        return await attempt(route.fallback, self.chat_for(task, escalated=True, reason=reason))

    async def complete(self, task: str, messages: list[dict], tools: list[dict] | None = None,
                       validate: Callable[[dict], tuple[bool, str | None]] | None = None) -> dict:
        """One call with the same escalation rule (used by single-shot callers such as the eval judge)."""
        route = self.route(task)

        async def attempt(model, chat):
            return await chat.complete(model, messages, tools, max_tokens=route.max_tokens)

        return await self.run(task, attempt, validate or (lambda r: (True, None)))

    def _mark_last(self, model: str, outcome: str, why: str | None) -> None:
        for row in reversed(self.log.rows):
            if row.model == model and row.outcome == "ok":
                row.outcome, row.error = outcome, (why or None) and str(why)[:500]
                return


class PgCallLogWriter:
    """Writes llm_calls rows as the caller (ci-dev in CI; aidq_llm_logger) and prunes by retention."""

    COLS = ("called_at", "agent", "task", "model", "escalated", "escalation_reason", "input_tokens", "output_tokens",
            "latency_ms", "est_cost_usd", "trace_id", "outcome", "error")

    def __init__(self, connect: Callable[[], Any]):
        self.connect = connect

    def write(self, rows: list[dict], retention_days: int) -> tuple[int, int]:
        sql = (f"INSERT INTO aidq_metadata.llm_calls ({', '.join(self.COLS)}) "
               f"VALUES ({', '.join(['%s'] * len(self.COLS))})")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, [tuple(r[c] for c in self.COLS) for r in rows])
            pruned = conn.execute("SELECT aidq_metadata.prune_llm_calls(%s)", (retention_days,)).fetchone()[0]
            conn.commit()
        return len(rows), int(pruned)
