"""MLflow tracing for the DQ recommender (plan #39 step 8; design v2 section 8).

One trace per agent run: a root span (inputs, prompt version, totals of tokens and latency), a span per read tool
call, per model call (model, tokens, latency) and per write. The trace id becomes the run's agent_trace_id, so every
proposal and incident links to its trace. Traces go to the experiment `/Shared/aidq/dq_recommender`, bound to the
Unity Catalog schema workspace.aidq_traces (owner choice 2026-10-14; tools/setup_dq_traces.py creates both).

Tracing never decides anything and never crashes the agent (same rule as the LLM call log): if MLflow cannot start,
the run continues untraced with a uuid trace id and the report says why; a failing span is skipped.
"""

from __future__ import annotations

import contextlib
import json
import uuid
from typing import Any

EXPERIMENT = "/Shared/aidq/dq_recommender"
MAX_TEXT = 4000       # long inputs and outputs (prompts, profiles) are cut in span payloads


def _short(value: Any) -> Any:
    """A span payload, JSON-safe and bounded."""
    try:
        text = json.dumps(value, default=str)
    except (TypeError, ValueError):
        text = str(value)
    return value if len(text) <= MAX_TEXT else text[:MAX_TEXT] + f"... ({len(text)} chars)"


class _NullSpan:
    def set_inputs(self, *_):
        pass

    def set_outputs(self, *_):
        pass

    def set_attributes(self, *_):
        pass


class NullTracer:
    """No tracing (local dry runs, evals, tests)."""

    enabled = False
    experiment_id = None
    error: str | None = None

    @contextlib.contextmanager
    def run(self, name: str, inputs: dict | None = None):
        yield uuid.uuid4().hex, _NullSpan()

    @contextlib.contextmanager
    def span(self, name: str, kind: str = "TOOL", inputs: dict | None = None):
        yield _NullSpan()


class MlflowTracer:
    """Fluent MLflow spans. `configure()` sets the tracking URI and experiment; any failure there leaves the tracer
    disabled (`error` says why) and every span a no-op."""

    def __init__(self, experiment: str = EXPERIMENT, tracking_uri: str = "databricks"):
        self.experiment, self.tracking_uri = experiment, tracking_uri
        self.enabled, self.error, self.experiment_id, self._mlflow = False, None, None, None

    def configure(self, warehouse_id: str | None = None) -> "MlflowTracer":
        try:
            import os

            import mlflow

            if warehouse_id:
                os.environ.setdefault("MLFLOW_TRACING_SQL_WAREHOUSE_ID", warehouse_id)
            mlflow.set_tracking_uri(self.tracking_uri)
            exp = mlflow.set_experiment(experiment_name=self.experiment)   # created and bound by the setup script
            self.experiment_id, self._mlflow, self.enabled = exp.experiment_id, mlflow, True
        except Exception as exc:  # noqa: BLE001 -- tracing must never stop the agent
            self.error = f"tracing off ({type(exc).__name__}: {str(exc)[:200]})"
        return self

    @contextlib.contextmanager
    def run(self, name: str, inputs: dict | None = None):
        """The root span; yields (trace id, span). Falls back to an untraced uuid if the span cannot start."""
        if not self.enabled:
            yield uuid.uuid4().hex, _NullSpan()
            return
        try:
            cm = self._mlflow.start_span(name=name, span_type="AGENT")
            span = cm.__enter__()
            span.set_inputs(_short(inputs or {}))
            trace_id = span.trace_id
        except Exception as exc:  # noqa: BLE001
            self.error, self.enabled = f"tracing off ({type(exc).__name__}: {str(exc)[:200]})", False
            yield uuid.uuid4().hex, _NullSpan()
            return
        exc_info = (None, None, None)
        try:
            yield trace_id, span
        except BaseException as exc:
            exc_info = (type(exc), exc, exc.__traceback__)
            raise
        finally:
            try:
                cm.__exit__(*exc_info)
                self._mlflow.flush_trace_async_logging()
            except Exception as exc:  # noqa: BLE001
                self.error = f"trace not exported ({type(exc).__name__}: {str(exc)[:200]})"

    @contextlib.contextmanager
    def span(self, name: str, kind: str = "TOOL", inputs: dict | None = None):
        if not self.enabled:
            yield _NullSpan()
            return
        try:
            cm = self._mlflow.start_span(name=name, span_type=kind)
            span = cm.__enter__()
            span.set_inputs(_short(inputs or {}))
        except Exception:  # noqa: BLE001 -- skip this span, keep the run
            yield _NullSpan()
            return
        exc_info = (None, None, None)
        try:
            yield _SafeSpan(span)
        except BaseException as exc:
            exc_info = (type(exc), exc, exc.__traceback__)
            raise
        finally:
            with contextlib.suppress(Exception):
                cm.__exit__(*exc_info)


class _SafeSpan:
    """A live span whose setters never raise."""

    def __init__(self, span):
        self._span = span

    def set_inputs(self, value):
        with contextlib.suppress(Exception):
            self._span.set_inputs(_short(value))

    def set_outputs(self, value):
        with contextlib.suppress(Exception):
            self._span.set_outputs(_short(value))

    def set_attributes(self, value: dict):
        with contextlib.suppress(Exception):
            self._span.set_attributes({k: v for k, v in value.items() if v is not None})


def trace_link(host: str | None, experiment_id: str | None, trace_id: str | None) -> str | None:
    """The workspace UI link of a trace: /ml/experiments/<id>/traces/<trace>. A UC trace id
    (trace:/catalog.schema/<hex>) is linked by its last component; a legacy id (tr-...) as it is."""
    if not (host and experiment_id and trace_id) or not (trace_id.startswith("trace:/") or trace_id.startswith("tr-")):
        return None
    tid = trace_id.rsplit("/", 1)[-1] if trace_id.startswith("trace:/") else trace_id
    return f"{host.rstrip('/')}/ml/experiments/{experiment_id}/traces/{tid}"
