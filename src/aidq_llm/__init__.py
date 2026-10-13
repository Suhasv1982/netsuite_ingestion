"""Model routing for the aidq agents (docs/model_routing_plan.md): task -> primary / fallback model, one
deterministic escalation, and a call log (aidq_metadata.llm_calls) that can never crash an agent."""

from .router import CallLog, LoggingChat, Route, Router, load_routes

__all__ = ["CallLog", "LoggingChat", "Route", "Router", "load_routes"]
