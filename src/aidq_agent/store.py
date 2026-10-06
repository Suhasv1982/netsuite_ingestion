"""The agent's one write: INSERT into aidq_metadata.incidents (dev). Dedupe is enforced by migration 005's unique
index on OPEN fingerprints; ON CONFLICT DO NOTHING makes a re-run a no-op. DryRunStore writes nothing."""

from __future__ import annotations

import json
from typing import Protocol

INSERT_SQL = (
    "INSERT INTO aidq_metadata.incidents (run_id, category, summary, suggested_fix, agent_trace_id, fingerprint, "
    "evidence) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) "
    "ON CONFLICT (fingerprint) WHERE status = 'OPEN' AND fingerprint IS NOT NULL DO NOTHING RETURNING incident_id")
OPEN_SQL = "SELECT fingerprint FROM aidq_metadata.incidents WHERE status = 'OPEN' AND fingerprint = ANY(%s)"


class IncidentStore(Protocol):
    def open_fingerprints(self, fingerprints: list[str]) -> set[str]: ...
    def insert(self, incident: dict) -> int | None: ...


def _row(incident: dict) -> tuple:
    return (incident.get("run_id"), incident["category"], incident["summary"], incident.get("suggested_fix"),
            incident["agent_trace_id"], incident["fingerprint"], json.dumps(incident["evidence"], default=str))


class DryRunStore:
    def __init__(self, open_fps: set[str] | None = None):
        self.open_fps, self.inserted = set(open_fps or ()), []

    def open_fingerprints(self, fingerprints: list[str]) -> set[str]:
        return self.open_fps & set(fingerprints)

    def insert(self, incident: dict) -> int | None:
        if incident["fingerprint"] in self.open_fps:
            return None
        self.open_fps.add(incident["fingerprint"])
        self.inserted.append(incident)
        return None          # nothing written: no id


class PgIncidentStore:
    """Writes as the caller's identity (ci-dev in CI, the CLI profile locally) to the dev metadata endpoint."""

    def __init__(self, reads, endpoint: str):
        self.reads, self.endpoint = reads, endpoint

    def _connect(self):
        import psycopg

        ep = self.reads.cli("postgres", "get-endpoint", self.endpoint)
        if ep.get("status", {}).get("disabled"):
            raise RuntimeError("metadata endpoint is disabled (the agent never enables it)")
        token = self.reads.cli("postgres", "generate-database-credential", self.endpoint)["token"]
        return psycopg.connect(host=ep["status"]["hosts"]["host"], dbname="databricks_postgres",
                               user=self.reads.current_user(), password=token, sslmode="require", connect_timeout=60)

    def open_fingerprints(self, fingerprints: list[str]) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute(OPEN_SQL, (fingerprints,)).fetchall()
            conn.rollback()
        return {r[0] for r in rows}

    def insert(self, incident: dict) -> int | None:
        with self._connect() as conn:
            row = conn.execute(INSERT_SQL, _row(incident)).fetchone()
            conn.commit()
        return row[0] if row else None
