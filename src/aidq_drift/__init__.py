"""The schema drift agent (docs/agents_next_design.md section 3; plan docs/phase6_schema_drift_plan.md).

Deterministic code finds the drift (detect.py: the source's information_schema.columns against the active
source_columns); the model only writes the rationale and judges relevance for each change; code submits ADD_COLUMN /
DEACTIVATE_COLUMN proposals and SCHEMA_TYPE_CHANGE / SCHEMA_DRIFT incidents through the shared write toolbox; the
validator checks them and sets requires_rebuild for incremental tables; a person approves. Runs as dq-agent.
"""

VERSION = "0.1"     # proposed_by = schema_drift@<VERSION>
