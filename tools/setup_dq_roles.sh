#!/usr/bin/env bash
# One-time setup of the DQ recommender identities in DEV. Run by the OWNER (role creation and grants are
# owner-only); Claude prepared it and does not run it. From the repo root, in Git Bash:
#
#   ! bash tools/setup_dq_roles.sh DEFAULT
#
# Idempotent: every step skips what already exists. Dev only: it touches the aidq-metadata/dev branch, the
# workspace catalog and the dev warehouse; nothing in prod.
#   1. service principal dq-agent (no OAuth secret yet: that and its GitHub secrets are a separate, asked-for step)
#   2. its Lakebase login role on aidq-metadata/dev (no membership roles: default privileges only)
#   3. group roles aidq_agent / aidq_validator / aidq_reviewer, memberships and privileges (grants/dev.yml
#      metadata_roles), in one transaction, then an exact-match check
#   4. schema workspace.generator, then the owner-applied Unity Catalog grants for dq-agent and data-generator
#      (grants/dev.yml), then a check
#   5. "Service Principal User" on dq-agent for ci-dev, so deploy-dev can deploy the DQ recommender job that runs
#      as dq-agent (grants/dev.yml service_principal_users; tools/sp_roles.py), then an exact-match check
#   6. the DQ recommender's trace store: experiment /Shared/aidq/dq_recommender bound to workspace.aidq_traces
#      (created in step 4), dq-agent CAN_EDIT on it (tools/setup_dq_traces.py; plan #39 step 8)
# Requires migration 006 on dev (approved_severity) before step 3: deploy-dev applies it on merge.
set -euo pipefail
PROFILE="${1:?usage: setup_dq_roles.sh <databricks profile>}"
PY="${PY:-.venv/Scripts/python}"
[ -x "$PY" ] || PY=python
BRANCH=projects/aidq-metadata/branches/dev

sp_id() {  # application id of a service principal by display name, empty if none
  databricks service-principals list --profile "$PROFILE" -o json |
    "$PY" -c "import json,sys; print(next((s['applicationId'] for s in json.load(sys.stdin) if s.get('displayName') == '$1'), ''))"
}

# 1. service principal
AGENT=$(sp_id dq-agent)
if [ -z "$AGENT" ]; then
  databricks service-principals create --profile "$PROFILE" --json '{"displayName": "dq-agent", "active": true}' >/dev/null
  AGENT=$(sp_id dq-agent)
  echo "created service principal dq-agent: $AGENT"
else
  echo "service principal dq-agent exists: $AGENT"
fi
CI=$(sp_id ci-dev)
GEN=$(sp_id data-generator)
OWNER=$(databricks current-user me --profile "$PROFILE" -o json | "$PY" -c "import json,sys; print(json.load(sys.stdin)['userName'])")
[ -n "$AGENT" ] && [ -n "$CI" ] && [ -n "$GEN" ] || { echo "missing a principal (dq-agent=$AGENT ci-dev=$CI data-generator=$GEN)"; exit 1; }

# 2. Lakebase login role for dq-agent (default privileges only; never DATABRICKS_SUPERUSER)
if databricks postgres get-role "$BRANCH/roles/dq-agent" --profile "$PROFILE" >/dev/null 2>&1; then
  echo "Lakebase role dq-agent exists"
else
  databricks postgres create-role "$BRANCH" --role-id dq-agent --profile "$PROFILE" \
    --json "{\"spec\": {\"identity_type\": \"SERVICE_PRINCIPAL\", \"postgres_role\": \"$AGENT\", \"auth_method\": \"LAKEBASE_OAUTH_V1\"}}" >/dev/null
  echo "created Lakebase role dq-agent ($AGENT) on $BRANCH"
fi

# 3. group roles, memberships, privileges (one transaction), then the exact-match check
"$PY" tools/dq_roles.py --apply --agent-principal "$AGENT" --ci-principal "$CI" --owner-principal "$OWNER" --profile "$PROFILE"

# 4. Unity Catalog: the generator manifests schema (PR #36), then the grants CI cannot make
if databricks schemas get workspace.generator --profile "$PROFILE" >/dev/null 2>&1; then
  echo "schema workspace.generator exists"
else
  databricks schemas create generator workspace --profile "$PROFILE" \
    --comment "Generator defect manifests (DQ recommender eval ground truth)" >/dev/null
  echo "created schema workspace.generator"
fi
if databricks schemas get workspace.aidq_traces --profile "$PROFILE" >/dev/null 2>&1; then
  echo "schema workspace.aidq_traces exists"
else
  databricks schemas create aidq_traces workspace --profile "$PROFILE" \
    --comment "MLflow traces of the DQ recommender (plan #39 step 8)" >/dev/null
  echo "created schema workspace.aidq_traces"
fi
"$PY" tools/apply_grants.py --target dev --owner-applied --profile "$PROFILE" \
  --ci-principal "$CI" --owner-principal "$OWNER" --agent-principal "$AGENT" --data-generator-principal "$GEN"
"$PY" tools/apply_grants.py --target dev --owner-applied --check --profile "$PROFILE" \
  --ci-principal "$CI" --owner-principal "$OWNER" --agent-principal "$AGENT" --data-generator-principal "$GEN"

# 5. Service Principal User (who may deploy a job that runs as the service principal), then the exact-match check
"$PY" tools/sp_roles.py --apply --profile "$PROFILE"

# 6. trace store (experiment bound to workspace.aidq_traces, dq-agent CAN_EDIT), with its check
"$PY" tools/setup_dq_traces.py --profile "$PROFILE"

echo "done. dq-agent needs no OAuth secret: it runs as a Databricks job (run_as). Repository variable DQ_AGENT_SP=$AGENT"
