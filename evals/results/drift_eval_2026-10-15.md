# Schema drift eval, dev, 2026-10-15

Dev branch `drift-eval` of netsuite-sample (never the shared production source). Real agent, dry run, model included.

| Check | Result |
|---|---|
| clean: no change detected | PASS |
| clean: no model call | PASS |
| clean: nothing written | PASS |
| injected: exactly the injected changes detected | PASS |
| injected: netsuite_transactions:added:custcol_eval_channel -> ADD_COLUMN proposal | PASS |
| injected: netsuite_transactions:added:custcol_eval_channel validated, requires_rebuild=True | PASS |
| injected: netsuite_memberships:removed:membership_type -> DEACTIVATE_COLUMN proposal | PASS |
| injected: netsuite_memberships:removed:membership_type validated, requires_rebuild=True | PASS |
| injected: netsuite_transaction_lines:type_changed:line_number -> SCHEMA_TYPE_CHANGE incident | PASS |
| injected: the removed column's proposal carries get_impact evidence | PASS |
| injected: rebuild sentence in every incremental proposal rationale | PASS |

**Overall: PASS**

## Clean phase report

# aidq schema drift, dev, 2026-10-15

Prompt drift_rationale_v1; trace eval
Dry run: nothing written.

**No drift:** the source matches the metadata. No model call.

## Injected phase report

# aidq schema drift, dev, 2026-10-15

Prompt drift_rationale_v1; trace eval
Dry run: nothing written.
Model databricks-gpt-oss-120b, 883/749 tokens

## Changes
- **netsuite_memberships:removed:membership_type** (incremental, relevance medium): DEACTIVATE_COLUMN proposal (dry run)
  The column membership_type is present in the metadata but no longer exists in the NetSuite source table. The column name suggests it stores the type of a membership (e.g., "Standard", "Premium"), but we have no profile information on null rate or distinct values, so its usage is unknown. Approving this removal will delete the column from the platform metadata; because the table is incremental, the next run will fail closed until a bronze rebuild is performed. This is urgent because the orphaned column would otherwise cause a run failure.
- **netsuite_transaction_lines:type_changed:line_number** (incremental, relevance high): SCHEMA_TYPE_CHANGE incident (dry run)
  The column line_number currently has source type bigint while the platform metadata records it as INTEGER; the proposed change is to align the metadata to BIGINT. line_number identifies the position of a line within a transaction, so it is critical for accurate line‑level reporting. Approving this type change is an incident that must be reviewed because a mismatch could lead to truncation or overflow errors when the data is written, especially on an incremental table where the hash depends on the column value. The table is incremental, so after approval a bronze rebuild will be required before the next successful run.
- **netsuite_transactions:added:custcol_eval_channel** (incremental, relevance low): ADD_COLUMN proposal (dry run)
  A new custom column custcol_eval_channel of type TEXT is being added to the transactions table. The profile shows 35,508 rows, a null rate of 1.0 and 0 distinct values, meaning the column is currently empty for all rows; its purpose is unknown from the name alone. Approving the addition will introduce the column into the metadata and mark it as active; because the table is incremental, the next run will fail closed until a bronze rebuild is performed. The column is unlikely to affect reporting at present.

## DDL on the branch

- `ALTER TABLE netsuite.netsuite_customers DROP COLUMN IF EXISTS created_date`
- `ALTER TABLE netsuite.netsuite_customers DROP COLUMN IF EXISTS updated_date`
- `ALTER TABLE netsuite.netsuite_transactions DROP COLUMN IF EXISTS custbody_region`
- `ALTER TABLE netsuite.netsuite_transactions DROP COLUMN IF EXISTS custcol_eval_channel`
- `ALTER TABLE netsuite.netsuite_memberships ADD COLUMN IF NOT EXISTS membership_type text`
- `ALTER TABLE netsuite.netsuite_transaction_lines ALTER COLUMN line_number TYPE integer`
- `ALTER TABLE netsuite.netsuite_transactions ADD COLUMN IF NOT EXISTS custcol_eval_channel text`
- `ALTER TABLE netsuite.netsuite_memberships DROP COLUMN IF EXISTS membership_type`
- `ALTER TABLE netsuite.netsuite_transaction_lines ALTER COLUMN line_number TYPE bigint`
