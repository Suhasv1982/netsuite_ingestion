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
Model databricks-gpt-oss-120b, 883/793 tokens

## Changes
- **netsuite_memberships:removed:membership_type** (incremental, relevance medium): DEACTIVATE_COLUMN proposal (dry run)
  The column `membership_type` was removed from the NetSuite source but remains defined in the platform metadata as TEXT. Removing this active column will alter the row hash for the incremental `netsuite_memberships` table, so after approval a bronze rebuild is required before the next run can succeed. Because the column is still present in metadata, the next pipeline execution would fail, making this change urgent. Approving will drop the column from future bronze rows once the rebuild completes.
- **netsuite_transaction_lines:type_changed:line_number** (incremental, relevance high): SCHEMA_TYPE_CHANGE incident (dry run)
  The `line_number` column in `netsuite_transaction_lines` currently has metadata type INTEGER but the source reports BIGINT, and the proposed change is to BIGINT. This type mismatch is flagged as an incident; approving the change could risk overflow or precision issues if downstream logic expects INTEGER, and it will also modify the row hash for this incremental table, requiring a bronze rebuild. The column likely identifies the line order in a transaction, so it is important for reporting and joins, giving it a high relevance. Review is needed to confirm the type alignment before proceeding.
- **netsuite_transactions:added:custcol_eval_channel** (incremental, relevance low): ADD_COLUMN proposal (dry run)
  A new custom column `custcol_eval_channel` of type TEXT is being added to the incremental `netsuite_transactions` table. The source profile shows the column is present in 35,508 rows but has a 100% null rate and zero distinct values, indicating it currently carries no data. Adding the column will change the row hash, so approval will require a bronze rebuild before the next run can succeed. Given the column is always null, its relevance for reporting or data quality is low.

## DDL on the branch

- `ALTER TABLE netsuite.netsuite_customers DROP COLUMN IF EXISTS created_date`
- `ALTER TABLE netsuite.netsuite_customers DROP COLUMN IF EXISTS updated_date`
- `ALTER TABLE netsuite.netsuite_transactions DROP COLUMN IF EXISTS custbody_region`
- `ALTER TABLE netsuite.netsuite_transactions ADD COLUMN IF NOT EXISTS custcol_eval_channel text`
- `ALTER TABLE netsuite.netsuite_memberships DROP COLUMN IF EXISTS membership_type`
- `ALTER TABLE netsuite.netsuite_transaction_lines ALTER COLUMN line_number TYPE bigint`
