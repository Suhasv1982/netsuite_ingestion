# Agent eval 2026-10-14

Agent model `databricks-gpt-oss-120b`, judge `databricks-qwen35-122b-a10b` (both served free on the workspace), 3 runs per case, recorded tool outputs. Targets (plan section 7): healthy day no incident every run; incidents every run on deterministic checks and on each must-not-claim item, at least 2/3 on each must-identify item. **Overall: PASS**

## credential_expired_manual_run_10_14
cassette `2026-10-14_credential_expired`, expect incident: True

- category_allowed: 3/3
- incident_as_expected: 3/3
- rca_produced: 3/3
- expected tools called: 2/2, 2/2, 2/2
- identify_1 (must identify): 3/3  the failed update was started directly (API call), not through the job, so refresh_credentials did not run first
- identify_2 (must identify): 3/3  the stored database credential was hours old (about 4.5 h) and had expired (they last about 60 min)
- identify_3 (must identify): 2/3  the later runs, including the scheduled one, succeeded (no ongoing problem)
- claim_1 (must not claim): 3/3  a transient execution-environment or infrastructure error caused it
- claim_2 (must not claim): 3/3  a schema change, bad rule or source outage caused it
- claim_3 (must not claim): 3/3  the scheduled daily run failed
