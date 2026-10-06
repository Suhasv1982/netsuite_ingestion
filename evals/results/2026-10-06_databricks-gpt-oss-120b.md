# Agent eval 2026-10-06

Agent model `databricks-gpt-oss-120b`, judge `databricks-qwen35-122b-a10b` (both served free on the workspace), 3 runs per case, recorded tool outputs. Targets (plan section 7): healthy day no incident every run; incidents every run on deterministic checks and on each must-not-claim item, at least 2/3 on each must-identify item. **Overall: FAIL**

## healthy_2026-10-05
cassette `2026-10-05_after_daily_run`, expect incident: False

- incident_as_expected: 3/3

## duplicate_versions
cassette `2026-10-05_pre_fix`, expect incident: True

- category_allowed: 3/3
- incident_as_expected: 3/3
- rca_produced: 2/3
- expected tools called: 3/3, 3/3, 3/3
- identify_1 (must identify): 2/3  the gap is confined to snapshot dates 2026-10-02 and 2026-10-03
- identify_2 (must identify): 0/3  same key and same updated_date appears twice in the source
- identify_3 (must identify): 0/3  the ledger / fingerprint keys on distinct (key, date) pairs, so the second version is skipped
- claim_1 (must not claim): 2/3  the pipeline or ledger_check failed
- claim_2 (must not claim): 2/3  rows were rejected by data quality rules
- claim_3 (must not claim): 2/3  the source rows are exact duplicates (most differ)
- errors: ['no submit_rca call; last text: ']

## missed_runs
cassette `2026-10-06_afternoon`, expect incident: True

- category_allowed: 3/3
- incident_as_expected: 3/3
- rca_produced: 3/3
- expected tools called: 2/2, 2/2, 2/2
- identify_1 (must identify): 3/3  no run was triggered (absence, not failure)
- identify_2 (must identify): 3/3  schedules were UNPAUSED and unchanged
- identify_3 (must identify): 3/3  no deploy or config change explains it, so it is outside the pipeline (platform)
- claim_1 (must not claim): 3/3  deploy-dev paused the schedules
- claim_2 (must not claim): 3/3  the job failed
- claim_3 (must not claim): 3/3  a specific confirmed platform cause (it is suspected only)
