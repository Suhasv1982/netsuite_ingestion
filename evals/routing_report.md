# Routing comparison 2026-10-14: task `root_cause`

Baseline (routed primary) `databricks-gpt-oss-120b`; 12 paired runs per case on recorded inputs; judge `databricks-qwen35-122b-a10b` (fixed). Rules: src/aidq_llm/compare.py.

Baseline tokens: 1836075 in / 111972 out

## `databricks-gpt-oss-20b`: DO NOT ROUTE

Tokens: 1326313 in / 168927 out (baseline 1836075 / 111972)

- duplicate_versions check:rca_produced: candidate 10/12, target 12
- duplicate_versions check:rca_produced: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- duplicate_versions claim:claim_1: candidate 10/12, target 12
- duplicate_versions claim:claim_1: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- duplicate_versions claim:claim_2: candidate 10/12, target 12
- duplicate_versions claim:claim_2: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- duplicate_versions claim:claim_3: candidate 10/12, target 12
- duplicate_versions claim:claim_3: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- duplicate_versions identify:identify_1: candidate 6/12, target 8
- duplicate_versions identify:identify_1: worse than the baseline in 5 paired runs, better in 0 (sign test p=0.062)
- duplicate_versions identify:identify_2: candidate 3/12, target 8
- duplicate_versions identify:identify_2: worse than the baseline in 6 paired runs, better in 1 (sign test p=0.125)
- duplicate_versions identify:identify_3: candidate 5/12, target 8
- duplicate_versions identify:identify_3: worse than the baseline in 4 paired runs, better in 1 (sign test p=0.375)
- missed_runs check:category_allowed: candidate 9/12, target 12
- missed_runs check:category_allowed: worse than the baseline in 3 paired runs, better in 0 (sign test p=0.250)
- missed_runs check:rca_produced: candidate 10/12, target 12
- missed_runs check:rca_produced: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- missed_runs claim:claim_1: candidate 10/12, target 12
- missed_runs claim:claim_1: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- missed_runs claim:claim_2: candidate 10/12, target 12
- missed_runs claim:claim_2: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- missed_runs claim:claim_3: candidate 10/12, target 12
- missed_runs claim:claim_3: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- missed_runs claim:claim_4: candidate 10/12, target 12
- missed_runs claim:claim_4: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- missed_runs identify:identify_1: worse than the baseline in 2 paired runs, better in 0 (sign test p=0.500)
- missed_runs identify:identify_2: worse than the baseline in 3 paired runs, better in 0 (sign test p=0.250)
- missed_runs identify:identify_3: candidate 2/12, target 8
- missed_runs identify:identify_3: worse than the baseline in 10 paired runs, better in 0 (sign test p=0.002)
- missed_runs_metadata_unavailable check:category_allowed: candidate 11/12, target 12
- missed_runs_metadata_unavailable claim:claim_1: candidate 11/12, target 12
- missed_runs_metadata_unavailable claim:claim_3: candidate 11/12, target 12
- missed_runs_metadata_unavailable claim:claim_4: candidate 11/12, target 12
- missed_runs_metadata_unavailable identify:identify_2: worse than the baseline in 3 paired runs, better in 1 (sign test p=0.625)
- missed_runs_metadata_unavailable identify:identify_3: candidate 3/12, target 8
- missed_runs_metadata_unavailable identify:identify_3: worse than the baseline in 8 paired runs, better in 0 (sign test p=0.008)
- watchdog_self_healed_10_11 check:category_allowed: candidate 11/12, target 12
- watchdog_self_healed_10_11 claim:claim_3: candidate 11/12, target 12
- watchdog_self_healed_10_11 identify:identify_3: candidate 6/12, target 8
- watchdog_self_healed_10_11 identify:identify_3: worse than the baseline in 6 paired runs, better in 0 (sign test p=0.031)

| case | item | target | baseline | candidate | b | c | p |
|---|---|---|---|---|---|---|---|
| healthy_2026-10-09 | check:incident_as_expected | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| duplicate_versions | check:category_allowed | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| duplicate_versions | check:incident_as_expected | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| duplicate_versions | check:rca_produced | 12/12 | 12 | 10 | 2 | 0 | 0.500 |
| duplicate_versions | claim:claim_1 | 12/12 | 12 | 10 | 2 | 0 | 0.500 |
| duplicate_versions | claim:claim_2 | 12/12 | 12 | 10 | 2 | 0 | 0.500 |
| duplicate_versions | claim:claim_3 | 12/12 | 12 | 10 | 2 | 0 | 0.500 |
| duplicate_versions | identify:identify_1 | 8/12 | 11 | 6 | 5 | 0 | 0.062 |
| duplicate_versions | identify:identify_2 | 8/12 | 8 | 3 | 6 | 1 | 0.125 |
| duplicate_versions | identify:identify_3 | 8/12 | 8 | 5 | 4 | 1 | 0.375 |
| missed_runs | check:category_allowed | 12/12 | 12 | 9 | 3 | 0 | 0.250 |
| missed_runs | check:incident_as_expected | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| missed_runs | check:rca_produced | 12/12 | 12 | 10 | 2 | 0 | 0.500 |
| missed_runs | claim:claim_1 | 12/12 | 12 | 10 | 2 | 0 | 0.500 |
| missed_runs | claim:claim_2 | 12/12 | 12 | 10 | 2 | 0 | 0.500 |
| missed_runs | claim:claim_3 | 12/12 | 12 | 10 | 2 | 0 | 0.500 |
| missed_runs | claim:claim_4 | 12/12 | 12 | 10 | 2 | 0 | 0.500 |
| missed_runs | identify:identify_1 | 8/12 | 12 | 10 | 2 | 0 | 0.500 |
| missed_runs | identify:identify_2 | 8/12 | 12 | 9 | 3 | 0 | 0.250 |
| missed_runs | identify:identify_3 | 8/12 | 12 | 2 | 10 | 0 | 0.002 |
| missed_runs_metadata_unavailable | check:category_allowed | 12/12 | 12 | 11 | 1 | 0 | 1.000 |
| missed_runs_metadata_unavailable | check:incident_as_expected | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| missed_runs_metadata_unavailable | check:rca_produced | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| missed_runs_metadata_unavailable | check:report_kept | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| missed_runs_metadata_unavailable | check:run_marked_failed | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| missed_runs_metadata_unavailable | claim:claim_1 | 12/12 | 12 | 11 | 1 | 0 | 1.000 |
| missed_runs_metadata_unavailable | claim:claim_2 | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| missed_runs_metadata_unavailable | claim:claim_3 | 12/12 | 12 | 11 | 1 | 0 | 1.000 |
| missed_runs_metadata_unavailable | claim:claim_4 | 12/12 | 12 | 11 | 1 | 0 | 1.000 |
| missed_runs_metadata_unavailable | identify:identify_1 | 8/12 | 12 | 12 | 0 | 0 | 1.000 |
| missed_runs_metadata_unavailable | identify:identify_2 | 8/12 | 11 | 9 | 3 | 1 | 0.625 |
| missed_runs_metadata_unavailable | identify:identify_3 | 8/12 | 11 | 3 | 8 | 0 | 0.008 |
| watchdog_self_healed_10_11 | check:category_allowed | 12/12 | 12 | 11 | 1 | 0 | 1.000 |
| watchdog_self_healed_10_11 | check:generator_before_ingestion | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| watchdog_self_healed_10_11 | check:incident_as_expected | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| watchdog_self_healed_10_11 | check:labelled_self_healed | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| watchdog_self_healed_10_11 | check:one_incident | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| watchdog_self_healed_10_11 | check:outcome_in_report | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| watchdog_self_healed_10_11 | check:rca_produced | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| watchdog_self_healed_10_11 | claim:claim_1 | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| watchdog_self_healed_10_11 | claim:claim_2 | 12/12 | 12 | 12 | 0 | 0 | 1.000 |
| watchdog_self_healed_10_11 | claim:claim_3 | 12/12 | 12 | 11 | 1 | 0 | 1.000 |
| watchdog_self_healed_10_11 | identify:identify_1 | 8/12 | 12 | 11 | 1 | 0 | 1.000 |
| watchdog_self_healed_10_11 | identify:identify_2 | 8/12 | 11 | 10 | 2 | 1 | 1.000 |
| watchdog_self_healed_10_11 | identify:identify_3 | 8/12 | 12 | 6 | 6 | 0 | 0.031 |
