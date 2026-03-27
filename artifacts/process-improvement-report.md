# Process Improvement Report

- generated_at: 2026-03-26
- completed_tasks_count: 23
- window_tasks_count: 20
- window_size: 20
- burn_in_complete: True

## Signal Snapshot

| Metric | Current | Target | Status |
| --- | --- | --- | --- |
| `correct_first_time_pct` | 0.25 | >= 0.80 | ACTION_REQUIRED |
| `start_match_pct` | 0.95 | >= 0.90 | OK |
| `context_expansion_rate` | 0.05 | <= 0.20 | OK |
| `repeat_error_rate` | 0.00 | <= 0.00 | OK |
| `environment_blocker_rate` | 0.00 | <= 0.05 | OK |

## Observed Patterns

- decision_quality_counts: correct_after_replan=12, correct_first_time=5, partial_outcome=3
- outcome_status_counts: completed=20
- rework_cause_counts: architecture_gap=2, requirements_gap=4, test_gap=2, workflow_gap=5
- improvement_action_counts: architecture=1, docs=4, test=10, validator=1, workflow=4
- repeat_incident_signatures: none
- same_path_retry_count: 0

## Action Items

| ID | Priority | Trigger | Action | Owner | Due |
| --- | --- | --- | --- | --- | --- |
| PROC-QUALITY-001 | P1 | correct_first_time_pct=0.25 (target >= 0.80) | Run a first-time-right retro for recent non-first-time tasks, then update first-time-right checklist before next PR closeout. | process-owner | before next PR gate |
