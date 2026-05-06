# Process Improvement Report

- generated_at: 2026-05-06
- completed_tasks_count: 44
- window_tasks_count: 20
- window_size: 20
- burn_in_complete: True

## Signal Snapshot

| Metric | Current | Target | Status |
| --- | --- | --- | --- |
| `correct_first_time_pct` | 0.15 | >= 0.80 | ACTION_REQUIRED |
| `start_match_pct` | 0.80 | >= 0.90 | ACTION_REQUIRED |
| `context_expansion_rate` | 0.20 | <= 0.20 | OK |
| `repeat_error_rate` | 0.00 | <= 0.00 | OK |
| `environment_blocker_rate` | 0.05 | <= 0.05 | OK |

## Observed Patterns

- decision_quality_counts: correct_after_replan=15, correct_first_time=3, environment_blocked=1, partial_outcome=1
- outcome_status_counts: blocked=1, completed=18, partial=1
- rework_cause_counts: environment=1, test_gap=2, workflow_gap=4
- improvement_action_counts: architecture=1, docs=1, env=1, pending=1, skill=1, test=4, validator=3, workflow=6
- repeat_incident_signatures: none
- same_path_retry_count: 0

## Action Items

| ID | Priority | Trigger | Action | Owner | Due |
| --- | --- | --- | --- | --- | --- |
| PROC-QUALITY-001 | P1 | correct_first_time_pct=0.15 (target >= 0.80) | Run a first-time-right retro for recent non-first-time tasks, then update first-time-right checklist before next PR closeout. | process-owner | before next PR gate |
| PROC-CLOSEOUT-002 | P1 | partial_or_blocked_count=2 | For each partial/blocked outcome, add explicit remediation note and follow-up task link to keep lifecycle evidence complete. | task-owner | in current session closeout |
