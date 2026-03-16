# Strict Acceptance Closure Addendum

Date: 2026-03-16
Repository: Trading Advisor 3000
Scope: closure of strict remarks for Phase 5, Phase 6, and Phase 8.

## Phase 5 (strict DoD)

Issue:
- `process_improvement_report.py` produced a rollup-only artifact without actionable outcomes.
- tests validated only file generation, not actionability.

Remediation:
- `scripts/process_improvement_report.py` now builds an actionable report payload:
  - signal snapshot with thresholds and status,
  - observed process patterns,
  - prioritized action items with trigger/action/owner/due.
- report supports markdown and JSON with explicit `action_items`.
- `tests/process/test_process_reports.py` now validates actionable content, not only artifact existence.

Evidence:
- `artifacts/process-improvement-report.md` now contains `## Action Items` with concrete items.
- `python -m pytest tests/process/test_process_reports.py -q` -> passed.

## Phase 6 (strict DoD)

Issue:
- skills corpus was not explicitly cold-by-default in `.cursorignore`.
- routing policy wording conflicted with narrow hot-context intent.

Remediation:
- `.cursorignore` now contains `.cursor/skills/**`.
- skills governance docs now explicitly enforce targeted skill loading by signal.
- `scripts/validate_skills.py` now fails if cold-context pattern for skills is missing.
- architecture governance tests now include explicit cold-by-default assertions.

Evidence:
- `python scripts/validate_skills.py` -> passed.
- `python -m pytest tests/architecture/test_governance_policies.py -q` -> passed.

## Phase 8 (strict acceptance evidence blocker)

Issue:
- GitHub checks could show infrastructure error when hosted runners are unavailable due billing/spending.

Remediation:
- `.github/workflows/ci.yml` now uses hosted CI opt-in guard:
  - repository variable `AI_SHELL_ENABLE_HOSTED_CI=1` enables lane execution.
  - default-off avoids false-red infrastructure failures when hosted runners are unavailable.
- lane model remains explicit (`loop-lane`, `pr-lane`, `nightly-lane`, `dashboard-refresh`).
- fallback local replay path documented in workflow/check/runbook docs.

Evidence:
- `python -m pytest tests/process/test_harness_contracts.py -q` -> passed.
- local lane replay succeeded:
  - `python scripts/run_loop_gate.py --skip-session-check --from-git --git-ref HEAD`
  - `python scripts/run_pr_gate.py --skip-session-check --from-git --git-ref HEAD`
  - `python scripts/run_nightly_gate.py --from-git --git-ref HEAD`
- dashboard/report refresh succeeded:
  - `python scripts/build_governance_dashboard.py --output-json artifacts/governance-dashboard.json --output-md artifacts/governance-dashboard.md`

## Consolidated validation run

- `python -m pytest tests/process tests/architecture tests/app -q` -> `55 passed`.

## Decision

Strict remarks for Phase 5, Phase 6, and Phase 8 are closed by code, tests, and executable evidence.
