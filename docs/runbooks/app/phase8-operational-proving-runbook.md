# Phase 8 Operational Proving Runbook

## Purpose
Execute and validate the full production-like delivery loop for shell-controlled app development:
`loop lane -> pr lane -> nightly lane -> dashboard refresh -> durable evidence`.

## Preconditions
- Phase 8 process tests are green.
- Working tree state is understood (changed files are intentional).
- If hosted CI lanes are expected, repository variable `AI_SHELL_ENABLE_HOSTED_CI=1` is set.
- For local proving, Python environment includes required test/runtime dependencies.

## Standard Execution
1. Preview lane plan without side effects:
   - `python scripts/run_phase8_operational_proving.py --dry-run --from-git --git-ref HEAD`
2. Run full proving:
   - `python scripts/run_phase8_operational_proving.py --from-git --git-ref HEAD --output artifacts/phase8-operational-proving.json`
3. Review report:
   - `artifacts/phase8-operational-proving.json`
4. Confirm dashboard pack:
   - `artifacts/dev-loop-baseline.md`
   - `artifacts/harness-baseline-metrics.json`
   - `artifacts/process-improvement-report.md`
   - `artifacts/autonomy-kpi-report.md`
   - `artifacts/governance-dashboard.json`
   - `artifacts/governance-dashboard.md`

## Non-Happy-Path Validation
1. Lane failure behavior:
   - simulate failing gate command;
   - verify proving report status is `failed` and execution stops at first failing step.
2. Artifact integrity behavior:
   - run with dashboard refresh enabled and remove one artifact;
   - verify proving report fails at `artifact-validation`.
3. Scope drift behavior:
   - run with explicit `--changed-files ...`;
   - verify all gates receive identical resolved scope contract.

## Targeted Modes
- Skip nightly lane for fast lane-only smoke:
  - `python scripts/run_phase8_operational_proving.py --skip-nightly-lane --from-git --git-ref HEAD`
- Skip dashboard refresh for gate-only smoke:
  - `python scripts/run_phase8_operational_proving.py --skip-dashboard-refresh --from-git --git-ref HEAD`
- Enforce lifecycle session checks in loop/pr steps:
  - `python scripts/run_phase8_operational_proving.py --enforce-session-check --from-git --git-ref HEAD`

## Failure Triage
1. If `loop-gate` fails:
   - fix contract/session/docs validation first; do not continue to PR/nightly.
2. If `pr-gate` fails after green loop:
   - inspect deeper regression tests and surface-specific checks.
3. If `nightly-gate` fails:
   - resolve hygiene/telemetry/report validators before refreshing dashboard.
4. If `artifact-validation` fails:
   - rerun dashboard refresh and verify artifact paths, permissions, and non-empty outputs.

## Exit Criteria
- Proving report status is `ok`.
- No missing dashboard artifacts.
- Loop and PR gates pass on current diff.
- Evidence artifacts are available for handoff and PR acceptance narrative.
