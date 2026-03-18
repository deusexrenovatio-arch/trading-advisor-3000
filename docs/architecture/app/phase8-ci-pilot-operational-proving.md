# Phase 8 - CI, Pilot, And Operational Proving

## Goal
Close the final production-readiness loop for the delivery shell + app integration:
- prove lane parity between local runtime and hosted CI (`loop`, `pr`, `nightly`, `dashboard-refresh`),
- provide one deterministic operational proving entrypoint for pilot execution,
- enforce fail-closed behavior for non-happy lane outcomes and missing reporting artifacts,
- keep evidence reproducible after session handoff.

## Deliverables
- `.github/workflows/ci.yml` lane model with hosted-CI opt-in guard.
- `scripts/run_phase8_operational_proving.py` consolidated proving runner.
- `tests/process/test_phase8_operational_proving.py` fail-path and dry-run contract tests.
- `docs/runbooks/app/phase8-operational-proving-runbook.md`.
- `docs/checklists/app/phase8-acceptance-checklist.md`.

## Design Decisions
1. Phase 8 proving is executable, not narrative-only: one script orchestrates all lanes in fixed order and emits a machine-readable report.
2. Proving flow is fail-closed: execution stops at first failed lane step, and missing or stale dashboard/report artifacts are treated as a hard failure.
3. Scope is deterministic per run: all gate lanes receive one resolved scope contract (`changed-files`, `base/head`, or `from-git`) to avoid drift.
4. Hosted CI remains opt-in (`AI_SHELL_ENABLE_HOSTED_CI=1`), but local proving is mandatory and equivalent by lane semantics.
5. Dry-run mode is side-effect-free by default (no report write), while explicit evidence persistence stays opt-in.
6. Evidence remains durable through a single report artifact (`artifacts/phase8-operational-proving.json`) plus dashboard pack outputs.

## Acceptance Commands
- `python -m pytest tests/process/test_phase8_operational_proving.py -q`
- `python -m pytest tests/process/test_harness_contracts.py -q`
- `python -m pytest tests/process tests/architecture tests/app -q`
- `python scripts/run_phase8_operational_proving.py --dry-run --from-git --git-ref HEAD`
- `python scripts/run_phase8_operational_proving.py --from-git --git-ref HEAD --output artifacts/phase8-operational-proving.json`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## Out of Scope
- automatic remediation of failing lanes without operator review,
- forced hosted runner enablement for repositories without CI billing,
- application release orchestration beyond shell lane proving.
