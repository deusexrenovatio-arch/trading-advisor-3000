# Phase 8 Acceptance Checklist

Date: 2026-03-18

## Acceptance Disposition
- [x] Phase 8 operational proving package delivered
- [x] CI lane model and local operational proving flow are aligned
- [x] Non-happy-path behavior is validated by tests and runtime contract

## Deliverables
- [x] CI workflow has explicit lanes (`loop`, `pr`, `nightly`, `dashboard-refresh`)
- [x] Hosted CI opt-in guard is documented and tested
- [x] Consolidated proving entrypoint (`run_phase8_operational_proving.py`) added
- [x] Proving entrypoint emits machine-readable execution report
- [x] Dashboard/report artifact validation is fail-closed (missing and stale artifacts)
- [x] Phase 8 runbook added
- [x] Phase 8 process tests added for dry-run and failure paths

## Acceptance Criteria
- [x] Proving flow stops on first lane failure and reports failing step
- [x] Missing or stale dashboard artifacts fail proving even when commands are green
- [x] Dry-run mode provides deterministic lane plan without side effects
- [x] Full process/architecture/app regression remains green
- [x] Loop gate green
- [x] PR gate green

## Evidence Commands
- [x] `python -m pytest tests/process/test_phase8_operational_proving.py -q`
- [x] `python -m pytest tests/process/test_harness_contracts.py -q`
- [x] `python -m pytest tests/process tests/architecture tests/product-plane -q`
- [x] `python scripts/run_phase8_operational_proving.py --dry-run --changed-files docs/README.md`
- [x] `python scripts/run_phase8_operational_proving.py --from-git --git-ref HEAD --output artifacts/phase8-operational-proving.json`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- [x] `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`
