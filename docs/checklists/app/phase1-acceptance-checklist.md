# Phase 1 Acceptance Checklist

Дата: 2026-03-16

## Acceptance Disposition (updated 2026-03-17)
- [x] Phase 1 accepted as MVP contract baseline
- [x] Phase 1 accepted as final/full contracts freeze

## Full DoD Resolution (closed 2026-03-17)
- `strategy_version_id` unification is accepted and stable across phase doc, signal contracts, and migration skeleton.
- `signal.publications` expanded to include `publication_id` and `publication_type` in contracts, schema snapshots, fixture payload, and migration skeleton.
- Contract tests cover traceability fields and reject `side="flat"` in public `signal_candidate.v1` surface.

## Deliverables
- [x] Contracts package добавлен (`src/trading_advisor_3000/app/contracts/*`)
- [x] Migration skeleton добавлен (`src/trading_advisor_3000/migrations/*`)
- [x] Fixture payloads добавлены (`tests/app/fixtures/contracts/*`)
- [x] Contract tests добавлены (`tests/app/contracts/test_phase1_contracts.py`)
- [x] Phase docs обновлены (`docs/architecture/app/phase1-contracts-and-scaffolding.md`)

## Acceptance Criteria
- [x] Contract tests green
- [x] Loop gate green
- [x] PR gate green
- [x] Docs updated

## Evidence Commands
- [x] `python -m pytest tests/app/contracts -q`
- [x] `python -m pytest tests/app -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- [x] `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
