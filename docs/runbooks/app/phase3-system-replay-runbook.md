# Phase 3 System Replay Runbook

## Purpose
Run and validate the integrated shadow-forward replay path:
`canonical bars -> research candidates -> runtime publication -> forward observations -> analytics outcomes`.

## Preconditions
- Python environment with project dependencies is active.
- Source bars fixture exists (for example `tests/app/fixtures/research/canonical_bars_sample.jsonl`).
- Strategy version used for replay is supported by sample strategy catalog (for MVP: `trend-follow-v1`).

## Replay Procedure
1. Run the integrated replay acceptance test:
   - `python -m pytest tests/app/integration/test_phase3_system_replay.py -q`
2. Run Phase 3 unit tests:
   - `python -m pytest tests/app/unit/test_phase3_forward_engine.py -q`
   - `python -m pytest tests/app/unit/test_phase3_analytics.py -q`
3. Run full app regression:
   - `python -m pytest tests/app -q`

## Expected Evidence
- Runtime replay report contains accepted/published signals (`runtime_report`).
- Output artifacts include:
  - `research.forward_observations.sample.jsonl`
  - `analytics.signal_outcomes.sample.jsonl`
- Delta manifests contain:
  - `research_forward_observations`
  - `analytics_signal_outcomes`

## Troubleshooting
- `unknown candidate_id in forward observation`:
  - Ensure forward observations are built from the same candidate list passed to analytics.
- `strategy_not_active` in replay report:
  - Verify strategy registration/activation in replay setup.
- Zero analytics outcomes:
  - Check that candidate generation produced non-zero `signal_contract_rows`.
  - Confirm bars include forward window after candidate decision timestamps.

## Recovery
- Re-run only the failing Phase 3 test first to isolate the failure.
- If contract drift is suspected, validate manifest assertions in `test_phase3_analytics.py`.
- Re-run `python scripts/run_loop_gate.py --from-git --git-ref HEAD` after fixes.
