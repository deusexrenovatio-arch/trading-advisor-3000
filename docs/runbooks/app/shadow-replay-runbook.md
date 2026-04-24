# Shadow Replay System Replay Runbook

## Purpose
Run and validate the integrated shadow-forward replay path:
`canonical bars -> research candidates -> runtime publication -> forward observations -> analytics outcomes`.

## Preconditions
- Python environment with project dependencies is active.
- Source bars fixture exists (for example `tests/product-plane/fixtures/research/canonical_bars_sample.jsonl`).
- Strategy version used for replay is supported by the reference strategy set (for MVP: `trend-follow-v1`).

## Replay Procedure
1. Run the integrated replay acceptance test:
   - `python -m pytest tests/product-plane/integration/test_shadow_replay_system.py -q`
2. Run Shadow Replay unit tests:
   - `python -m pytest tests/product-plane/unit/test_shadow_forward_engine.py -q`
   - `python -m pytest tests/product-plane/unit/test_shadow_replay_analytics.py -q`
3. Run full app regression:
   - `python -m pytest tests/product-plane -q`

## Expected Evidence
- Runtime replay report contains accepted/published signals (`runtime_report`).
- Forward and analytics rows are produced only for runtime-accepted/published signal IDs.
- `candidate_id` in `research.forward_observations` matches `research.signal_candidates` for the same signal.
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
- Re-run only the failing Shadow Replay test first to isolate the failure.
- If contract drift is suspected, validate manifest assertions in `test_shadow_replay_analytics.py`.
- Re-run `python scripts/run_loop_gate.py --from-git --git-ref HEAD` after fixes.
