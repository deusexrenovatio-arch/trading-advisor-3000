# Approved Universe v1 (Governed Promotion Contract)

## Decision Snapshot
- Version: `universe-v1`
- Effective date: `2026-04-10`
- Owner: Product-plane research governance
- Source canonical baseline root: `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`
- Scope: medium-term strategy evaluation and promotion evidence

## Evaluation Profile (v1)
- Allowed timeframes: `15m`, `1h`, `1d`
- Excluded timeframe from promotion profile: `5m`
- Minimum asset breadth: at least `3` instruments from this approved list per promotion run

## Approved Instruments (from pinned baseline)
- `FUT_BR`
- `FUT_GOLD`
- `FUT_MIX`
- `FUT_MXI`
- `FUT_NASD`
- `FUT_NG`
- `FUT_PLD`
- `FUT_PLT`
- `FUT_RGBI`
- `FUT_RTS`
- `FUT_SILV`
- `FUT_SPYF`
- `FUT_WHEAT`

## Inclusion and Exclusion Rules
- Include only instruments that exist in the pinned canonical baseline root with complete bars for all v1 timeframes (`15m`, `1h`, `1d`).
- Exclude instruments with broken roll continuity, missing canonical lineage, or failed QC gates in baseline artifacts.
- Exclude instruments that do not satisfy minimum history coverage for the current 4-year window.

## Baseline Coverage Contract
- Data source: `canonical_bars.delta` from `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current` only.
- Baseline coverage in current snapshot: `13` instruments, `2,567,246` rows for (`15m`, `1h`, `1d`) combined.
- Reproducibility rule: promotion evidence must pin (`baseline_id`, `run_id`, `universe_version`) in each result package.

## Promotion Criteria (v1)
- Historical window: full `4` years.
- No losing months in the evaluation window.
- Annualized return strictly above `30%`.
- Signal frequency at least `2` signals per week.
- Capital assumption: `1,000,000 RUB`.
- Risk controls: `Sharpe >= 1.0`, `max_drawdown <= 20%`, `gross_leverage <= 1.2`, `risk_per_position <= 1.5%`.
- Repeatability is mandatory: rerun with same versions/params must reproduce deterministic IDs and materially stable metrics.

## Governance and Update Policy
- Cadence: monthly review or ad-hoc only on explicit governance request.
- Change control: any add/remove operation requires a version bump (`universe-v2`, etc.) and documented reason.
- Compatibility rule: promotion comparisons are valid only within the same universe version.
- Audit rule: each promotion artifact must include a universe manifest reference and checksum/hash of the manifest content.
