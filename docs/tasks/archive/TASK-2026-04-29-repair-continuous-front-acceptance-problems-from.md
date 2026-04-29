# Task Note
Updated: 2026-04-29 12:32 UTC

## Goal
- Deliver: repair the continuous-front acceptance blockers captured in `docs/tasks/active/continuous-front-acceptance-problems-2026-04-29.md`.

## Task Request Contract
- Objective: make continuous-front a first-class research/backtest series by preserving bar-level active-contract identity, roll metadata, policy identity, and native execution prices through materialization, loaders, and backtests.
- In Scope: Spark continuous-front contour, continuous-front contracts/loaders, research dataset views/materialization, research campaign identity, Dagster research config, backtest loader/engine/results contracts, focused tests, real Delta proof, and active task documentation.
- Out of Scope: direct production promotion to `research/gold/current`, unrelated strategy logic, 5m scope, and local installation of native Hadoop DLLs on this Windows host.
- Constraints: canonical MOEX truth stays under `D:/TA3000-data/trading-advisor-3000-nightly`; verification outputs stay under `research/gold/verification`; missing active bars must fail closed; policy changes must not reuse stale materializations; backtests must use continuous signal prices and native execution prices.
- Done Evidence: focused and broad pytest passes, real verification Delta root with `_delta_log` and matching row counts, real loader/backtest proof, validators, loop gate, and PR gate.
- Priority Rule: quality and acceptance semantics over fast closure; if Spark runtime cannot be proven locally, record the environment blocker instead of claiming acceptance.

## Current Delta
- Preserved bar-level continuous-front metadata into research context and `research_bar_views`.
- Removed silent missing-active substitution; missing active bars now block QC.
- Changed loader grouping so continuous-front mode returns one continuous series per instrument/timeframe.
- Split signal and execution frames; vectorbt execution/PnL now use native active-contract prices.
- Added series, price-basis, active-contract, and roll metadata to backtest run/stat/trade/order outputs.
- Kept continuous-front policy in materialization identity and kept scheduled post-MOEX research prep on continuous-front mode.
- Changed Spark contour to Spark-native window batch processing and deleted the Python builder/materializer surface.
- Fixed backward current-anchor adjustment and moved historical adjustment into ladder-driven as-of indicator computation.

## First-Time-Right Report
1. Confirmed coverage: the repair covers all P1/P0 acceptance blockers except local Spark runtime proof, which is now isolated as an environment blocker rather than hidden behind a passing claim.
2. Missing or risky scenarios: full Spark baseline proof still needs a provisioned Spark runtime with native Hadoop support or Linux/Docker execution.
3. Resource/time risks and chosen controls: removed the Python continuous-front materializer; unsupported Spark policy variants now fail closed.
4. Highest-priority fixes or follow-ups: run loop/PR gates after doc updates; run Spark proof in a provisioned runtime before claiming full Spark acceptance.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: continuous-front bar identity, downstream research metadata, backtest price basis, and Spark runtime proof boundary.
- Next Probe: run task validators, loop gate, and PR gate after final task-note update.

## Task Outcome
- Outcome Status: partial
- Decision Quality: environment_blocked
- Final Contexts: CTX-RESEARCH, CTX-OPS, CTX-CONTRACTS
- Route Match: expanded
- Primary Rework Cause: environment
- Incident Signature: continuous_front_downstream_contract_collapse_and_spark_runtime_gap
- Improvement Action: env
- Improvement Artifact: docs/tasks/active/continuous-front-acceptance-problems-2026-04-29.md

## Blockers
- Full local Spark runtime proof is blocked by Windows Hadoop NativeIO: Spark/Delta fails with `UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0` before processing real Delta input.

## Next Step
- Run full Spark runtime proof in a provisioned Spark runtime with native Hadoop support or Linux/Docker execution.

## Validation
- Passed: focused continuous-front/research/backtest pytest set, `26 passed`.
- Passed: research materialization/Dagster focused set, `32 passed`.
- Passed: broad product-plane continuous-front/research/backtest/Dagster set, `66 passed`.
- Passed after deleting the Python materializer route: broad product-plane continuous-front/research/backtest/Dagster set, `67 passed`.
- Passed after current-anchor repair: broad product-plane continuous-front/research/backtest/Dagster set, `64 passed`.
- Passed after point-in-time ladder repair: broad product-plane continuous-front/research/backtest/Dagster set, `66 passed`.
- Passed after point-in-time ladder repair: indicator materialization integration set, `6 passed`.
- Passed after current-anchor repair: unit no-materializer/loader and Spark-job tests, `6 passed`.
- Passed: historical Spark integration set, `2 passed`.
- Passed after deleting the Python materializer route: in-memory Spark SQL transform smoke, `bars=3`, `events=1`, `ladder=1`, QC `PASS`.
- Passed: real Delta proof at `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/continuous-front-codex-proof-20260429-br-roll-repair`.
- Passed: real loader/backtest proof at `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/continuous-front-codex-proof-20260429-br-roll-repair/backtest-proof-ma-cross-final`.
- Passed: task validators.
- Passed: loop gate.
- Passed: PR gate.
- Blocked: local Spark runtime proof on real Delta input, due missing native Hadoop Windows runtime.
