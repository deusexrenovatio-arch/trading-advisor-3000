# Continuous Front Acceptance Repair

Date: 2026-04-29
Change surface: product-plane
Source intent: local intake artifact named PLAN_continious_front; its acceptance points are reflected in this task note.

## Current Verdict

Status: repaired for the Spark-native target path, backward current-anchor adjustment semantics, point-in-time ladder application, and backtest execution semantics. The former Python/delta-rs continuous-front materializer has been removed from the active code surface. Real Spark/Delta proof now passes in the Docker/Linux Dagster runtime with the current worktree mounted.

The original acceptance block was valid: continuous-front bars were produced, but downstream research and backtests could collapse back into contract-native behavior. This repair keeps the continuous front as a first-class research series, preserves roll metadata, and separates signal prices from execution prices.

The remaining local Windows limitation is not an acceptance blocker: raw Windows Spark/Delta still needs a proper Hadoop native runtime (`hadoop.dll`/NativeIO), but the provisioned Docker/Linux Dagster image can run the real Spark contour against `D:/TA3000-data`.

## 2026-05-02 Follow-Up: Session-Bounded Calendar Front

Status: implemented for the default continuous-front refresh policy.

Decision:
- Continuous-front defaults now use `calendar_expiry_v1` with `calendar_roll_offset_trading_days=2`.
- Intraday bars are bounded to the Moscow regular research window `09:00 <= time < 23:50`.
- Expected timeline is `active_contract_bars`: it is built from bars that actually exist for the selected active contract, not from the union of timestamps across all contracts.

Reason:
- Current MOEX candle input has no usable open interest: raw history stores it as null and canonical bars carry zeroes.
- Union-timestamp timelines turn sparse off-session or inactive-contract candles into false `missing_active_bar_count` failures.
- Volume is useful for audit, but the production default is now deterministic date/session policy.

Proof:
- Focused Spark regression covers a sparse roll window with an 08:45 Moscow old-contract candle and validates that only active-contract session bars are emitted.
- Docker/Linux real Delta audit on `FUT_BR|15m|2025-09-20..2025-09-30` wrote `continuous_front_bars=352`, `roll_events=1`, `qc_report=1`, QC `PASS`, `missing_active_bar_count=0`.
- Host-side Delta audit confirmed `_delta_log` exists for all continuous-front tables and `bad_session_rows=0`; output bar times are `09:00..23:45` Moscow.
- Full Docker/Linux audit on target timeframes `15m,1h,4h,1d` wrote `continuous_front_bars=994820`, `roll_events=1253`, `qc_report=52`; all QC rows `PASS`, `missing_active_sum=0`, `missing_reference_sum=0`, `bad_intraday_session_rows=0`.

## Repaired Findings

### 1. Intraday Roll Rows Were Dropped Downstream

Status: fixed.

Repair:
- `load_continuous_front_as_research_context` now loads bar-level continuous-front rows with active contract metadata instead of collapsing the day to one session-level contract.
- `build_research_bar_views` now consumes the preserved active contract per bar.

Proof:
- Real verification slice: `continuous_front_bars=1272`, `research_bar_views=1272`.
- `missing_front_to_research=0`, `extra_research_to_front=0`.
- Indicator and derived rows also equal `1272`.

### 2. Policy Changes Could Reuse Stale Materialization

Status: fixed.

Repair:
- Materialization identity includes normalized `continuous_front_policy`, not only `series_mode`.
- Lock/manifest metadata keeps the full policy payload.

Proof:
- Focused tests prove different policy values produce different materialization identity.
- Real manifest includes `continuous_front_policy` with `confirmation_bars`, adjustment mode, switch timing, reference price policy, and related fields.

### 3. Scheduled Post-MOEX Chain Defaulted To Contract Mode

Status: fixed.

Repair:
- Scheduled post-MOEX research data-prep config defaults to `series_mode='continuous_front'`.
- Sensor path passes the continuous-front policy explicitly.

Proof:
- Dagster integration coverage confirms product jobs and the MOEX-success sensor expose the continuous-front path.

### 4. Spark Contour Was Only A Probe

Status: fixed and runtime-proven in Docker/Linux.

Repair:
- Spark job now reads Delta through Spark, builds active timeline with Spark windows, derives roll events and adjustment ladder in Spark, writes all continuous-front Delta outputs through Spark, stages/promotes output, and validates promoted contracts.
- The main contour has no Python continuous-front materializer to call: active timeline, roll events, ladder, bars, and QC are produced by the Spark-native batch contour.
- Unsupported policy variants fail closed.

Proof:
- Unit tests assert the Spark job main path uses `spark-native-window-batch`; the old Python builder/materializer symbols and report surface are absent.
- Unit test proves continuous-front bars stay native at every row, while the adjustment ladder carries the BRK -> BRM gap for downstream as-of computation.
- In-memory Spark SQL smoke on this host produced `bars=3`, `roll_events=1`, `ladder=1`, QC `PASS`, and active sequence `BRK2@MOEX, BRK2@MOEX, BRM2@MOEX`.
- Docker/Linux real Delta proof produced `continuous_front_bars=1200`, `continuous_front_roll_events=2`, `continuous_front_adjustment_ladder=2`, `continuous_front_qc_report=1`, QC `PASS`.
- Spark profile reports `delta_reader=spark`, `delta_writer=spark`, `causal_roll_engine=spark-native-window-batch`.
- Historical Spark integration tests still pass.

### 5. Roll Policy Allowed Contract Flip-Flop

Status: fixed for the observed acceptance case.

Repair:
- Candidate selection keeps monotonic maturity/no-rollback behavior and confirmed causal roll decisions.

Proof:
- Real BR slice produced stable sequence:
  - `BRK2@MOEX -> BRM2@MOEX`, decision `2022-05-04T07:30:00Z`, effective `2022-05-04T07:45:00Z`.
  - `BRM2@MOEX -> BRN2@MOEX`, decision `2022-05-31T10:00:00Z`, effective `2022-05-31T10:15:00Z`.
- No BRM2 -> BRK2 rollback appears in the repaired proof.

### 6. Missing Active Bar Silently Switched Contract

Status: fixed.

Repair:
- Silent substitution from missing active contract to candidate contract was removed.
- Missing current active bar increments QC and fails closed instead of emitting an unaudited PASS row.

Proof:
- Unit test covers BRK2 at `t0` and only BRM2 at `t1`: no silent roll event, QC becomes `BLOCKED`, and missing-active is reported.

### 7. Research Views Dropped Roll Metadata

Status: fixed.

Repair:
- `research_bar_views` now carries continuous identity and roll/price-basis fields:
  `series_id`, `series_mode`, `roll_epoch`, `roll_event_id`, `is_roll_bar`, `is_first_bar_after_roll`, `bars_since_roll`, `price_space`, native OHLC, continuous OHLC, execution OHLC, active/previous/candidate contracts, adjustment mode, additive offset, and ratio factor.

Proof:
- Real `research_bar_views.delta` contains the metadata fields.
- Focused dataset-layer tests verify metadata survives a same-session roll into research views.

### 8. Continuous Front Split Back Into Contract Series Before Backtest

Status: fixed.

Repair:
- Loader groups continuous-front mode by continuous `series_id`/instrument/timeframe rather than active `contract_id`.
- Active contract remains row metadata.

Proof:
- Real loader proof returns one series:
  - `contract_id=continuous-front`
  - `series_id=FUT_BR|15m|continuous_front`
  - rows `1272`
  - active contracts `BRK2@MOEX`, `BRM2@MOEX`, `BRN2@MOEX`
  - roll epochs `0, 1, 2`

### 9. Backtest Execution Used Adjusted Close As Execution Price

Status: fixed.

Repair:
- Loader now returns separate `signal_frame` and `execution_frame`.
- Signal frame keeps current active prices plus continuous/as-of indicators.
- Execution frame uses native active-contract OHLC for vectorbt execution/PnL.
- Backtest runs, stats, orders, and trades keep `series_id`, `series_mode`, price-space fields, active contract id, and roll metadata.

Proof:
- Real backtest proof:
  - one continuous series, `1272` rows
  - `trade_count=19`
  - `order_rows=38`
  - order execution price space is `native`
  - trade price spaces are `continuous_backward_current_anchor_additive -> native`
  - active contracts in orders include `BRK2@MOEX`, `BRM2@MOEX`, `BRN2@MOEX`

### 10. Adjustment Direction Was Inverted

Status: fixed in Spark-native code, indicator materialization, and contract fixtures.

Repair:
- Roll gap is now `new_reference_price - old_reference_price`.
- Current/new active segment remains native: its cumulative additive offset is `0`.
- `continuous_front_bars` no longer writes future roll gaps into prior rows.
- Ladder records before/after offsets for backward adjustment: before the roll includes the gap; after the roll excludes it.
- Indicator materialization applies ladder per current roll epoch: old history is adjusted only inside the current computation window, and already materialized earlier rows are not rewritten by later roll events.

Proof:
- Spark-native unit test uses BRK2 -> BRM2 with a 10-point gap and verifies:
  - BRM current bars: `continuous_close == native_close`.
  - BRK prior bars remain native in `continuous_front_bars`.
  - ladder: `cumulative_offset_before=10`, `cumulative_offset_after=0`.
- Indicator unit test verifies the same 10-point gap is applied as-of the roll epoch: the roll-bar SMA uses adjusted BRK history, while the pre-roll SMA remains native.

### 11. Static Adjusted Bars Used Future Roll Gaps

Status: fixed.

Repair:
- Removed the Spark window that summed future roll gaps into `continuous_front_bars`.
- `continuous_open/high/low/close` now equal native active-contract OHLC on each emitted front row.
- `cumulative_additive_offset` on front rows is `0`; the adjustment identity lives in `continuous_front_adjustment_ladder`.
- Rolled continuous-front indicator partitions now require ladder rows and fail closed if the ladder is missing.

Proof:
- Spark unit proof verifies continuous closes `100, 101, 112, 113`, native closes `100, 101, 112, 113`, offsets `0, 0, 0, 0`, and ladder before/after `10/0`.
- Indicator unit proof verifies roll-epoch as-of adjustment: SMA before the roll remains `100.5`; SMA on the first BRM bar uses BRK adjusted to BRM basis and equals `111.5`.

## Current Spark/Docker Real Proof Artifact

This root is evidence for the corrected point-in-time ladder path on real Delta rows. Continuous-front tables were written by the Spark contour inside Docker/Linux with the current worktree mounted; research, indicator, and derived-indicator tables were then materialized from that Spark output.

Verification root:
`D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/continuous-front-codex-proof-20260429-br-asof-spark-docker`

Dataset version:
`continuous_front_codex_proof_20260429_br_asof_spark_docker`

Rows:
- `continuous_front_bars=1200`
- `continuous_front_roll_events=2`
- `continuous_front_adjustment_ladder=2`
- `continuous_front_qc_report=1`, status `PASS`
- `research_datasets=1`
- `research_instrument_tree=1`
- `research_bar_views=1200`
- `research_indicator_frames=1200`
- `research_derived_indicator_frames=1200`

Adjustment proof:
- `price_mismatch_count=0`: continuous OHLC equals native OHLC in `continuous_front_bars`.
- `nonzero_front_offset_count=0`: front rows do not store future roll gaps.
- Roll 1: `BRK2@MOEX -> BRM2@MOEX`, decision `2022-05-04T07:30:00Z`, effective `2022-05-04T07:45:00Z`, additive gap `-1.85`, ladder before/after `-1.85/0`.
- Roll 2: `BRM2@MOEX -> BRN2@MOEX`, decision `2022-05-31T10:00:00Z`, effective `2022-05-31T10:15:00Z`, additive gap `-3.86`, ladder before/after `-3.86/0`.
- `missing_front_to_research=0`.
- All listed tables have `_delta_log`.

## Earlier Downstream/Backtest Proof Artifact

This earlier root remains evidence for loader/backtest continuity on real Delta rows. It predates the point-in-time ladder repair and is retained only as supporting backtest semantics proof, not as the current adjustment-direction proof.

Verification root:
`D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/continuous-front-codex-proof-20260429-br-roll-repair`

Dataset version:
`continuous_front_codex_proof_20260429_br_roll_repair`

Rows:
- `continuous_front_bars=1272`
- `continuous_front_roll_events=2`
- `continuous_front_adjustment_ladder=2`
- `continuous_front_qc_report=1`, status `PASS`
- `research_datasets=1`
- `research_instrument_tree=1`
- `research_bar_views=1272`
- `research_indicator_frames=1272`
- `research_derived_indicator_frames=1272`

Backtest proof root:
`D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification/continuous-front-codex-proof-20260429-br-roll-repair/backtest-proof-ma-cross-final`

Backtest rows:
- `research_backtest_batches=1`
- `research_backtest_runs=1`
- `research_strategy_stats=1`
- `research_trade_records=19`
- `research_order_records=38`
- `research_drawdown_records=6`

All listed tables have `_delta_log`.

## Validation Commands

Passed:
- `py -3.11 -m pytest tests/product-plane/contracts/test_continuous_front_contracts.py tests/product-plane/unit/test_continuous_front.py tests/product-plane/unit/test_research_dataset_layer.py tests/product-plane/unit/test_continuous_front_spark_job.py tests/product-plane/unit/test_research_indicator_layer.py tests/product-plane/unit/test_research_derived_indicator_layer.py tests/product-plane/unit/test_research_campaign_runner.py tests/product-plane/unit/test_research_dagster_manifests.py tests/product-plane/unit/test_research_backtest_layer.py tests/product-plane/unit/test_research_backtest_engine_semantics.py tests/product-plane/integration/test_research_vectorbt_backtests.py tests/product-plane/integration/test_research_dagster_jobs.py -q --basetemp=.tmp/pytest-cf-asof-broad-rerun`
  - `66 passed`
- `py -3.11 -m pytest tests/product-plane/integration/test_historical_data_spark_execution.py -q --basetemp=.tmp/pytest-cf-asof-historical-spark`
  - `2 passed`
- `py -3.11 -m pytest tests/product-plane/unit/test_research_indicator_layer.py -q --basetemp=.tmp/pytest-cf-asof-indicators`
  - `8 passed`
- `py -3.11 -m pytest tests/product-plane/unit/test_continuous_front_spark_job.py -q --basetemp=.tmp/pytest-cf-asof-spark`
  - `3 passed`
- `py -3.11 -m pytest tests/product-plane/integration/test_research_indicator_materialization.py -q --basetemp=.tmp/pytest-cf-asof-indicator-materialization`
  - `6 passed`
- Spark current-anchor unit proof for `_build_spark_native_tables`
  - active sequence `BRK2@MOEX, BRK2@MOEX, BRM2@MOEX, BRM2@MOEX`
  - continuous closes `100, 101, 112, 113`; native closes `100, 101, 112, 113`
  - offsets `0, 0, 0, 0`; ladder before/after `10/0`
- Indicator as-of ladder proof:
  - pre-roll SMA remains `100.5`
  - first BRM roll-bar SMA becomes `111.5` by applying the 10-point ladder gap to BRK history inside the current computation window

Passed after this document update:
- `py -3.11 scripts/validate_task_request_contract.py`
- `py -3.11 scripts/validate_session_handoff.py`
- `py -3.11 scripts/validate_solution_intent.py`
- `py -3.11 scripts/validate_critical_contour_closure.py`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `py -3.11 scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`

Passed in Docker/Linux against real Delta input:
- Spark continuous-front refresh over `FUT_BR`, `15m`, `2022-04-20T00:00:00Z` to `2022-06-10T23:59:59Z`
  - `continuous_front_bars=1200`, `roll_events=2`, `ladder=2`, QC `PASS`
  - Spark reader/writer: `spark`
  - causal roll engine: `spark-native-window-batch`
- Spark output verification:
  - `price_mismatch_count=0`
  - `nonzero_front_offset_count=0`
  - both roll bars remain native-priced
- Downstream materialization from that Spark output:
  - `research_bar_views=1200`
  - `research_indicator_frames=1200`
  - `research_derived_indicator_frames=1200`
  - `missing_front_to_research=0`

## Acceptance Checklist

- [x] Intraday roll rows are preserved through research, indicators, and derived indicators.
- [x] Full continuous-front policy is part of materialization identity and lock.
- [x] Scheduled post-MOEX path runs continuous-front mode by default.
- [x] Missing active bar cannot silently PASS.
- [x] Roll flip-flop and rollback are blocked for the observed real slice and covered by tests.
- [x] Roll events and adjustment ladder exist for real roll switches.
- [x] Research views expose roll metadata downstream.
- [x] Continuous-front backtest loader returns one continuous series per instrument/timeframe.
- [x] Backtests use separate signal and execution frames.
- [x] Execution/PnL uses native active-contract prices.
- [x] Spark contour does not collect the full canonical processing slice with `.collect()`.
- [x] Python continuous-front builder/materializer surface is deleted from the active research module.
- [x] Adjustment direction is backward current-anchor: current front remains native and prior history is adjusted backward only inside the as-of computation window.
- [x] `continuous_front_bars` does not materialize a final adjusted history line.
- [x] Rolled continuous-front indicators require the adjustment ladder.
- [x] Current Spark/Docker proof includes `_delta_log`, row counts, QC PASS, roll events, ladder rows, indicators, derived indicators, native front prices, and no stored future gap offsets.
- [x] Loop gate and PR gate pass.
- [x] Full Spark runtime proof on real Delta input passes in a provisioned Docker/Linux Spark runtime.
