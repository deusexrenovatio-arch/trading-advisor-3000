# Continuous Signal Layer Technical Specification

## Purpose

The Continuous Signal Layer is the default research input for futures strategy signals.

It solves the roll-boundary problem that appears when indicators are calculated on short
contract-native series. Long rolling indicators such as MA, EMA, ATR, ADX, Bollinger Bands,
Donchian channels, volatility windows, and derived MTF relationships must see a continuous
instrument history. Execution and PnL must still use the raw active contract price.

This specification defines the target implementation for:

- continuous adjusted signal bars;
- raw active-contract execution alignment;
- indicator and derived-indicator materialization on the signal layer;
- loader changes for vectorbt input bundles;
- Dagster and Spark integration using the existing product-plane patterns.

Change surface: `product-plane`.

## Current State

The current research route is:

```text
canonical_bars + canonical_session_calendar + canonical_roll_map
  -> research_datasets / research_bar_views
  -> research_indicator_frames
  -> research_derived_indicator_frames
  -> research_backtest_batches
  -> ranking / projection
```

The existing code already has a `series_mode=continuous_front` concept, but it is not a
complete signal layer:

- `research_bar_views` can select the active contract through `canonical_roll_map`;
- it keeps the actual contract id on each row, so a continuous instrument series can still be
  split again by downstream grouping;
- it does not produce adjusted OHLC values;
- it does not explicitly separate signal prices from execution prices;
- the backtest loader still reads broad Delta tables and filters in Python.

Therefore, enabling `continuous_front` alone is not sufficient.

## Target Decision

Keep canonical contract bars as the source of truth, but stop using contract-native bars as
the default strategy signal input.

Target flow:

```text
Dagster -> Python raw ingest -> Spark canonical refresh
  -> canonical contract bars + session calendar + roll map
  -> Spark continuous signal layer
  -> indicators on adjusted signal bars
  -> derived indicators on adjusted signal bars and MTF signal views
  -> loader builds aligned signal/execution frames
  -> vectorbt simulates on raw active-contract execution prices
```

Canonical contract bars are retained for:

- rebuilds with a changed roll rule or adjustment method;
- audit and lineage;
- execution price reconstruction;
- roll-day and liquidity filters;
- contract-native research such as calendar spreads or expiration behavior.

## Data Model

### New Table: `research_signal_bar_views.delta`

This table is the durable Continuous Signal Layer. It has one row per:

```text
dataset_version, signal_layer_version, series_id, instrument_id, timeframe, ts
```

Required identity columns:

- `dataset_version`
- `signal_layer_version`
- `series_id`
- `instrument_id`
- `timeframe`
- `ts`

Required source and roll columns:

- `active_contract_id`
- `source_contract_id`
- `previous_active_contract_id`
- `session_date`
- `session_open_ts`
- `session_close_ts`
- `roll_rule_id`
- `roll_reason`
- `is_roll_bar`
- `roll_sequence`
- `roll_gap_raw`

Required price columns:

- `raw_open`
- `raw_high`
- `raw_low`
- `raw_close`
- `adjusted_open`
- `adjusted_high`
- `adjusted_low`
- `adjusted_close`
- `price_adjustment_factor`
- `price_adjustment_offset`
- `adjustment_method`

Required market columns:

- `volume`
- `open_interest`
- `raw_true_range`
- `adjusted_true_range`
- `adjusted_ret_1`
- `adjusted_log_ret_1`
- `bar_index`
- `slice_role`

Required lineage columns:

- `source_canonical_bars_hash`
- `source_roll_map_hash`
- `source_session_calendar_hash`
- `signal_layer_hash`
- `created_at`
- `code_version`

The logical series key must be stable across rolls. Do not use the changing futures
`contract_id` as the logical series identity for continuous rows.

Recommended `series_id` format:

```text
<instrument_id>:continuous_front:<adjustment_method>:<roll_rule_id>
```

Example:

```text
FUT_RGBI:continuous_front:forward_ratio_v1:max_oi_then_latest_close
```

### Optional Table: `research_signal_roll_events.delta`

This table records one row per detected roll event.

Required columns:

- `dataset_version`
- `signal_layer_version`
- `instrument_id`
- `timeframe`
- `roll_ts`
- `roll_session_date`
- `from_contract_id`
- `to_contract_id`
- `roll_rule_id`
- `roll_reason`
- `raw_old_close`
- `raw_new_open`
- `raw_new_close`
- `price_adjustment_factor_before`
- `price_adjustment_factor_after`
- `price_adjustment_offset_before`
- `price_adjustment_offset_after`
- `created_at`

This table is required if the implementation supports more than one adjustment method or if
roll diagnostics are shown in reports.

### No Separate Full Duplicate Of Canonical Contracts

The signal layer materializes only the active continuous view. It must not duplicate every
individual contract series. Full contract history remains in canonical storage.

## Adjustment Policy

Default adjustment method:

```text
forward_ratio_v1
```

Policy:

1. The signal layer is built in timestamp order per instrument and timeframe.
2. Before the first roll, adjusted OHLC equals raw OHLC.
3. At a roll boundary, calculate a new forward adjustment factor from information available
   by the closed roll bar.
4. Apply that factor to the new active contract from the roll bar onward.
5. Do not rewrite earlier history using future roll gaps.

The first roll bar is allowed for signal calculation only after the bar is closed.
Same-bar execution from a freshly computed roll adjustment is not allowed.

Required execution rule:

```text
roll_bar_trade_policy = next_bar_only
```

Future methods may include:

- `forward_additive_v1`
- `back_adjusted_ratio_v1`
- `back_adjusted_additive_v1`

Back-adjusted methods are not the default because they rewrite historical prices using future
roll events and are harder to reason about for point-in-time research.

## Spark Implementation

Implement the heavy signal-layer construction as a Spark job, following the existing native
pattern under `src/trading_advisor_3000/spark_jobs/`.

New module:

```text
src/trading_advisor_3000/spark_jobs/continuous_signal_bars_job.py
```

New CLI wrapper:

```text
scripts/run_research_continuous_signal_spark.py
```

The job must follow the existing Spark conventions:

- `SparkSession` uses UTC timezone;
- local and docker profiles are supported;
- `TA3000_SPARK_RUNTIME_ROOT` is honored for Ivy and Spark local dirs;
- output is written as Delta;
- output schema is validated after write;
- the wrapper normalizes host/container paths like the current canonicalization wrappers;
- the job writes a machine-readable execution report.

Suggested job spec:

```python
@dataclass(frozen=True)
class ContinuousSignalSparkJobSpec:
    app_name: str
    source_bars_table: str
    source_session_calendar_table: str
    source_roll_map_table: str
    target_signal_bars_table: str
    target_roll_events_table: str
```

Inputs:

- canonical bars Delta path;
- canonical session calendar Delta path;
- canonical roll map Delta path;
- dataset manifest payload;
- signal-layer policy payload;
- output directory.

Outputs:

- `research_signal_bar_views.delta`
- `research_signal_roll_events.delta`, if enabled;
- `continuous-signal-layer-report.json`.

Spark logic:

1. Read canonical bars with predicate pushdown:
   - selected instruments;
   - selected timeframes;
   - selected date range plus warmup.
2. Join each bar to the point-in-time active contract from `canonical_roll_map`.
3. Keep only bars where `contract_id == active_contract_id`.
4. Join session calendar for session metadata.
5. Sort by `instrument_id`, `timeframe`, `ts`.
6. Detect roll transitions.
7. Compute adjustment factors and adjusted OHLC.
8. Compute adjusted return and range fields.
9. Write Delta outputs with deterministic ordering and stable schema.

## Dagster Integration

Add `research_signal_bar_views` as a first-class research asset.

Target asset graph:

```text
canonical_bars_delta
canonical_session_calendar_delta
canonical_roll_map_delta
  -> research_datasets
  -> research_signal_bar_views
  -> research_indicator_frames
  -> research_derived_indicator_frames
  -> research_backtest_batches
```

The existing `research_bar_views` asset may remain for raw/audit compatibility during
migration, but indicators and backtests must not use it as the default signal source once
the Continuous Signal Layer is enabled.

Required Dagster changes:

- add `research_signal_bar_views` to `RESEARCH_DATA_PREP_ASSETS`;
- add it to `research_asset_specs()`;
- add it to `DATA_PREP_TABLES`;
- include its output path and row count in run summaries;
- preserve the current sensor handoff:

```text
moex_baseline_update_job SUCCESS -> research_data_prep_after_moex_sensor -> research_data_prep_job
```

Dagster config must expose:

- `signal_layer_enabled`
- `signal_layer_version`
- `adjustment_method`
- `roll_rule_id`
- `roll_bar_trade_policy`
- `signal_price_basis`
- `execution_price_basis`
- `materialize_roll_events`

## Campaign Contract Changes

Extend `research_campaign.v1.json` with a `signal_layer` block.

Required shape:

```yaml
signal_layer:
  enabled: true
  signal_layer_version: signal-layer-v1
  series_mode: continuous_front
  adjustment_method: forward_ratio_v1
  roll_rule_id: max_oi_then_latest_close
  roll_bar_trade_policy: next_bar_only
  signal_price_basis: adjusted_continuous
  execution_price_basis: raw_active_contract
  materialize_roll_events: true
```

Materialization identity must include the full `signal_layer` block. Changing any signal
policy must create a new materialization key or force recomputation.

## Indicator Materialization

Indicators must be calculated from `research_signal_bar_views.delta` when
`signal_layer.enabled=true`.

Price inputs:

- `open` maps to `adjusted_open`
- `high` maps to `adjusted_high`
- `low` maps to `adjusted_low`
- `close` maps to `adjusted_close`
- `true_range` maps to `adjusted_true_range`
- `volume` remains raw active-contract volume
- `open_interest` remains raw active-contract open interest

Indicator identity must use `series_id`, not active `contract_id`.

Required changes:

- add source table selection to indicator materialization;
- include `signal_layer_version` and `price_basis` in metadata;
- include `source_signal_layer_hash`;
- keep wide output columns unchanged unless an indicator itself changes;
- fail closed if a required adjusted OHLC column is missing.

## Derived Indicator Materialization

Derived indicators must use the same signal-layer series identity as indicators.

Required changes:

- MTF overlays must align continuous adjusted signal rows across timeframes;
- derived rows must include `signal_layer_version` and `price_basis`;
- source hashes must include both indicator hash and signal-layer hash;
- roll metadata columns must be available for derived no-trade or roll-day filters;
- derived logic must not merge rows by changing active contract id.

The derived layer remains the causal relationship layer. It must not collapse into final
strategy signals.

## Loader And Backtest Engine

The backtest loader must stop being a broad table reader with late Python filtering.

Introduce a resolver boundary:

```text
ResearchInputResolver
```

Responsibilities:

1. Read campaign/dataset/signal-layer manifests.
2. Resolve required timeframes from StrategyFamilySearchSpec.
3. Resolve required indicator and derived columns from strategy input contracts.
4. Read only selected instruments, timeframes, date windows, and required columns.
5. Build aligned signal and execution frames.
6. Fail closed on missing columns, duplicate keys, or signal/execution index mismatch.

The resolver output must separate:

```text
signal_frame:
  adjusted continuous OHLC + indicators + derived indicators

execution_frame:
  raw active-contract OHLC + active_contract_id + roll metadata
```

Vectorbt integration:

- strategy families read signal columns from `signal_frame`;
- entries and exits are generated from signal columns;
- portfolio prices use `execution_frame.raw_close` or another explicit execution price;
- roll-bar trades obey `roll_bar_trade_policy`;
- output rows retain `series_id`, `instrument_id`, `active_contract_id`, and `price_basis`.

Backtest requests must support:

- `series_mode=continuous_front`
- `signal_price_policy=adjusted_continuous`
- `execution_price_policy=raw_active_contract`
- `series_ids`
- `instrument_ids`
- `timeframes`
- required column projection.

## Quality Gates

Data quality checks:

- exactly one active contract row per `instrument_id`, `timeframe`, `ts`;
- no duplicate `dataset_version`, `signal_layer_version`, `series_id`, `timeframe`, `ts`;
- every signal row has a non-empty `active_contract_id`;
- every signal row has raw and adjusted OHLC;
- roll transitions have roll-event rows when roll-events are enabled;
- adjusted close continuity at roll boundaries stays within configured tolerance;
- no missing session calendar rows;
- no signal rows outside dataset range except declared warmup.

Methodology checks:

- MA/EMA/ATR/ADX windows do not reset at contract roll boundaries;
- indicator values before a future roll do not change when later data is appended;
- first roll-bar signal cannot execute on the same bar open;
- derived MTF alignment uses only the latest closed higher-timeframe bar;
- execution prices are raw active-contract prices, not adjusted prices.

Performance checks:

- loader must use Delta filters and column projection;
- one-instrument profile must avoid full-table reads;
- full universe data prep reports rows, durations, and refreshed/reused partition counts;
- backtest output must report signal-frame load time and execution-frame load time separately.

## Migration Plan

### Step 1: Contracts And Docs

- Add signal-layer campaign block.
- Add signal-layer Delta store contract.
- Add run-summary output paths and row counts.
- Add docs and runbook updates.

### Step 2: Spark Signal Layer

- Add Spark job module and CLI wrapper.
- Add fixture tests for roll adjustment.
- Add Delta output contract validation.

### Step 3: Dagster Asset Wiring

- Add `research_signal_bar_views` asset.
- Keep `research_bar_views` for compatibility.
- Update data-prep asset selection and sensor run config.

### Step 4: Indicator And Derived Retargeting

- Make signal bars the source for indicators when enabled.
- Make derived indicators inherit signal-layer identity.
- Add no-reset rolling-window tests across a roll boundary.

### Step 5: Loader And Vectorbt Input Resolver

- Add resolver with Delta filter pushdown and column projection.
- Return aligned `signal_frame` and `execution_frame`.
- Update vectorbt family search to use signal columns for entries/exits and raw execution prices for simulation.

### Step 6: Campaign Cutover

- Create a new approved-universe campaign config with signal layer enabled.
- Keep the old contract-mode config as audit/diagnostic only.
- Use a new dataset version for the cutover, for example:

```text
moex_approved_universe_signal_v1
```

## Acceptance Criteria

The implementation is accepted only when all of the following are true:

1. `research_data_prep_job` materializes:
   - `research_datasets`
   - `research_instrument_tree`
   - `research_signal_bar_views`
   - `research_indicator_frames`
   - `research_derived_indicator_frames`
2. The Dagster sensor still starts research data prep after canonical MOEX baseline success.
3. A fixture with a contract roll proves that MA/EMA values continue across the roll.
4. A fixture proves that execution PnL uses raw active-contract prices.
5. A fixture proves that same-bar roll execution is blocked under `next_bar_only`.
6. A real-data smoke run on one approved instrument reports:
   - signal row count;
   - roll event count;
   - indicator row count;
   - derived indicator row count;
   - loader timing;
   - vectorbt output row counts.
7. The loader no longer reads all research gold tables for one instrument.
8. Existing contract-mode research remains available for audit and special contract-native research.

## Non-Goals

- Do not delete canonical contract bars.
- Do not make adjusted prices the execution price.
- Do not move trading logic into shell surfaces.
- Do not make a second operator route outside `run_campaign` and Dagster research assets.
- Do not reintroduce the retired feature layer as the strategy input.

## Open Decisions

1. Default adjustment method after validation:
   - proposed: `forward_ratio_v1`;
   - alternative: `forward_additive_v1`.
2. Whether `research_signal_roll_events.delta` is mandatory in v1 or optional until diagnostics UI exists.
3. Whether raw active execution rows should stay only as raw columns inside `research_signal_bar_views` or be split into a separate `research_execution_bar_views.delta` table for very large backtest scans.
4. Whether contract-native strategy families should require an explicit opt-in flag to prevent accidental use.
