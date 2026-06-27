---
name: ta3000-futures-money-math-and-execution-economics
description: Use for TA3000 product-plane futures money math and execution economics when work touches MOEX contract economics, tick/step value, margin estimates and buffers, fees, slippage, PnL, risk sizing, money ledger truth, vectorbt portfolio truth, execution_* research fields, or propagation of contract economics into research, backtest, ranking, and signal-candidate outputs.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-COMPUTE
scope: TA3000 futures contract economics, research execution economics, and simulated-vs-ledger money truth
routing_triggers:
  - futures money math
  - contract economics
  - canonical_contract_economics
  - execution economics
  - execution_ fields
  - margin buffer
  - tick value
  - step_price_rub
  - execution_step_price_rub
  - money ledger
  - vectorbt PnL
  - fees slippage PnL
  - risk sizing
---

# TA3000 Futures Money Math And Execution Economics

## When To Use
- A task touches futures contract value, tick/step value, margin, margin buffer, fees, slippage, PnL, risk sizing, or execution-money fields.
- A task asks which surface is authoritative: MOEX contract economics, research `execution_*` fields, vectorbt results, ranking/projection rows, broker fills, position snapshots, risk snapshots, or an explicit money ledger.
- Research, campaign, backtest, ranking, projection, or signal-candidate outputs need contract economics propagated or proven fail-closed.
- An agent might confuse contract economics with portfolio truth or confuse vectorbt simulated truth with paper/live ledger truth.

Do not use this for generic strategy validation alone. Pair with `ta3000-backtest-validation-and-overfit-control` when the question is whether costs/slippage make a strategy acceptable.

## Boundary With Neighbor Skills
- `ta3000-data-plane-proof` owns proof against authoritative `D:/TA3000-data` roots, published current Delta tables, `_delta_log`, row counts, report binding, and current-vs-verification claims.
- `ta3000-quant-compute-methodology` owns vectorbt, pandas-ta-classic, Optuna, Spark/Delta/Dagster runtime choices, and signal-matrix implementation method.
- `ta3000-signal-to-action-lifecycle` owns advisory, paper, semi-auto, live execution, broker connectivity, operator workflow, and live fail-closed gates.
- `ta3000-backtest-validation-and-overfit-control` owns robustness, OOS, walk-forward, overfit control, and promotion/rejection decisions after money assumptions are explicit.

## Truth Layers
Keep these layers separate:

1. Raw economics inputs:
   `raw_moex_contract_securities`, `raw_moex_indicative_fx_rates`, `raw_moex_rms_limits`, and `raw_moex_rms_staticparams`.
2. Canonical economics:
   `canonical_fx_rates`, `canonical_asset_risk_parameters`, and `canonical_contract_economics`.
3. Research propagation:
   research bar views and continuous-front bars carry `execution_*` fields joined from canonical economics; they are not the full contract economics source.
4. Research simulation truth:
   vectorbt portfolio outputs and persisted `research_vbt_param_results`, stats, trades, orders, drawdowns, rankings, and candidates describe simulated research results.
5. Ledger/execution truth:
   paper/live truth comes from order intents, broker orders, broker fills, position snapshots, risk snapshots, signal events, and any explicit money ledger table for that execution run.
6. Promotion/evaluation truth:
   `research_strategy_rankings`, `research_signal_candidates`, and `research_strategy_evaluation_profiles` are downstream decisions or projections, not raw execution ledgers.

## Formula Guardrails
Use the current product-plane formula chain unless the code contract changes:

- `tick_value_currency = min_step * lot_volume`
- `step_price_rub = tick_value_currency * fx_rate_to_rub`
- `margin_formula_base = last_settle_price * (step_price_rub / min_step) * mr1`
- `margin_radius_adjusted = margin_formula_base * (1 + radius_pct / 100)`
- `margin_required_no_buffer = max(official_initial_margin, margin_radius_adjusted)`
- `margin_buffer_pct` policy:
  - `0.30` for far contracts: `maturity_rank >= 3` or `days_to_expiry > 120`;
  - `0.05` for non-RUB or FX/USD-linked assets;
  - `0.01` otherwise.
- `margin_required_estimate = margin_required_no_buffer * (1 + margin_buffer_pct)`

When explaining or changing formulas, include `model_version`, `buffer_policy_version`, source flags, and the source tables. Do not silently replace official margin, RMS margin, FX, or buffer policy with a hand-written shortcut.

## Fail-Closed Rules
- Fail closed on missing or non-positive `MINSTEP`, `LOTVOLUME`, FX rate, `MR1`, or `LASTSETTLEPRICE`.
- If `execution_economics_required=True`, require `canonical_contract_economics_path` and verify it is a Delta table with `_delta_log`.
- Do not convert missing economics to zero cost, zero margin, or zero slippage.
- Do not treat default `fees_bps=0.0` or `slippage_bps=0.0` as accepted economics unless the campaign/config explicitly owns that assumption.
- Do not refresh money-math side tables by overwriting historical bar tables.

## Research Propagation
- Join canonical economics by contract and effective interval; use the latest as-of row and avoid duplicate bar rows.
- For weekly bars, use the bar end timestamp when the implementation exposes `bar_end_ts`.
- Research and continuous-front layers propagate fields such as:
  `execution_step_price_rub`, `execution_lot_volume`, `execution_tick_value_currency`, `execution_margin_required_estimate`, `execution_margin_buffer_pct`, `economics_effective_from_ts`, and `economics_model_version`.
- For continuous-front logic, keep signal price space separate from execution price space: indicators/signals may use adjusted continuous data, while execution and PnL must use raw active-contract prices.
- Backtest result tables may contain `execution_assumptions_json`, `fees_paid`, `slippage_paid`, `net_pnl`, `execution_price_space`, `commission`, `slippage`, `estimated_commission`, and `estimated_slippage`; interpret each in its owning table, not as a universal ledger.

## VectorBT Versus Ledger Truth
- vectorbt is authoritative for research simulation results for the run and inputs it executed.
- vectorbt output is not paper/live cash truth. Paper/live truth requires broker fills, position snapshots, risk snapshots, and ledger entries when present.
- Optimizer study/trial tables explain how parameters were searched; vectorbt result tables explain what was simulated; execution ledger/fills explain what actually happened.
- Risk sizing must be expressed in contracts/quantity against contract economics, margin estimate, step price, configured risk limits, and execution mode. Do not size from raw price movement alone.

## Evidence Checklist
- Name the source table/path for contract economics and whether it is current, verification, fixture, or test output.
- State the formula inputs actually present: `min_step`, `lot_volume`, `fx_rate_to_rub`, `official_initial_margin`, `last_settle_price`, `mr1`, `radius_pct`, and buffer policy.
- Prove missing-economics behavior with a fail-closed test or runtime error when relevant.
- For research propagation, prove the expected `execution_*` columns exist and are populated for the intended instruments/timeframes.
- For backtest/ranking, name whether the claim comes from vectorbt outputs, ranking rows, projection candidates, evaluation profiles, or a ledger/fill surface.
