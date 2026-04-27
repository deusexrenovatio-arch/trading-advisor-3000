---
name: ta3000-backtest-validation-and-overfit-control
description: Use for TA3000 product-plane strategy testing, validation, robustness checks, walk-forward or out-of-sample evaluation, backtest overfitting control, leakage prevention, cost/slippage modeling, and strategy acceptance or rejection.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-RESEARCH
scope: TA3000 backtest validation, robustness, and overfitting control
routing_triggers:
  - backtest validation
  - overfit
  - overfitting
  - walk-forward
  - out-of-sample
  - robustness
  - slippage
  - transaction costs
  - lookahead
  - survivorship
  - strategy testing
---

# TA3000 Backtest Validation And Overfit Control

## When To Use
- A strategy backtest result is being interpreted, compared, accepted, rejected, or promoted.
- Work touches out-of-sample testing, walk-forward, parameter sweeps, robustness, cost assumptions, leakage, or validation reports.
- A result looks good but may be explained by overfitting, data leakage, costs, or fragile parameters.

Pair with `ta3000-strategy-research-methodology` so validation tests a stated hypothesis, not a post-hoc story.

## Knowledge Entry Points
Refresh methodology from durable sources when validation choices matter:
- Probability of Backtest Overfitting and CSCV: `https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253`
- Statistical overfitting and backtest performance: `https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2507040`
- Algorithm framework risk/execution separation: `https://www.quantconnect.com/docs/v2/writing-algorithms/algorithm-framework/overview`
- VectorBT portfolio and signal docs from `ta3000-quant-compute-methodology`

## Validation Mindset
Treat a good backtest as a candidate requiring disproof, not as proof.

Before claiming promise, check:
- hypothesis was stated before parameter search;
- train/test or walk-forward split is explicit;
- data availability is causal and closed-bar aligned;
- costs, slippage, commissions, and liquidity filters are included or explicitly out of scope;
- parameter sensitivity is stable rather than a single sharp optimum;
- results survive relevant regimes and instruments;
- rejected runs are preserved and explain why they failed.

## Bias Checklist
- Lookahead bias: no future bar, future session, future roll, or future higher-timeframe state leaks into signal time.
- Survivorship bias: universe membership and delistings/rolls are handled for the intended market.
- Selection bias: avoid choosing only the best instruments, periods, or configurations after seeing results.
- Multiple testing: record how many variants were tried; avoid presenting the winner as a single clean test.
- Data snooping: separate exploration from confirmation.
- Cost blindness: test turnover, spread/slippage assumptions, and trade count.
- Execution mismatch: signal bar, execution bar, and fill assumptions match the strategy horizon.

## Minimum Evidence Package
For a strategy candidate, collect:
- in-sample and out-of-sample metrics;
- drawdown, volatility, turnover, exposure, win/loss distribution, and trade count;
- per-instrument and per-regime breakdown;
- parameter stability or robustness grid;
- cost/slippage sensitivity;
- examples of trades and non-trades;
- rejected rows or failure codes for missing required data;
- written reason to promote, revise, or reject.

## Acceptance Labels
Use explicit labels instead of vague "works":
- `reject`: hypothesis failed or validation is invalid.
- `research-only`: interesting but not robust enough.
- `paper-signal`: safe to emit advisory/paper signals with monitoring.
- `paper-trade`: safe for simulated order routing.
- `live-candidate`: passed evidence threshold, still needs operations/risk approval.
- `live-enabled`: explicit operational decision, not implied by backtest success.

## Done Criteria
- The validation method is reproducible.
- OOS/walk-forward or a conscious alternative is named.
- Costs and execution assumptions are visible.
- Failure modes and rejected cases are recorded.
- No strategy is promoted to signal/live flow from aggregate performance alone.
