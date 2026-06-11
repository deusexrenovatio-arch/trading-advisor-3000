# Strategy Evaluation Layer v1

## Purpose
`Strategy Evaluation Layer v1` is the post-backtest evaluation layer for
strategy research outputs. It keeps ranking, projection evidence, and promotion
readiness in one product concept instead of presenting them as separate
business processes.

The persisted output is `strategy_evaluation_profile.v1`.

`policy_pass` means a row passed the campaign research ranking policy, currently
`research_screen_strict_v1` by default. It is not paper-trade readiness, live
readiness, or final proof of strategy quality.

## Decision Ladder

| Verdict | Meaning |
| --- | --- |
| `reject` | The research hypothesis or ranking validation failed. |
| `research-only` | The result is interesting but lacks candidate or promotion evidence. |
| `paper-signal` | Advisory or paper signal publication is allowed. |
| `paper-trade` | Paper execution is allowed because sizing, risk, exposure, leverage, and no-trade state evidence exist. |
| `live-candidate` | Strict promotion gates passed; this is still not `live-enabled`. |

Live execution is outside this contract. A strategy can reach only
`live-candidate` here.

## Inputs And Output

Inputs:
- campaign config and strategy space identity;
- ranking rows from vectorbt/Optuna result tables;
- trades, orders, drawdowns, and run metadata when available;
- projected candidates from `research_signal_candidates`;
- scorecard/promotion evidence when available;
- approved universe profile.

Output:
- `strategy_evaluation_profile.v1`;
- persisted `research_strategy_evaluation_profiles.delta`;
- `verdict`, `promotion_state`, blocker reasons, missing-data gaps, and evidence snapshot.

The evaluator can run with partial evidence after ranking and with enriched
evidence after projection. That is still one evaluation layer; the later run
only has more inputs available.

## Terminology

`policy_pass` means the row passed research ranking policy.

`qualifies_for_projection` means the row may be used for candidate projection.

`paper_signal_ready` means a projected candidate exists with signal levels,
cost estimates, and no projection blockers.

`paper_trade_ready` requires paper-signal readiness plus capital model, risk per
position, exposure model, leverage, and no-trade states.

`live-candidate` requires the governed promotion profile:
- four-year evaluation window;
- at least three approved-universe instruments;
- no losing months;
- annual return above 30%;
- Sharpe at least 1.0;
- max drawdown at most 20%;
- gross leverage at most 1.2;
- risk per position at most 1.5%;
- repeatability evidence.

## Overfit Control

`research_screen_strict_v1` remains a research filter, not final promotion
evidence. Older campaigns may still carry `robust_oos_v1` as a configured
policy id, but the stricter default is the current route.

Promotion must preserve:
- exploration and confirmation separation;
- walk-forward or OOS evidence;
- trade count and positive fold ratio;
- parameter-neighborhood stability;
- slippage stress;
- explicit trial and parameter-hash provenance.

For `validation.scheme=nested_walk_forward_v1`, the evaluator records two
independent fields:
- `walk_forward_reoptimization_pass`;
- `latest_frozen_param_confirmation_pass`.

The first field is based on optimizer-visible folds. The second field is based
only on vectorbt confirmation rows for the frozen selected `param_hash`.
If optimization passes but blind confirmation fails, the row remains
`research-only` or `reject`; it must not become `paper-signal`, `paper-trade`,
or `live-candidate`. Legacy validation can still rank and project through the
old route, but it cannot claim a strict blind-confirmation verdict.

PBO/CSCV and Deflated Sharpe Ratio are target methods for stricter future
levels, not mandatory for the first v1 implementation.

## Data Gap Map

Already available in the current route:
- fold count and positive fold ratio;
- trade count;
- drawdown;
- slippage sensitivity;
- parameter stability;
- score and ranking policy metadata;
- projected candidates.

Needed for stricter promotion:
- monthly return series;
- signal frequency history;
- leverage;
- risk per position;
- repeatability fingerprint;
- multi-instrument breadth over the approved universe.

## Forbidden Shortcut

No strategy may be promoted from one aggregate backtest result. Promotion must
use this strategy evaluation profile and carry blocker reasons when evidence is missing.
