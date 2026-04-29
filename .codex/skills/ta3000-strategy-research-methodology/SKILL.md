---
name: ta3000-strategy-research-methodology
description: Use for TA3000 product-plane strategy research when a trading idea must be turned into an explicit hypothesis, measurable strategy intent, research protocol, acceptance criteria, or rejection decision before technical-analysis design, compute implementation, or integration.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-RESEARCH
scope: TA3000 strategy hypothesis, intent, and research protocol ownership
routing_triggers:
  - strategy research
  - trading hypothesis
  - strategy intent
  - market regime
  - alpha idea
  - research protocol
  - strategy acceptance
  - reject strategy
---

# TA3000 Strategy Research Methodology

## When To Use
- The user proposes, modifies, compares, accepts, or rejects a trading strategy idea.
- A strategy needs clear intent, market regime assumptions, required data, evaluation questions, or acceptance criteria.
- Work risks jumping from an indicator idea straight to code without a research hypothesis.

Use this with `ta3000-technical-analysis-system-design` for technical-analysis ideas and with `ta3000-backtest-validation-and-overfit-control` before claiming a strategy is promising.

## Boundary With Neighbor Skills
- This skill owns strategy intent: hypothesis, regime, research protocol, acceptance criteria, and rejection logic.
- `ta3000-technical-analysis-system-design` owns the translation of chart/indicator language into measurable technical-analysis states.
- `ta3000-quant-compute-methodology` owns the library-native implementation path after the hypothesis and measurable states are clear.
- `ta3000-backtest-validation-and-overfit-control` owns evidence quality after a strategy is implemented or being promoted.

## Research Entry Points
Use external references to refresh methodology when the strategy concept is not already clear:
- Technical-analysis patterns as computational/statistical objects: `https://www.nber.org/papers/w7613`
- Algorithm framework separation of universe, alpha, portfolio, risk, and execution: `https://www.quantconnect.com/docs/v2/writing-algorithms/algorithm-framework/overview`
- Portfolio target construction from signals/insights: `https://www.quantconnect.com/docs/v1/algorithm-framework/portfolio-construction`
- Library-native compute route for TA3000: `ta3000-quant-compute-methodology`

## Hypothesis Before Code
Write the strategy as a falsifiable research note before implementing:

- `intent`: what market behavior the strategy tries to exploit.
- `regime`: trend, range, breakout, volatility expansion/contraction, rotation, event-like condition, or mixed.
- `instrument_universe`: why these instruments belong together.
- `clock_profile`: signal timeframe, execution timeframe, holding horizon, warmup, and rebalance cadence.
- `entry_logic`: measurable conditions that open a trade or emit a signal.
- `exit_logic`: invalidation, profit-taking, stop, time stop, regime exit, or explicit signal reversal.
- `risk_logic`: position size, concentration, max exposure, stop model, and no-trade states.
- `data_requirements`: base indicators, derived indicators, calendar/session fields, roll/open-interest fields, or external inputs.
- `verification_questions`: what evidence would make the strategy credible or reject it.
- `failure_modes`: lookahead risk, thin liquidity, unstable parameters, regime dependence, churn, and high cost sensitivity.

## Research Protocol
1. Read the current product-plane research docs and active strategy registry before assuming old semantics still apply.
2. State the hypothesis in trading terms, then translate it into measurable columns and signal states.
3. Decide whether the output is an advisory signal, paper-trade candidate, or execution-ready strategy. Do not silently jump to robot semantics.
4. Choose the compute path with `ta3000-quant-compute-methodology`: base indicators, derived relationships, signal matrix, and backtest.
5. Define the minimum evidence package before implementation: metrics, OOS split, robustness checks, rejected-case reporting, and examples.
6. Keep rejected strategies as explicit outcomes when evidence invalidates the hypothesis.

## TA3000 Strategy Metadata
Strategy specs should preserve research intent, not only required columns:
- intent;
- allowed clock profiles;
- market regimes;
- required base indicator columns;
- required derived indicator columns;
- entry logic;
- exit logic;
- verification questions.

Prefer capability-oriented strategy names that describe the behavior, not phase, debug, or experiment labels.

## Done Criteria
- The strategy has a falsifiable hypothesis and clear invalidation path.
- Required columns and timeframes follow from the hypothesis.
- The output mode is explicit: advisory signal, paper trade, semi-auto, or full-auto.
- Backtest validation requirements are named before positive performance is claimed.
- TA3000 storage and compute routes are extended rather than duplicated.
