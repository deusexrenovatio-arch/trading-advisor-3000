---
name: ta3000-technical-analysis-system-design
description: Use for TA3000 product-plane technical-analysis strategy design when indicators, chart patterns, trend, momentum, mean reversion, breakout, volatility, volume, market structure, or multi-timeframe logic must be translated into measurable and testable system rules.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-RESEARCH
scope: TA3000 technical-analysis signal taxonomy and system design
routing_triggers:
  - technical analysis
  - trend strategy
  - momentum strategy
  - mean reversion
  - breakout
  - volatility squeeze
  - volume confirmation
  - divergence
  - market structure
  - multi-timeframe
---

# TA3000 Technical Analysis System Design

## When To Use
- A trading idea is expressed through technical analysis, indicators, patterns, regimes, or chart behavior.
- Work needs to turn discretionary technical-analysis language into deterministic features, signals, and tests.
- A strategy mixes base indicators, derived relationships, confirmation filters, and exits.

Use with `ta3000-quant-compute-methodology` when choosing `pandas-ta-classic`, custom pandas, and `vectorbt` responsibilities.

## Knowledge Entry Points
Use these sources as methodology anchors when designing or revising technical-analysis logic:
- Systematic pattern recognition and empirical testing of technical analysis: `https://www.nber.org/papers/w7613`
- CMT body-of-knowledge orientation for technical analysis and risk discipline: `https://cmtassociation.org/cmt-program/`
- Pandas TA Classic indicator catalog: `https://xgboosted.github.io/pandas-ta-classic/indicators.html`
- Pandas TA Classic strategy system: `https://xgboosted.github.io/pandas-ta-classic/strategies.html`
- VectorBT signal and portfolio docs from `ta3000-quant-compute-methodology`

## Design Principle
Do not encode chart language directly. Translate it into measurable states:

- observation: what the chart-reader sees;
- measurement: which base/derived columns represent it;
- condition: exact boolean or scored rule;
- timing: when the condition becomes knowable;
- action: signal, alert, entry, exit, reduce, hold, or reject;
- invalidation: what proves the setup is no longer valid.

## Technical-Analysis Taxonomy
Classify each idea before implementation:

- Trend following: moving-average state, trend slope, higher-high/lower-low structure, MTF alignment.
- Momentum: ROC, RSI regimes, relative strength, cross-sectional rank, acceleration/deceleration.
- Mean reversion: distance to VWAP/MA/bands, range state, exhaustion, failed extension.
- Breakout/continuation: channel break, volatility expansion, range compression release, volume confirmation.
- Reversal: failed breakout, divergence, swing structure, exhaustion after extended move.
- Volatility: ATR, band width, squeeze/release, realized range, stop distance.
- Volume/liquidity: volume confirmation, abnormal volume, turnover proxy, liquidity no-trade filters.
- Session/market structure: opening range, session VWAP, prior high/low, gap, roll/session constraints.

## Layering Rules
- Base indicators belong in `research_indicator_frames` and should use `pandas-ta-classic` when available.
- Derived relationships belong in `research_derived_indicator_frames`: distance, slope, cross, divergence, session states, MTF overlays.
- Strategy-specific scores may be ephemeral in loader/backtest input when they are not durable research facts.
- A signal is not an indicator. Keep the final entry/exit matrix separate from base measurements.

## Multi-Timeframe Discipline
- Define primary signal timeframe first.
- Define execution/trigger timeframe separately.
- Align lower timeframe data to the latest closed higher timeframe state.
- Avoid forward-filling future higher-timeframe knowledge into earlier bars.
- In TA3000 launch work, prefer `1h` or `4h` as primary signal timeframes unless the decision is reopened.

## Done Criteria
- The TA concept is mapped to measurable columns and boolean/scored conditions.
- Base indicators, derived relationships, and final signals are not collapsed into one layer.
- Timing and closed-bar availability are explicit.
- Regime filters and no-trade filters are stated.
- The design names which evidence would reject the pattern.
