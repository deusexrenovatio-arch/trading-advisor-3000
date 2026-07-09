# Continuous Front Market Structure State Spec

This document owns the product semantics for deterministic continuous-front
market-structure state. It defines what the state layer means, what it may read,
and which outputs downstream research and signal systems may rely on.

## Phase Contract

Input: existing continuous-front bar and indicator semantics.

Output: stable market-structure state vocabulary and source boundary.

Done when: downstream phases can treat state labels, input tables, threshold
inputs, and causality rules as frozen v1 contract.

## Implementation Criteria

Implementation is acceptable when:

- the allowed `trend_state_label` enum is represented in product-plane
  contracts and cannot silently accept unknown state labels;
- every closed input bar maps to exactly one output state row;
- ATR is read from `continuous_front_indicator_frames.delta`, not recomputed in
  the market-structure layer;
- `continuous_front_derived_indicator_frames.delta` and
  `research_derived_indicator_frames.delta` are not read by the v1 state path;
- existing swing or rolling extrema columns are treated only as diagnostics;
- all-time high and all-time low, if added later, are named as global context
  diagnostics and do not drive local `HH/LH/HL/LL` state.

## Purpose

The market-structure layer assigns exactly one trend state to every closed
continuous-front bar. It is a deterministic bar-level state machine, not an
indicator formula, not a derived-indicator relationship, and not a strategy
signal.

The stable dataflow is:

```text
continuous_front_bars
+ continuous_front_indicator_frames
-> continuous_front_market_structure_frames
```

The calculation must not depend on `continuous_front_derived_indicator_frames`
or `research_derived_indicator_frames`.

## Existing Data Position

Existing required inputs:

```text
continuous_front_bars.delta
continuous_front_indicator_frames.delta
```

`continuous_front_indicator_frames.delta` already owns `atr_14`, so ATR must be
read from the indicator layer.

Existing derived swing or rolling-high/rolling-low columns are not the
authoritative market-structure state. They may be used only as comparison
diagnostics, not as inputs or storage contract.

New required output:

```text
continuous_front_market_structure_frames.delta
```

## Global And Local Context

All-time high and all-time low are global context, not v1 state drivers. They
can remain diagnostics or later features, but they must not replace local
structure.

The v1 state table stores local context per bar:

```text
current_leg_high
current_leg_low
last_confirmed_local_high
prev_confirmed_local_high
last_confirmed_local_low
prev_confirmed_local_low
high_relation
low_relation
```

This prevents an old global maximum from suppressing later local uptrends that
remain below the all-time high.

## Source Inputs

Required input tables:

- `continuous_front_bars.delta` for continuous-front OHLC, bar identity, roll
  metadata, and active contract context.
- `continuous_front_indicator_frames.delta` for `atr_14` and indicator lineage.

Required configuration:

- `structure_set_version`
- `structure_model_version`
- `threshold_atr_multiple`
- `threshold_tick_multiple`
- `tick_size_by_instrument`

`atr_14` is the only base indicator required for v1 state computation.
Additional indicators such as ADX or CHOP may be used later for diagnostics,
but they are not part of the v1 state transition contract.

## State Enum

Allowed `trend_state_label` values:

```text
insufficient_history
range
uptrend_developing
uptrend_confirmed
uptrend_pullback
downtrend_developing
downtrend_confirmed
downtrend_pullback
compression
expansion_mixed
```

Every output row must have exactly one state. If required data is unavailable,
the row receives `insufficient_history`; it must not be dropped.

## Structure Terms

High relations:

```text
HH  last_confirmed_local_high > prev_confirmed_local_high + threshold
LH  last_confirmed_local_high < prev_confirmed_local_high - threshold
EQH otherwise
```

Low relations:

```text
HL  last_confirmed_local_low > prev_confirmed_local_low + threshold
LL  last_confirmed_local_low < prev_confirmed_local_low - threshold
EQL otherwise
```

Immediate breaks:

```text
break_up   close0 > last_confirmed_local_high + threshold
break_down close0 < last_confirmed_local_low - threshold
```

The state machine compares local confirmed structure levels only. All-time highs
and long-horizon record levels may be stored later as context, but they must not
drive local `HH/LH/HL/LL` state.

## Causality

The causal contract is:

```text
trend_state[t] = F(previous_state, closed_bar[t], levels_known_at_or_before_t)
```

Rules:

- `known_at_ts` equals `ts_close`.
- No future bar may influence the current row.
- No pivot model with right-side future bars is allowed in the v1 state path.
- Recomputing a prefix of the input must produce the same rows for that prefix
  as a full-history run.

## Boundary

This layer may output durable state fields such as `trend_state_label`,
`high_relation`, `low_relation`, `break_up`, and `break_down`.

This layer must not output:

- entry or exit signals;
- position intent;
- strategy ranking labels;
- portfolio or execution decisions.

## Ticket Handoff

Implementation tickets may reference this document as the source of truth for
semantic scope, allowed states, source inputs, and out-of-scope trading
decisions. Tickets must not duplicate or redefine the state enum.
