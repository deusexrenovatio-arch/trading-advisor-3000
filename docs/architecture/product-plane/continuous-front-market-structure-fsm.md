# Continuous Front Market Structure FSM

This document defines the deterministic finite state machine for continuous
front trend state.

## Phase Contract

Input: Phase 1 state vocabulary and source boundary.

Output: deterministic bar-by-bar automaton, memory fields, transition order,
and reason codes.

Done when: the same ordered input rows always produce the same state sequence
with no subjective chart interpretation.

## Implementation Criteria

Implementation is acceptable when:

- the FSM has a small deterministic core that can be tested without Spark,
  Delta, or Dagster;
- the FSM input is an ordered per-series stream of closed bars with `high0`,
  `low0`, `close0`, `atr_14`, threshold config, and seed state;
- the FSM output contains state label/code, reason code, current leg, confirmed
  local levels, relation fields, break flags, and state age;
- the implementation never uses centered swing windows or right-side future
  bars;
- full rebuild and incremental seed behavior produce the same rows for the
  overlapping prefix;
- changing `structure_model_version` requires replay for the affected scope.

## Per-Series Memory

The FSM keeps state per:

```text
dataset_version + instrument_id + timeframe
```

Memory fields:

```text
previous_trend_state
state_age_bars
current_leg_direction
current_leg_high
current_leg_high_ts
current_leg_low
current_leg_low_ts
last_confirmed_local_high
last_confirmed_local_high_ts
prev_confirmed_local_high
prev_confirmed_local_high_ts
last_confirmed_local_low
last_confirmed_local_low_ts
prev_confirmed_local_low
prev_confirmed_local_low_ts
```

The FSM does not keep an unbounded extremum catalog in runtime memory. It keeps
only the current leg plus the last and previous confirmed local high/low. The
Delta output keeps one row per bar for audit, replay, and downstream joins.

For a full rebuild, compute each series from its first scoped bar. For an
incremental refresh, seed the FSM from the last accepted output row for the
same `dataset_version + instrument_id + timeframe + structure_set_version`.
Any change to source versions, price adjustment policy, threshold config, or
FSM version requires a full rebuild for the affected scope.

## Leg Confirmation

The FSM does not use centered swing windows and does not wait for future bars.
Confirmation is causal: only already closed bars up to the current row are
available.

In an up leg:

```text
current_leg_high = max(current_leg_high, high0)
```

Confirm the local high when:

```text
close0 <= current_leg_high - threshold_price
```

Then shift:

```text
prev_confirmed_local_high = last_confirmed_local_high
last_confirmed_local_high = current_leg_high
```

In a down leg:

```text
current_leg_low = min(current_leg_low, low0)
```

Confirm the local low when:

```text
close0 >= current_leg_low + threshold_price
```

Then shift:

```text
prev_confirmed_local_low = last_confirmed_local_low
last_confirmed_local_low = current_leg_low
```

## Relations

High relation:

```text
HH  last_high > prev_high + threshold
LH  last_high < prev_high - threshold
EQH otherwise
```

Low relation:

```text
HL  last_low > prev_low + threshold
LL  last_low < prev_low - threshold
EQL otherwise
```

If either previous level is unavailable, the relation is `none`.

## Breaks

Immediate structural breaks:

```text
break_up = close0 > last_confirmed_local_high + threshold_price
break_down = close0 < last_confirmed_local_low - threshold_price
```

Breaks are known on the current closed bar. They do not require future-bar
confirmation.

## State Precedence

Apply precedence exactly in this order:

```text
1. insufficient_history
2. opposite break against confirmed trend
3. HH + HL -> uptrend_confirmed
4. LH + LL -> downtrend_confirmed
5. break_up -> uptrend_developing
6. break_down -> downtrend_developing
7. previous uptrend + counter-leg without break_down -> uptrend_pullback
8. previous downtrend + counter-leg without break_up -> downtrend_pullback
9. LH + HL -> compression
10. HH + LL -> expansion_mixed
11. fallback -> range
```

After final state assignment:

- `state_changed_flag` is true if the final state differs from the previous row;
- `state_age_bars` resets to `1` on state change;
- `state_age_bars` increments by `1` when state does not change.

## Reason Codes

Every state assignment must include a deterministic `reason_code`.

Required reason code families:

```text
insufficient_history_missing_atr
insufficient_history_missing_local_structure
confirmed_hh_hl
confirmed_lh_ll
developing_break_up
developing_break_down
pullback_uptrend_no_break_down
pullback_downtrend_no_break_up
compression_lh_hl
expansion_hh_ll
fallback_range
opposite_break_from_uptrend
opposite_break_from_downtrend
```

## Correctness Checks

The FSM implementation must pass deterministic checks before publication:

- transition corpus: small hand-authored OHLC sequences that force HH, HL, LH,
  LL, compression, expansion, pullback, break-up, and break-down states;
- prefix invariance: computing rows `[0..N]` and computing rows `[0..N+K]`
  must produce identical state values for the shared prefix;
- causality: no transition may read rows later than the current closed bar;
- replay determinism: the same scoped input versions and config hash must
  produce identical `market_structure_row_hash` values;
- row parity: output frame rows must equal scoped `continuous_front_bars` rows.

## Roll Windows

Rows inside a cross-contract window are retained. The output must preserve
`cross_contract_window_any` so downstream consumers can filter or audit these
rows.

The FSM does not reset at a roll boundary in v1 unless a future
`structure_model_version` explicitly changes that behavior.

## Ticket Handoff

Implementation tickets may reference this document for the deterministic state
kernel and its tests. The first implementation ticket should complete the FSM
core and transition corpus before Spark or Dagster work starts.
