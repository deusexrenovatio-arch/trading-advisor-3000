"""Append proof and atomic last-row checkpoint for the existing CF producer."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from .native_checkpoint import BUFFER, CheckpointUnavailable, NativeCheckpoint, identity


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def compute(
    frame,
    profile,
    ladder,
    cached_rows,
    prefix_length,
    diagnostics,
    state_out,
    bootstrap_reason=None,
):
    from . import materialize as m

    if not frame["roll_epoch"].is_monotonic_increasing:
        diagnostics["cf_tail_fallback_reason"] = "nonmonotonic_roll_epoch"
        diagnostics["cf_full_replacement_required"] = True
        state_out.clear()
        return m._compute_profile_frame_for_series(
            frame, profile, series_mode="continuous_front", adjustment_ladder_rows=ladder
        )

    context = _digest(
        dict(
            profile=asdict(profile),
            ladder=ladder,
            native=identity(),
            adapter={
                name: hashlib.sha256(
                    Path(__file__).with_name(name + ".py").read_bytes()
                ).hexdigest()
                for name in ("cf_tail", "native_checkpoint")
            },
        )
    )
    previous = {}
    reason = bootstrap_reason or "missing_checkpoint"
    if cached_rows and cached_rows[-1].compute_state_json:
        try:
            previous = json.loads(cached_rows[-1].compute_state_json)
            checksum = previous.pop("checksum")
            if _digest(previous) != checksum:
                raise ValueError("checkpoint checksum differs")
            if previous.get("context") != context or previous.get("format") != "cf-tail.v1":
                raise ValueError("checkpoint context differs")
            if not isinstance(previous.get("states"), dict):
                raise ValueError("malformed checkpoint states")
            if (
                previous.get("count") != len(cached_rows)
                or previous.get("last_ts") != cached_rows[-1].ts
                or prefix_length != len(cached_rows)
                or len(frame) < len(cached_rows)
                or previous.get("epoch") != int(frame["roll_epoch"].iloc[-1])
            ):
                raise ValueError("source is not an unchanged append")
            reason = ""
        except (ValueError, KeyError, TypeError, AttributeError):
            previous = {}
            reason = "invalid_checkpoint_or_changed_history"
    cutoff = len(cached_rows) if previous else None
    if cutoff is not None and len(frame) == cutoff:
        diagnostics["cf_tail_noop_count"] = int(diagnostics.get("cf_tail_noop_count", 0)) + 1
        state_out.update(previous)
        state_out["checksum"] = _digest(previous)
        return pd.concat([frame, pd.DataFrame([r.values for r in cached_rows])], axis=1).loc[
            :, lambda f: ~f.columns.duplicated(keep="last")
        ]

    states = dict(previous.get("states", {}))
    work = {}
    bootstrap_candidates = []

    def runner(inputs, spec, group):
        key = f"{group}:{spec.indicator_id}"
        local = inputs
        if cutoff is not None:
            required = max(
                spec.warmup_bars,
                m._finite_indicator_history(spec) or 0,
                *(
                    int(value)
                    for name, value in spec.params_dict().items()
                    if name
                    in {
                        "length",
                        "fast",
                        "slow",
                        "signal",
                        "rsi_length",
                        "k",
                        "d",
                        "smooth_k",
                        "upper_length",
                        "lower_length",
                    }
                ),
            )
            if required > BUFFER:
                raise CheckpointUnavailable("profile exceeds native checkpoint buffer")
            prefix = inputs.index[inputs.index < cutoff]
            start = prefix[-BUFFER] if len(prefix) >= BUFFER else inputs.index[0]
            local = inputs.loc[start:]
        old = previous.get("states", {}).get(key) if cutoff is not None else None
        try:
            engine = NativeCheckpoint(previous=old, cutoff=cutoff)
            outputs = engine.compute(m._compute_spec, local, spec)
        except (ValueError, KeyError, TypeError, AttributeError, IndexError) as error:
            if cutoff is None:
                raise
            if len(inputs) <= BUFFER:
                # This is the COMPLETE dependency group, never a truncated
                # approximation to a recursive indicator's warmup.
                engine = NativeCheckpoint()
                outputs = engine.compute(m._compute_spec, inputs, spec)
                bootstrap_candidates.append(
                    (spec.output_columns, inputs.index[inputs.index < cutoff])
                )
            else:
                raise CheckpointUnavailable(f"invalid hidden state: {error}") from error
        if group.endswith(":invariant") or group.endswith(
            ":" + str(int(frame["roll_epoch"].iloc[-1]))
        ):
            states[key] = engine.export()
        work[key] = dict(
            formula_rows=engine.formula_rows,
            full_history_rows=engine.full_history_rows,
            complete_group_bootstrap_rows=engine.full_history_rows if cutoff is not None else 0,
        )
        return {column: series.reindex(inputs.index) for column, series in outputs.items()}

    try:
        result = m._compute_profile_frame_for_series(
            frame,
            profile,
            series_mode="continuous_front",
            adjustment_ladder_rows=ladder,
            checkpoint_runner=runner,
            tail_start=cutoff,
        )
    except CheckpointUnavailable as error:
        # A failed attempt cannot publish a partial state. Bootstrap all streams
        # with the full native producer in this ordinary materialization call.
        diagnostics["cf_tail_fallback_reason"] = str(error)
        if cutoff is not None:
            if "global zero-range" in str(error):
                diagnostics["cf_full_replacement_required"] = True
            return compute(frame, profile, ladder, [], 0, diagnostics, state_out, str(error))
        result = m._compute_profile_frame_for_series(
            frame,
            profile,
            series_mode="continuous_front",
            adjustment_ladder_rows=ladder,
        )
        state_out.clear()
        return result
    if cutoff is not None:
        columns = list(profile.expected_output_columns())
        for names, indices in bootstrap_candidates:
            for column in names:
                for index in indices:
                    left, right = result.loc[index, column], cached_rows[index].values[column]
                    same = (pd.isna(left) and pd.isna(right)) or (
                        not pd.isna(left) and not pd.isna(right) and left == right
                    )
                    if not same:
                        diagnostics["cf_full_replacement_required"] = True
                        return compute(
                            frame,
                            profile,
                            ladder,
                            [],
                            0,
                            diagnostics,
                            state_out,
                            "native_minimum_length_changed_prefix",
                        )
        result[columns] = result[columns].astype(object)
        result.loc[: cutoff - 1, columns] = pd.DataFrame(
            [r.values for r in cached_rows], columns=columns
        ).to_numpy()
        # Reproject held/reset state across a boundary with no eligible new row.
        if "bar_usage_flags" in frame:
            for rule, outputs in m.indicator_bar_usage_groups_for_outputs(tuple(columns)):
                outputs = tuple(c for c in outputs if c not in m.VOLUME_PROFILE_INDICATOR_COLUMNS)
                if not outputs:
                    continue
                eligible = m._bar_usage_eligible_mask(frame, rule.required_flags)
                projected = m._project_bar_usage_computed_columns(
                    frame=frame,
                    computed=result.loc[eligible, list(outputs)],
                    output_columns=outputs,
                    rule=rule,
                )
                for column, series in projected.items():
                    result[column] = series
        diagnostics["cf_tail_partition_count"] = (
            int(diagnostics.get("cf_tail_partition_count", 0)) + 1
        )
    else:
        diagnostics["cf_tail_fallback_reason"] = reason or "bootstrap"
    diagnostics["cf_formula_work"] = work
    payload = dict(
        format="cf-tail.v1",
        context=context,
        count=len(frame),
        last_ts=str(frame["ts"].iloc[-1]),
        epoch=int(frame["roll_epoch"].iloc[-1]),
        states=states,
    )
    state_out.update(payload)
    state_out["checksum"] = _digest(payload)
    return result
