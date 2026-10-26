"""Numerically proven restoration before a changed CF window; existing store owns persistence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from .native_checkpoint import BUFFER, CheckpointUnavailable, NativeCheckpoint, identity

STRIDE = 256
RETENTION = 4096
FORMAT = "cf-tail.v2"


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def checkpoint_counts(count):
    floor = max(STRIDE, ((count - RETENTION) // STRIDE) * STRIDE)
    return sorted({*range(floor, count, STRIDE), count})


def _prefix_digests(hashes, counts):
    wanted = set(counts)
    digest = hashlib.sha256()
    result = {}
    for count, value in enumerate(hashes, 1):
        digest.update(value.encode())
        digest.update(b"\n")
        if count in wanted:
            result[count] = digest.hexdigest()
    return result


def _state_epoch(key):
    # group:spec; group ends in an epoch or 'invariant'.
    token = key.rsplit(":", 2)[-2]
    return None if token == "invariant" else int(token)


def compute(
    frame,
    profile,
    ladder,
    cached_rows,
    prefix_length,
    diagnostics,
    state_out,
    bootstrap_reason=None,
    *,
    input_hashes=(),
    checkpoints_out=None,
):
    from . import materialize as m

    checkpoints_out = checkpoints_out if checkpoints_out is not None else {}
    checkpoints_out.clear()
    state_out.clear()

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
            rules=[rule.rule_hash for rule in m.rules_for_indicator_profile(profile)],
            ladder=m._normalize_ladder_rows(ladder),
            native=identity(),
            adapter={
                name: hashlib.sha256(
                    Path(__file__).with_name(name + ".py").read_bytes()
                ).hexdigest()
                for name in ("cf_tail", "native_checkpoint")
            },
        )
    )
    targets = checkpoint_counts(len(frame))
    counts = {*targets, *(i + 1 for i, row in enumerate(cached_rows) if row.compute_state_json)}
    proofs = _prefix_digests(input_hashes, counts)
    valid = {}
    incompatible_context = False
    reason = bootstrap_reason or "missing_checkpoint"
    for index, row in enumerate(cached_rows):
        count = index + 1
        if not row.compute_state_json or count > min(prefix_length, len(frame)):
            continue
        try:
            previous = json.loads(row.compute_state_json)
            checksum = previous.pop("checksum")
            if _digest(previous) != checksum:
                raise ValueError("checkpoint checksum differs")
            if previous.get("context") != context or previous.get("format") != FORMAT:
                incompatible_context = True
                raise ValueError("checkpoint context differs")
            if not isinstance(previous.get("states"), dict):
                raise ValueError("malformed checkpoint states")
            if (
                previous.get("count") != count
                or previous.get("last_ts") != row.ts
                or row.ts != str(frame["ts"].iloc[index])
                or previous.get("prefix_hash") != proofs.get(count)
                or previous.get("max_epoch") != int(frame["roll_epoch"].iloc[-1])
                or previous.get("epoch") != int(frame["roll_epoch"].iloc[index])
            ):
                raise ValueError("anchor does not certify the unchanged numerical prefix")
            valid[count] = previous
        except (ValueError, KeyError, TypeError, AttributeError):
            reason = "invalid_checkpoint_or_changed_history"
    cutoff = max(valid) if valid else None
    previous = valid[cutoff] if cutoff is not None else {}
    if cached_rows and not valid:
        reason = "invalid_checkpoint_or_changed_history"
        if incompatible_context:
            diagnostics["cf_full_replacement_required"] = True
    if cutoff is not None:
        reason = ""
        diagnostics["cf_replay_start"] = cutoff
    for count in targets:
        if count in valid:
            payload = dict(valid[count])
            payload["checksum"] = _digest(payload)
            checkpoints_out[count - 1] = payload
    if cutoff is not None and len(frame) == cutoff:
        diagnostics["cf_tail_noop_count"] = int(diagnostics.get("cf_tail_noop_count", 0)) + 1
        state_out.update(previous)
        state_out["checksum"] = _digest(previous)
        return pd.concat([frame, pd.DataFrame([r.values for r in cached_rows])], axis=1).loc[
            :, lambda f: ~f.columns.duplicated(keep="last")
        ]

    states = dict(previous.get("states", {}))
    captures = [count for count in targets if cutoff is None or count > cutoff]
    states_at = {count: dict(states) for count in captures}
    work = {}
    bootstrap_candidates = []
    eligible_indices = {}

    def complete_prefix_size(group):
        parts = group.split(":")
        flags = int(parts[0]) if parts[0].isdigit() else None
        if flags not in eligible_indices:
            eligible_indices[flags] = (
                frame.index[m._bar_usage_eligible_mask(frame, flags)]
                if flags is not None
                else frame.index
            )
        indices = eligible_indices[flags]
        token = parts[-1]
        if token != "invariant":
            epoch = int(token)
            epochs = frame.loc[indices, "roll_epoch"]
            indices = indices[epochs.eq(epoch) if "reset" in parts else epochs.le(epoch)]
        return int((indices < cutoff).sum())

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
            needed = [
                count
                for count in captures
                if group.endswith(":invariant")
                or int(group.rsplit(":", 1)[-1]) >= int(frame["roll_epoch"].iloc[count - 1])
            ]
            engine = NativeCheckpoint(previous=old, cutoff=cutoff, snapshot_counts=needed)
            outputs = engine.compute(m._compute_spec, local, spec)
        except (ValueError, KeyError, TypeError, AttributeError, IndexError) as error:
            if cutoff is None:
                raise
            if complete_prefix_size(group) <= BUFFER:
                # Source admission proves this contains the COMPLETE causal
                # prefix, not merely 512 rows sliced from a longer recurrence.
                engine = NativeCheckpoint(snapshot_counts=needed)
                outputs = engine.compute(m._compute_spec, inputs, spec)
                bootstrap_candidates.append(
                    (spec.output_columns, inputs.index[inputs.index < cutoff])
                )
            else:
                raise CheckpointUnavailable(f"invalid hidden state: {error}") from error
        for count in needed:
            states_at[count][key] = engine.export_at(count)
        invalid_inputs = next(
            (
                s.attrs["invalid_price_inputs"]
                for s in outputs.values()
                if "invalid_price_inputs" in s.attrs
            ),
            None,
        )
        if invalid_inputs is not None:
            diagnostics.setdefault("invalid_price_inputs", {})[key] = invalid_inputs
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
            return compute(
                frame,
                profile,
                ladder,
                [],
                0,
                diagnostics,
                state_out,
                str(error),
                input_hashes=input_hashes,
                checkpoints_out=checkpoints_out,
            )
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
                            input_hashes=input_hashes,
                            checkpoints_out=checkpoints_out,
                        )
        result = pd.concat(
            [frame.drop(columns=columns, errors="ignore"), result[columns].reindex(frame.index)],
            axis=1,
        )
        result[columns] = result[columns].astype(object)
        result.loc[: cutoff - 1, columns] = pd.DataFrame(
            [r.values for r in cached_rows[:cutoff]], columns=columns
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
    for count in captures:
        epoch = int(frame["roll_epoch"].iloc[count - 1])
        payload = dict(
            format=FORMAT,
            context=context,
            count=count,
            prefix_hash=proofs[count],
            last_ts=str(frame["ts"].iloc[count - 1]),
            epoch=epoch,
            max_epoch=int(frame["roll_epoch"].iloc[-1]),
            states={
                key: value
                for key, value in states_at[count].items()
                if _state_epoch(key) is None or _state_epoch(key) >= epoch
            },
        )
        payload["checksum"] = _digest(payload)
        checkpoints_out[count - 1] = payload
    state_out.update(checkpoints_out[len(frame) - 1])
    diagnostics["cf_checkpoint_count"] = len(checkpoints_out)
    return result
