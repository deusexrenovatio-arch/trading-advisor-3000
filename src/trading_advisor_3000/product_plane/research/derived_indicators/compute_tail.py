"""Determine the changed semantic-input prefix BEFORE evaluating L2 formulas."""

from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd


def input_hashes(frame, profile):
    from ..datasets import ResearchBarView
    from .materialize import _source_indicator_columns_for_profile

    semantic = set(ResearchBarView.__dataclass_fields__) | set(
        _source_indicator_columns_for_profile(profile)
    )
    names = sorted(c for c in frame if c in semantic or c.startswith("_cf_"))
    domain = ["derived-input-row.v1", profile.version, profile.output_columns, names]
    header = json.dumps(domain, sort_keys=True).encode()
    normalized = frame[names].astype(object).where(frame[names].notna(), None)
    return [
        hashlib.sha256(header + json.dumps(row, default=str, allow_nan=False).encode()).hexdigest()
        for row in normalized.itertuples(index=False, name=None)
    ]


def compute(frame, profile, old, hashes, compute_contract_hash, diagnostics):
    from . import materialize as m

    needed = {"input_row_hash", "compute_contract_hash", "ts", *profile.output_columns}
    fallback_start = 0
    if old is not None and {"ts", "input_row_hash"} <= set(old.columns) and not old.empty:
        old = old.sort_values("ts").reset_index(drop=True)
        size = min(len(old), len(frame))
        same_input = (
            pd.to_datetime(old["ts"].iloc[:size], utc=True).to_numpy()
            == pd.to_datetime(frame["ts"].iloc[:size], utc=True).to_numpy()
        ) & (old["input_row_hash"].iloc[:size].to_numpy() == np.array(hashes[:size]))
        changed = np.flatnonzero(~same_input)
        fallback_start = int(changed[0]) if len(changed) else size
    if old is None or not needed <= set(old.columns) or old.empty:
        diagnostics["l2_tail_fallback_reason"] = "legacy_or_missing_input_proof"
        return m._compute_derived_frame(base_frame=frame, profile=profile), fallback_start
    old = old.sort_values("ts").reset_index(drop=True)
    if (
        not compute_contract_hash
        or old["ts"].duplicated().any()
        or old.iloc[-1]["compute_contract_hash"] != compute_contract_hash
    ):
        diagnostics["l2_tail_fallback_reason"] = "changed_compute_or_duplicate_keys"
        return m._compute_derived_frame(base_frame=frame, profile=profile), fallback_start
    count = min(len(old), len(frame))
    left = pd.to_datetime(old["ts"], utc=True)
    right = pd.to_datetime(frame["ts"], utc=True)
    same = (left.iloc[:count].to_numpy() == right.iloc[:count].to_numpy()) & (
        old["input_row_hash"].iloc[:count].to_numpy() == np.array(hashes[:count])
    )
    mismatch = np.flatnonzero(~same)
    first = int(mismatch[0]) if len(mismatch) else count
    if first == 0:
        diagnostics["l2_tail_fallback_reason"] = "changed_prefix_or_legacy_hash"
        return m._compute_derived_frame(base_frame=frame, profile=profile), 0
    result = frame.copy()
    outputs = list(profile.output_columns)
    for column in outputs:
        result[column] = pd.Series(np.nan, index=frame.index, dtype=object)
        result.loc[: first - 1, column] = old[column].iloc[:first].to_numpy()
    if first == len(frame):
        diagnostics["l2_formula_rows"] = 0
        return result, first
    groups = (
        m.derived_bar_usage_groups_for_outputs(profile.output_columns)
        if "bar_usage_flags" in frame
        else [(None, profile.output_columns)]
    )
    work = {}
    for rule, columns in groups:
        eligible = (
            m._bar_usage_eligible_mask(frame, rule.required_flags)
            if rule
            else pd.Series(True, index=frame.index)
        )
        indices = frame.index[eligible]
        changed = indices[indices >= first]
        if not len(changed):
            continue
        before = indices[indices < first]
        # 21 eligible observations for diff+20 rolling/divergence; retain a
        # predecessor for crossings and the 5+5 swing confirmation neighborhood.
        start = int(before[-64]) if len(before) >= 64 else int(indices[0])
        affected = int(before[-1]) if len(before) else int(changed[0])
        column_set = set(columns)
        # Scopes include the predecessor's entire session/week because its
        # value participates in a crossing at a scope boundary.
        if any("session" in c or "opening_range" in c for c in columns):
            session = frame.loc[affected, "session_date"]
            matching = indices[(frame.loc[indices, "session_date"] == session).to_numpy()]
            start = min(start, int(matching[0]))
        if any("week" in c for c in columns):
            weeks = pd.to_datetime(frame["ts"], utc=True).dt.strftime("%G-%V")
            matching = indices[(weeks.loc[indices] == weeks.loc[affected]).to_numpy()]
            start = min(start, int(matching[0]))
        local = frame.loc[indices[indices >= start]].copy()
        swing = None
        if column_set & {"swing_high_10", "swing_low_10"} and len(before):
            prior = int(before[-1])
            offset = float(frame.loc[prior, "_cf_offset"]) if "_cf_offset" in frame else 0.0
            swing = dict(
                start=int((local.index < first).sum()),
                high=old.loc[prior].get("swing_high_10", np.nan) - offset,
                low=old.loc[prior].get("swing_low_10", np.nan) - offset,
            )
        grouped = m._profile_for_output_columns(profile, column_set)
        computed = m._compute_derived_frame_unmasked(
            base_frame=local, profile=grouped, swing_state=swing
        )
        for column in columns:
            result.loc[changed, column] = computed.loc[changed, column]
        work[str(rule)] = len(local)
    if "bar_usage_flags" in frame:
        for rule, columns in m.derived_bar_usage_groups_for_outputs(profile.output_columns):
            eligible = m._bar_usage_eligible_mask(frame, rule.required_flags)
            projected = m._project_bar_usage_computed_columns(
                frame=frame,
                computed=result.loc[eligible, list(columns)],
                output_columns=columns,
                rule=rule,
            )
            for column, values in projected.items():
                result[column] = values
    diagnostics["l2_formula_rows"] = sum(work.values())
    diagnostics["l2_formula_work"] = work
    diagnostics["l2_reused_input_prefix"] = first
    return result, first
