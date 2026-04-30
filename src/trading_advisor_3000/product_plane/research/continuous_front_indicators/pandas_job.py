from __future__ import annotations

from pathlib import Path
import json
import math

import pandas as pd
import pandas_ta_classic as ta

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    has_delta_log,
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    current_derived_indicator_profile,
)
from trading_advisor_3000.product_plane.research.indicators.registry import default_indicator_profile

from .contracts import CF_INDICATOR_TABLES, continuous_front_indicator_store_contract
from .input_projection import (
    build_cf_indicator_input_rows,
    load_adjustment_ladder_rows,
    load_research_bar_views,
)
from .publish import fail_count, publish_status_from_qc
from .qc import qc_observation, stable_hash, utc_now_iso
from .rules import (
    DEFAULT_RULE_SET_VERSION,
    IndicatorRollRule,
    adapter_bundle_hash,
    default_indicator_roll_rules,
    rule_set_hash,
    rules_for_indicator_profile,
    rules_to_rows,
)
from .verifier import verify_input_projection_identity, verify_rule_coverage


REQUIRED_ACCEPTANCE_QC_GROUPS = frozenset(
    {
        "adapter_authorization",
        "formula_sample",
        "input_projection",
        "lineage",
        "pandas_ta_parity",
        "prefix_invariance",
        "rule_coverage",
        "schema",
    }
)

MINIMUM_FORMULA_SAMPLE_COLUMNS = (
    ("base", "sma_20"),
    ("base", "ema_20"),
    ("base", "true_range"),
    ("base", "atr_14"),
    ("base", "rsi_14"),
    ("base", "roc_10"),
    ("base", "ppo_12_26_9"),
    ("base", "vwma_20"),
    ("derived", "session_vwap"),
    ("base", "rvol_20"),
    ("base", "oi_change_1"),
    ("derived", "rolling_high_20"),
    ("derived", "session_position"),
    ("derived", "cross_close_ema_20_code"),
)

FORMULA_SAMPLE_MAX_ROWS_PER_COLUMN = 5
FORMULA_SAMPLE_MAX_FAILURES = 50


def _load_dataset_manifest(*, materialized_output_dir: Path, dataset_version: str) -> dict[str, object]:
    rows = read_delta_table_rows(
        materialized_output_dir / "research_datasets.delta",
        filters=[("dataset_version", "=", dataset_version)],
    )
    if not rows:
        raise KeyError(f"dataset_version not found: {dataset_version}")
    return dict(rows[0])


def _json_payload(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        loaded = json.loads(value)
        if isinstance(loaded, dict):
            return {str(key): item for key, item in loaded.items()}
    return {}


def _policy_versions(manifest: dict[str, object]) -> tuple[str, str]:
    policy = _json_payload(manifest.get("continuous_front_policy"))
    return (
        str(policy.get("roll_policy_version") or "front_liquidity_oi_v1"),
        str(policy.get("adjustment_policy_version") or "backward_current_anchor_additive_v1"),
    )


def _source_versions_hash(paths: dict[str, Path]) -> tuple[str, str]:
    versions = {
        table_name: {
            "path": path.as_posix(),
            "row_count": count_delta_table_rows(path) if has_delta_log(path) else 0,
        }
        for table_name, path in sorted(paths.items())
    }
    return json.dumps(versions, ensure_ascii=False, sort_keys=True), stable_hash(versions)


def _row_hash(row: dict[str, object], value_columns: tuple[str, ...]) -> str:
    payload = {column: row.get(column) for column in value_columns}
    payload.update(
        {
            "dataset_version": row.get("dataset_version"),
            "instrument_id": row.get("instrument_id"),
            "timeframe": row.get("timeframe"),
            "ts": row.get("ts"),
        }
    )
    return stable_hash(payload)


def _row_key(row: dict[str, object]) -> tuple[str, str, str]:
    return (str(row["instrument_id"]), str(row["timeframe"]), str(row["ts"]).replace("+00:00", "Z"))


def _cross_contract_window_any(input_row: dict[str, object], *, max_window_bars: int) -> bool:
    if max_window_bars <= 0 or int(input_row.get("roll_seq") or 0) <= 0:
        return False
    return int(input_row.get("bars_since_roll") or 0) < max_window_bars


def _max_strict_roll_window_bars(
    rules: tuple[IndicatorRollRule, ...],
    *,
    output_family: str,
) -> int:
    return max(
        (
            max(1, int(rule.warmup_bars))
            for rule in rules
            if rule.output_family == output_family and not rule.group.allow_cross_contract_window
        ),
        default=0,
    )


def _base_sidecar_rows(
    *,
    base_rows: list[dict[str, object]],
    input_rows_by_key: dict[tuple[str, str, str], dict[str, object]],
    dataset_version: str,
    roll_policy_version: str,
    adjustment_policy_version: str,
    indicator_set_version: str,
    rule_set_version: str,
    adapter_hash: str,
    value_columns: tuple[str, ...],
    max_cross_contract_window_bars: int,
    created_at_utc: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for source in sorted(base_rows, key=lambda row: (str(row["instrument_id"]), str(row["timeframe"]), str(row["ts"]))):
        key = _row_key(source)
        input_row = input_rows_by_key.get(key)
        if input_row is None:
            continue
        row = {
            "dataset_version": dataset_version,
            "roll_policy_version": roll_policy_version,
            "adjustment_policy_version": adjustment_policy_version,
            "indicator_set_version": indicator_set_version,
            "rule_set_version": rule_set_version,
            "instrument_id": source["instrument_id"],
            "timeframe": source["timeframe"],
            "ts": source["ts"],
            "ts_close": input_row["ts_close"],
            "session_date": input_row["session_date"],
            "active_contract_id": input_row["active_contract_id"],
            "roll_epoch_id": input_row["roll_epoch_id"],
            "roll_seq": input_row["roll_seq"],
            "cumulative_additive_offset": input_row["cumulative_additive_offset"],
            "source_input_row_hash": input_row["input_front_row_hash"],
            "indicator_row_hash": _row_hash(source, value_columns),
            "adapter_bundle_hash": adapter_hash,
            "cross_contract_window_any": _cross_contract_window_any(
                input_row,
                max_window_bars=max_cross_contract_window_bars,
            ),
            "created_at_utc": created_at_utc,
        }
        for column in value_columns:
            row[column] = source.get(column)
        rows.append(row)
    return rows


def _derived_sidecar_rows(
    *,
    derived_rows: list[dict[str, object]],
    base_rows_by_key: dict[tuple[str, str, str], dict[str, object]],
    input_rows_by_key: dict[tuple[str, str, str], dict[str, object]],
    dataset_version: str,
    roll_policy_version: str,
    adjustment_policy_version: str,
    indicator_set_version: str,
    derived_set_version: str,
    rule_set_version: str,
    adapter_hash: str,
    value_columns: tuple[str, ...],
    max_cross_contract_window_bars: int,
    created_at_utc: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for source in sorted(derived_rows, key=lambda row: (str(row["instrument_id"]), str(row["timeframe"]), str(row["ts"]))):
        key = _row_key(source)
        input_row = input_rows_by_key.get(key)
        base_row = base_rows_by_key.get(key)
        if input_row is None:
            continue
        row = {
            "dataset_version": dataset_version,
            "roll_policy_version": roll_policy_version,
            "adjustment_policy_version": adjustment_policy_version,
            "indicator_set_version": indicator_set_version,
            "derived_set_version": derived_set_version,
            "rule_set_version": rule_set_version,
            "instrument_id": source["instrument_id"],
            "timeframe": source["timeframe"],
            "ts": source["ts"],
            "ts_close": input_row["ts_close"],
            "session_date": input_row["session_date"],
            "active_contract_id": input_row["active_contract_id"],
            "roll_epoch_id": input_row["roll_epoch_id"],
            "roll_seq": input_row["roll_seq"],
            "cumulative_additive_offset": input_row["cumulative_additive_offset"],
            "source_input_row_hash": input_row["input_front_row_hash"],
            "source_base_indicator_row_hash": "" if base_row is None else str(base_row["indicator_row_hash"]),
            "derived_row_hash": _row_hash(source, value_columns),
            "adapter_bundle_hash": adapter_hash,
            "cross_contract_window_any": _cross_contract_window_any(
                input_row,
                max_window_bars=max_cross_contract_window_bars,
            ),
            "created_at_utc": created_at_utc,
        }
        for column in value_columns:
            row[column] = source.get(column)
        rows.append(row)
    return rows


def _write_table(
    *,
    output_dir: Path,
    table_name: str,
    rows: list[dict[str, object]],
    contract: dict[str, dict[str, object]],
) -> str:
    table_path = output_dir / f"{table_name}.delta"
    write_delta_table_rows(table_path=table_path, rows=rows, columns=dict(contract[table_name]["columns"]))
    return table_path.as_posix()


def _prefix_cut_timestamps(bar_views: list[object]) -> list[str]:
    grouped: dict[tuple[str, str], list[object]] = {}
    for row in sorted(bar_views, key=lambda item: (str(item.instrument_id), str(item.timeframe), str(item.ts))):
        grouped.setdefault((str(row.instrument_id), str(row.timeframe)), []).append(row)

    cuts: set[str] = set()
    for series in grouped.values():
        if len(series) < 2:
            continue
        cuts.add(str(series[-2].ts))
        for index, row in enumerate(series):
            previous = series[index - 1] if index > 0 else None
            is_roll_boundary = bool(getattr(row, "is_roll_bar", False)) or bool(getattr(row, "is_first_bar_after_roll", False))
            if previous is not None and int(getattr(previous, "roll_epoch", 0) or 0) != int(getattr(row, "roll_epoch", 0) or 0):
                is_roll_boundary = True
            if not is_roll_boundary:
                continue
            for candidate_index in (index - 1, index, index + 1, index + 20):
                if 0 <= candidate_index < len(series):
                    cuts.add(str(series[candidate_index].ts))
    return sorted(cuts)


def _objects_through_cut(rows: list[object], cut_ts: str) -> list[object]:
    return [row for row in rows if str(getattr(row, "ts")) <= cut_ts]


def _ladder_through_cut(rows: tuple[dict[str, object], ...], cut_ts: str) -> tuple[dict[str, object], ...]:
    return tuple(row for row in rows if str(row.get("effective_ts") or "") <= cut_ts)


def _rows_through_cut(rows: list[dict[str, object]], cut_ts: str) -> list[dict[str, object]]:
    return [row for row in rows if str(row.get("ts")).replace("+00:00", "Z") <= cut_ts]


def _is_nullish(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _value_matches(left: object, right: object) -> bool:
    if _is_nullish(left) and _is_nullish(right):
        return True
    if _is_nullish(left) or _is_nullish(right):
        return False
    try:
        return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-9)
    except (TypeError, ValueError):
        return str(left) == str(right)


def _json_value(value: object) -> object:
    if _is_nullish(value):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if numeric != numeric:
        return None
    return numeric


def _compare_value_rows(
    *,
    observed_rows: list[dict[str, object]],
    expected_rows: list[dict[str, object]],
    value_columns: tuple[str, ...],
    entity_key: str,
) -> list[dict[str, object]]:
    observed_by_key = {_row_key(row): row for row in observed_rows}
    failures: list[dict[str, object]] = []
    for expected in expected_rows:
        key = _row_key(expected)
        observed = observed_by_key.get(key)
        if observed is None:
            failures.append({"entity": entity_key, "key": key, "failure": "missing_prefix_row"})
            continue
        for column in value_columns:
            if not _value_matches(observed.get(column), expected.get(column)):
                failures.append(
                    {
                        "entity": entity_key,
                        "key": key,
                        "column": column,
                        "observed": _json_value(observed.get(column)),
                        "expected": _json_value(expected.get(column)),
                    }
                )
                if len(failures) >= 50:
                    return failures
    return failures


def _null_mask(row: dict[str, object], value_columns: tuple[str, ...]) -> dict[str, bool]:
    return {column: _is_nullish(row.get(column)) for column in value_columns}


def _compare_sidecar_evidence(
    *,
    observed_rows: list[dict[str, object]],
    expected_rows: list[dict[str, object]],
    value_columns: tuple[str, ...],
    metadata_columns: tuple[str, ...],
    entity_key: str,
) -> list[dict[str, object]]:
    observed_by_key = {_row_key(row): row for row in observed_rows}
    failures: list[dict[str, object]] = []
    for expected in expected_rows:
        key = _row_key(expected)
        observed = observed_by_key.get(key)
        if observed is None:
            failures.append({"entity": entity_key, "key": key, "failure": "missing_prefix_evidence_row"})
            continue
        for column in metadata_columns:
            if observed.get(column) != expected.get(column):
                failures.append(
                    {
                        "entity": entity_key,
                        "key": key,
                        "column": column,
                        "observed": _json_value(observed.get(column)),
                        "expected": _json_value(expected.get(column)),
                    }
                )
        observed_null_mask = _null_mask(observed, value_columns)
        expected_null_mask = _null_mask(expected, value_columns)
        if observed_null_mask != expected_null_mask:
            failures.append(
                {
                    "entity": entity_key,
                    "key": key,
                    "failure": "null_mask_changed",
                    "observed": observed_null_mask,
                    "expected": expected_null_mask,
                }
            )
        if len(failures) >= 50:
            return failures
    return failures


def _verify_prefix_invariance(
    *,
    run_id: str,
    bar_views: list[object],
    ladder_rows: tuple[dict[str, object], ...],
    dataset_version: str,
    source_canonical_version: str,
    roll_policy_version: str,
    adjustment_policy_version: str,
    indicator_set_version: str,
    derived_set_version: str,
    rule_set_version: str,
    adapter_hash: str,
    indicator_profile: object,
    derived_profile: object,
    base_rows: list[dict[str, object]],
    derived_rows: list[dict[str, object]],
    indicator_value_columns: tuple[str, ...],
    derived_value_columns: tuple[str, ...],
    max_base_cross_contract_window_bars: int,
    max_derived_cross_contract_window_bars: int,
    build_indicator_frames_fn: object,
    build_derived_indicator_frames_fn: object,
    created_at_utc: str,
) -> dict[str, object]:
    cut_timestamps = _prefix_cut_timestamps(bar_views)
    failures: list[dict[str, object]] = []
    checked_cuts = 0
    if not cut_timestamps:
        failures.append({"failure": "not_enough_rows_for_prefix_cut_check"})
    for cut_ts in cut_timestamps:
        prefix_views = _objects_through_cut(bar_views, cut_ts)
        prefix_ladder_rows = _ladder_through_cut(ladder_rows, cut_ts)
        if not prefix_views:
            continue
        checked_cuts += 1
        prefix_input_rows = build_cf_indicator_input_rows(
            bar_views=prefix_views,
            adjustment_ladder_rows=prefix_ladder_rows,
            dataset_version=dataset_version,
            source_canonical_version=source_canonical_version,
            roll_policy_version=roll_policy_version,
            adjustment_policy_version=adjustment_policy_version,
            created_at_utc=created_at_utc,
        )
        prefix_input_rows_by_key = {_row_key(row): row for row in prefix_input_rows}
        prefix_base = build_indicator_frames_fn(
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            bar_views=prefix_views,
            series_mode="continuous_front",
            profile=indicator_profile,
            adjustment_ladder_rows=prefix_ladder_rows,
        )
        prefix_base_rows = _base_sidecar_rows(
            base_rows=[row.to_dict() for row in prefix_base],
            input_rows_by_key=prefix_input_rows_by_key,
            dataset_version=dataset_version,
            roll_policy_version=roll_policy_version,
            adjustment_policy_version=adjustment_policy_version,
            indicator_set_version=indicator_set_version,
            rule_set_version=rule_set_version,
            adapter_hash=adapter_hash,
            value_columns=indicator_value_columns,
            max_cross_contract_window_bars=max_base_cross_contract_window_bars,
            created_at_utc=created_at_utc,
        )
        prefix_base_rows_by_key = {_row_key(row): row for row in prefix_base_rows}
        prefix_derived = build_derived_indicator_frames_fn(
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_set_version,
            bar_views=prefix_views,
            indicator_rows=prefix_base,
            series_mode="continuous_front",
            profile=derived_profile,
            adjustment_ladder_rows=prefix_ladder_rows,
        )
        prefix_derived_rows = _derived_sidecar_rows(
            derived_rows=[row.to_dict() for row in prefix_derived],
            base_rows_by_key=prefix_base_rows_by_key,
            input_rows_by_key=prefix_input_rows_by_key,
            dataset_version=dataset_version,
            roll_policy_version=roll_policy_version,
            adjustment_policy_version=adjustment_policy_version,
            indicator_set_version=indicator_set_version,
            derived_set_version=derived_set_version,
            rule_set_version=rule_set_version,
            adapter_hash=adapter_hash,
            value_columns=derived_value_columns,
            max_cross_contract_window_bars=max_derived_cross_contract_window_bars,
            created_at_utc=created_at_utc,
        )
        failures.extend(
            _compare_sidecar_evidence(
                observed_rows=_rows_through_cut(base_rows, cut_ts),
                expected_rows=prefix_base_rows,
                value_columns=indicator_value_columns,
                metadata_columns=("source_input_row_hash", "indicator_row_hash", "cross_contract_window_any"),
                entity_key="continuous_front_indicator_frames",
            )
        )
        failures.extend(
            _compare_sidecar_evidence(
                observed_rows=_rows_through_cut(derived_rows, cut_ts),
                expected_rows=prefix_derived_rows,
                value_columns=derived_value_columns,
                metadata_columns=(
                    "source_input_row_hash",
                    "source_base_indicator_row_hash",
                    "derived_row_hash",
                    "cross_contract_window_any",
                ),
                entity_key="continuous_front_derived_indicator_frames",
            )
        )
        if len(failures) >= 50:
            break
    return qc_observation(
        run_id=run_id,
        check_id="prefix_roll_cut_recompute_invariance",
        check_group="prefix_invariance",
        severity="blocker",
        status="fail" if failures else "pass",
        entity_key="continuous_front_indicator_sidecars",
        observed_value={"checked_cuts": checked_cuts, "failures": len(failures)},
        expected_value=0,
        sample_rows=failures[:50],
    )


def _series_dataframes(input_rows: list[dict[str, object]]) -> list[pd.DataFrame]:
    if not input_rows:
        return []
    frame = pd.DataFrame(input_rows).sort_values(["instrument_id", "timeframe", "ts"]).reset_index(drop=True)
    numeric_columns = (
        "open0",
        "high0",
        "low0",
        "close0",
        "true_range0",
        "native_volume",
        "native_open_interest",
        "cumulative_additive_offset",
        "roll_seq",
    )
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return [
        group.reset_index(drop=True)
        for _, group in frame.groupby(["instrument_id", "timeframe"], sort=False)
    ]


def _frame_key_at(frame: pd.DataFrame, position: int) -> tuple[str, str, str]:
    row = frame.iloc[position]
    return (str(row["instrument_id"]), str(row["timeframe"]), str(row["ts"]).replace("+00:00", "Z"))


def _target_anchor_indicator_sample(
    *,
    close0: pd.Series,
    offset: pd.Series,
    position: int,
    formula: str,
) -> object:
    target_close = close0.iloc[: position + 1] + float(offset.iloc[position])
    if formula == "roc_10":
        series = ta.roc(target_close, length=10)
    elif formula == "ppo_12_26_9":
        ppo = ta.ppo(target_close, fast=12, slow=26, signal=9)
        series = None if ppo is None else ppo["PPO_12_26_9"]
    else:
        raise ValueError(f"unsupported target-anchor sample formula: {formula}")
    if series is None or series.empty:
        return None
    value = series.iloc[-1]
    return None if _is_nullish(value) else float(value)


def _change_code_sample(current: pd.Series, reference: pd.Series) -> pd.Series:
    diff = current - reference
    previous = diff.shift(1)
    values: list[int | None] = []
    for prev, now in zip(previous.tolist(), diff.tolist(), strict=True):
        if _is_nullish(prev) or _is_nullish(now):
            values.append(None)
        elif float(prev) <= 0.0 < float(now):
            values.append(1)
        elif float(prev) >= 0.0 > float(now):
            values.append(-1)
        else:
            values.append(0)
    return pd.Series(values, index=current.index, dtype="object")


def _same_roll_window_mask(roll_seq: pd.Series, *, window: int) -> pd.Series:
    crosses = roll_seq.rolling(window=window, min_periods=window).apply(
        lambda values: 0.0 if len(set(int(value) for value in values)) == 1 else 1.0,
        raw=False,
    )
    return crosses.fillna(1.0).astype(bool)


def _series_or_null(series: pd.Series | None, index: pd.Index) -> pd.Series:
    return pd.Series([pd.NA] * len(index), index=index, dtype="object") if series is None else series


def _sample_positions_for_expected_column(
    *,
    frame: pd.DataFrame,
    rows_by_key: dict[tuple[str, str, str], dict[str, object]],
    expected: pd.Series,
    column: str,
) -> list[int]:
    if frame.empty:
        return []
    priority: list[int] = [len(frame) - 1, max(0, len(frame) // 2), 0]
    offset = pd.to_numeric(frame["cumulative_additive_offset"], errors="coerce").fillna(0.0)
    roll_seq = pd.to_numeric(frame["roll_seq"], errors="coerce").fillna(0).astype(int)
    for position in range(1, len(frame)):
        if int(roll_seq.iloc[position]) == int(roll_seq.iloc[position - 1]):
            continue
        priority.extend([position - 1, position, min(position + 1, len(frame) - 1), min(position + 20, len(frame) - 1)])
    for position in range(len(frame) - 1, -1, -1):
        value = expected.iloc[position] if position < len(expected) else pd.NA
        row = rows_by_key.get(_frame_key_at(frame, position))
        observed = None if row is None else row.get(column)
        if not _is_nullish(value) or not _is_nullish(observed):
            priority.append(position)
    priority.extend(range(len(frame) - 1, -1, -1))

    selected: list[int] = []
    seen: set[int] = set()
    for position in priority:
        if position in seen or position < 0 or position >= len(frame):
            continue
        seen.add(position)
        if _frame_key_at(frame, position) not in rows_by_key:
            continue
        selected.append(position)
        if len(selected) >= FORMULA_SAMPLE_MAX_ROWS_PER_COLUMN:
            break
    return selected


def _sample_positions_for_column(
    *,
    frame: pd.DataFrame,
    rows_by_key: dict[tuple[str, str, str], dict[str, object]],
    column: str,
) -> list[int]:
    if frame.empty:
        return []
    priority: list[int] = [len(frame) - 1, max(0, len(frame) // 2)]
    roll_seq = pd.to_numeric(frame["roll_seq"], errors="coerce").fillna(0).astype(int)
    for position in range(1, len(frame)):
        if int(roll_seq.iloc[position]) == int(roll_seq.iloc[position - 1]):
            continue
        priority.extend([position - 1, position, min(position + 1, len(frame) - 1), min(position + 20, len(frame) - 1)])
    priority.extend(range(len(frame) - 1, -1, -1))

    selected: list[int] = []
    seen: set[int] = set()
    for position in priority:
        if position in seen:
            continue
        seen.add(position)
        row = rows_by_key.get(_frame_key_at(frame, position))
        if row is None or column not in row or _is_nullish(row.get(column)):
            continue
        selected.append(position)
        if len(selected) >= FORMULA_SAMPLE_MAX_ROWS_PER_COLUMN:
            break
    return selected


def _expected_formula_series(frame: pd.DataFrame) -> dict[str, pd.Series]:
    close0 = pd.to_numeric(frame["close0"], errors="coerce")
    high0 = pd.to_numeric(frame["high0"], errors="coerce")
    low0 = pd.to_numeric(frame["low0"], errors="coerce")
    volume = pd.to_numeric(frame["native_volume"], errors="coerce")
    oi = pd.to_numeric(frame["native_open_interest"], errors="coerce")
    roll_seq = pd.to_numeric(frame["roll_seq"], errors="coerce").fillna(0).astype(int)

    typical0 = (high0 + low0 + close0) / 3.0
    weighted = typical0 * volume
    session_key = frame["session_date"]
    session_vwap0 = weighted.groupby(session_key, sort=False).cumsum() / volume.groupby(session_key, sort=False).cumsum().replace({0.0: pd.NA})
    rolling_high_20 = high0.rolling(window=20, min_periods=20).max() + offset
    session_high = high0.groupby(session_key, sort=False).cummax() + offset
    session_low = low0.groupby(session_key, sort=False).cummin() + offset
    close = close0 + offset
    session_position = (close - session_low) / (session_high - session_low).replace({0.0: pd.NA})

    volume_mean_20 = volume.rolling(window=20, min_periods=20).mean()
    rvol_20 = volume / volume_mean_20.replace({0.0: pd.NA})
    rvol_20.loc[_same_roll_window_mask(roll_seq, window=20)] = pd.NA

    oi_change_1 = oi.diff(1)
    oi_change_1.loc[~(roll_seq == roll_seq.shift(1)).fillna(False)] = pd.NA

    sma_20 = _series_or_null(ta.sma(close0, length=20), frame.index) + offset
    ema_20 = _series_or_null(ta.ema(close0, length=20), frame.index) + offset
    atr_14 = _series_or_null(ta.atr(high0, low0, close0, length=14), frame.index)
    rsi_14 = _series_or_null(ta.rsi(close0, length=14), frame.index)
    vwma_20 = _series_or_null(ta.vwma(close0, volume, length=20), frame.index) + offset

    return {
        "sma_20": sma_20,
        "ema_20": ema_20,
        "true_range": pd.to_numeric(frame["true_range0"], errors="coerce"),
        "atr_14": atr_14,
        "rsi_14": rsi_14,
        "vwma_20": vwma_20,
        "session_vwap": session_vwap0 + offset,
        "rvol_20": rvol_20,
        "oi_change_1": oi_change_1,
        "rolling_high_20": rolling_high_20,
        "session_position": session_position,
        "cross_close_ema_20_code": _change_code_sample(close, ema_20),
    }


def _recompute_formula_price_inputs(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    close = pd.to_numeric(result["close"], errors="coerce")
    high = pd.to_numeric(result["high"], errors="coerce")
    low = pd.to_numeric(result["low"], errors="coerce")
    open_ = pd.to_numeric(result["open"], errors="coerce")
    previous_close = close.shift(1)
    result["ret_1"] = (close / previous_close) - 1.0
    result.loc[previous_close.isna() | (previous_close == 0.0), "ret_1"] = pd.NA
    result["log_ret_1"] = pd.NA
    valid_ret = previous_close.notna() & (previous_close > 0.0) & (close > 0.0)
    result.loc[valid_ret, "log_ret_1"] = (close[valid_ret] / previous_close[valid_ret]).map(math.log)
    result["hl_range"] = high - low
    result["oc_range"] = close - open_
    result["true_range"] = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return result


def _formula_input_frame(
    frame: pd.DataFrame,
    *,
    price_space: str,
    target_offset: float = 0.0,
) -> pd.DataFrame:
    result = pd.DataFrame(index=frame.index)
    if price_space == "zero_anchor":
        result["open"] = pd.to_numeric(frame["open0"], errors="coerce")
        result["high"] = pd.to_numeric(frame["high0"], errors="coerce")
        result["low"] = pd.to_numeric(frame["low0"], errors="coerce")
        result["close"] = pd.to_numeric(frame["close0"], errors="coerce")
    elif price_space == "target_anchor":
        result["open"] = pd.to_numeric(frame["open0"], errors="coerce") + target_offset
        result["high"] = pd.to_numeric(frame["high0"], errors="coerce") + target_offset
        result["low"] = pd.to_numeric(frame["low0"], errors="coerce") + target_offset
        result["close"] = pd.to_numeric(frame["close0"], errors="coerce") + target_offset
    elif price_space == "native":
        result["open"] = pd.to_numeric(frame["native_open"], errors="coerce")
        result["high"] = pd.to_numeric(frame["native_high"], errors="coerce")
        result["low"] = pd.to_numeric(frame["native_low"], errors="coerce")
        result["close"] = pd.to_numeric(frame["native_close"], errors="coerce")
    else:
        raise ValueError(f"unsupported formula reference price_space: {price_space}")
    result["volume"] = pd.to_numeric(frame["native_volume"], errors="coerce")
    result["open_interest"] = pd.to_numeric(frame["native_open_interest"], errors="coerce")
    return _recompute_formula_price_inputs(result)


def _formula_group_frame(
    group_id: str,
    *,
    zero_anchor_frame: pd.DataFrame,
    target_anchor_frame: pd.DataFrame,
    native_frame: pd.DataFrame,
) -> pd.DataFrame:
    if group_id in {"price_level_post_transform", "price_range_on_p0", "oscillator_on_p0"}:
        return zero_anchor_frame
    if group_id in {"anchor_sensitive_roll_aware", "price_volume_roll_aware"}:
        return target_anchor_frame
    if group_id in {"native_volume_oi_roll_aware", "grid_locked_native"}:
        return native_frame
    return target_anchor_frame


def _outputs_by_group_for_spec(spec: object, rules_by_output: dict[str, IndicatorRollRule]) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for output_column in spec.output_columns:
        grouped.setdefault(rules_by_output[output_column].calculation_group_id, []).append(output_column)
    return {group_id: tuple(columns) for group_id, columns in grouped.items()}


def _enforce_formula_base_roll_boundaries(result: pd.DataFrame, indicator_profile: object) -> pd.DataFrame:
    from trading_advisor_3000.product_plane.research.indicators.materialize import _compute_spec

    adjusted = result.copy()
    roll_seq = pd.to_numeric(adjusted["roll_seq"], errors="coerce").fillna(0).astype(int)
    rules_by_output = {rule.output_column: rule for rule in rules_for_indicator_profile(indicator_profile)}
    specs_by_output = {
        output_column: spec
        for spec in indicator_profile.indicators
        for output_column in spec.output_columns
    }
    reset_operation_keys = {"obv", "ad", "adosc", "pvt", "pvo"}
    for spec in indicator_profile.indicators:
        spec_rules = [rules_by_output[column] for column in spec.output_columns]
        if spec.operation_key not in reset_operation_keys or not any(rule.group.reset_on_roll for rule in spec_rules):
            continue
        recomputed_outputs = pd.DataFrame(index=adjusted.index, columns=list(spec.output_columns), dtype="object")
        for _, segment in adjusted.groupby(roll_seq, sort=False):
            computed = _compute_spec(segment.copy(), spec)
            for column in spec.output_columns:
                recomputed_outputs.loc[segment.index, column] = computed[column].reindex(segment.index)
        for column in spec.output_columns:
            adjusted[column] = recomputed_outputs[column]

    for column, rule in rules_by_output.items():
        if column not in adjusted.columns or rule.group.allow_cross_contract_window:
            continue
        spec = specs_by_output[column]
        if spec.operation_key in {"oi_change", "oi_roc"}:
            lag = max(1, int(spec.params_dict().get("length", 1)))
            same_epoch_lag = roll_seq == roll_seq.shift(lag)
            adjusted.loc[~same_epoch_lag.fillna(False), column] = pd.NA
            continue
        if spec.operation_key == "volume_oi_ratio":
            continue
        length = max(1, int(spec.params_dict().get("length", spec.warmup_bars)))
        window_crosses = roll_seq.rolling(window=length, min_periods=length).apply(
            lambda values: 0.0 if len(set(int(value) for value in values)) == 1 else 1.0,
            raw=False,
        )
        adjusted.loc[window_crosses.fillna(1.0).astype(bool), column] = pd.NA
    return adjusted


def _expected_base_formula_frame(frame: pd.DataFrame, indicator_profile: object) -> pd.DataFrame:
    from trading_advisor_3000.product_plane.research.indicators.materialize import _compute_spec

    rules_by_output = {rule.output_column: rule for rule in rules_for_indicator_profile(indicator_profile)}
    roll_seq = pd.to_numeric(frame["roll_seq"], errors="coerce").fillna(0).astype(int)
    offsets = pd.to_numeric(frame["cumulative_additive_offset"], errors="coerce").fillna(0.0)
    offset_by_epoch = {
        int(epoch): float(offsets.loc[roll_seq == int(epoch)].iloc[-1])
        for epoch in sorted(set(roll_seq.tolist()))
    }
    computed_segments: list[pd.DataFrame] = []
    for target_epoch in sorted(offset_by_epoch):
        target_index = frame.index[roll_seq == target_epoch]
        source = frame.loc[roll_seq <= target_epoch].copy()
        target_offset = offset_by_epoch[target_epoch]
        zero_anchor_frame = _formula_input_frame(source, price_space="zero_anchor")
        target_anchor_frame = _formula_input_frame(source, price_space="target_anchor", target_offset=target_offset)
        native_frame = _formula_input_frame(source, price_space="native")
        segment = target_anchor_frame.loc[target_index].copy()
        segment["instrument_id"] = frame.loc[target_index, "instrument_id"].to_numpy()
        segment["timeframe"] = frame.loc[target_index, "timeframe"].to_numpy()
        segment["ts"] = frame.loc[target_index, "ts"].to_numpy()
        segment["session_date"] = frame.loc[target_index, "session_date"].to_numpy()
        segment["session_open_ts"] = frame.loc[target_index, "session_open_ts"].to_numpy()
        segment["session_close_ts"] = frame.loc[target_index, "session_close_ts"].to_numpy()
        segment["roll_seq"] = roll_seq.loc[target_index].to_numpy()
        segment["cumulative_additive_offset"] = offsets.loc[target_index].to_numpy()
        compute_cache: dict[tuple[str, str], dict[str, pd.Series]] = {}

        for spec in indicator_profile.indicators:
            for group_id, output_columns in _outputs_by_group_for_spec(spec, rules_by_output).items():
                cache_key = (spec.indicator_id, group_id)
                if cache_key not in compute_cache:
                    group_frame = _formula_group_frame(
                        group_id,
                        zero_anchor_frame=zero_anchor_frame,
                        target_anchor_frame=target_anchor_frame,
                        native_frame=native_frame,
                    )
                    compute_cache[cache_key] = _compute_spec(group_frame, spec)
                for output_column in output_columns:
                    series = compute_cache[cache_key][output_column].reindex(target_anchor_frame.index)
                    if group_id == "price_level_post_transform":
                        series = series + target_offset
                    segment[output_column] = series.loc[target_index]
        computed_segments.append(segment)

    expected = pd.concat(computed_segments).sort_index()
    return _enforce_formula_base_roll_boundaries(expected, indicator_profile)


def _derived_reference_base_frame(frame: pd.DataFrame, expected_base: pd.DataFrame, indicator_columns: tuple[str, ...]) -> pd.DataFrame:
    offset = pd.to_numeric(frame["cumulative_additive_offset"], errors="coerce").fillna(0.0)
    data: dict[str, object] = {
        "instrument_id": frame["instrument_id"],
        "timeframe": frame["timeframe"],
        "ts": frame["ts"],
        "session_date": frame["session_date"],
        "session_open_ts": frame["session_open_ts"],
        "session_close_ts": frame["session_close_ts"],
        "active_contract_id": frame["active_contract_id"],
        "roll_epoch": pd.to_numeric(frame["roll_seq"], errors="coerce").fillna(0).astype(int),
        "_cf_offset": offset,
    }
    for column in ("open", "high", "low", "close"):
        zero_anchor = pd.to_numeric(frame[f"{column}0"], errors="coerce")
        data[f"_cf_{column}0"] = zero_anchor
        data[column] = zero_anchor + offset
    data["volume"] = pd.to_numeric(frame["native_volume"], errors="coerce")
    data["open_interest"] = pd.to_numeric(frame["native_open_interest"], errors="coerce")
    for column in indicator_columns:
        data[column] = expected_base[column] if column in expected_base.columns else pd.Series([pd.NA] * len(frame), index=frame.index)
    return pd.DataFrame(data, index=frame.index)


def _expected_derived_formula_frame(
    *,
    frame_key: tuple[str, str],
    frames_by_key: dict[tuple[str, str], pd.DataFrame],
    expected_base_by_key: dict[tuple[str, str], pd.DataFrame],
    indicator_columns: tuple[str, ...],
    derived_profile: object,
) -> pd.DataFrame:
    from trading_advisor_3000.product_plane.research.derived_indicators.materialize import _compute_derived_frame

    instrument_id, timeframe = frame_key
    base_frame = _derived_reference_base_frame(
        frames_by_key[frame_key],
        expected_base_by_key[frame_key],
        indicator_columns,
    )
    source_frames = {
        source_timeframe: _derived_reference_base_frame(source_frame, expected_base_by_key[(source_instrument, source_timeframe)], indicator_columns)
        for (source_instrument, source_timeframe), source_frame in frames_by_key.items()
        if source_instrument == instrument_id
    }
    return _compute_derived_frame(
        base_frame=base_frame,
        current_timeframe=timeframe,
        source_frames=source_frames,
        profile=derived_profile,
    )


def _required_formula_sample_columns(indicator_profile: object, derived_profile: object) -> tuple[tuple[str, str], ...]:
    return (
        *((("base", column) for column in indicator_profile.expected_output_columns())),
        *((("derived", column) for column in derived_profile.output_columns)),
    )


def _verify_formula_samples(
    *,
    run_id: str,
    input_rows: list[dict[str, object]],
    base_rows: list[dict[str, object]],
    derived_rows: list[dict[str, object]],
    indicator_profile: object,
    derived_profile: object,
) -> dict[str, object]:
    base_by_key = {_row_key(row): row for row in base_rows}
    derived_by_key = {_row_key(row): row for row in derived_rows}
    failures: list[dict[str, object]] = []
    checked = 0
    checked_columns: set[str] = set()

    series_frames = _series_dataframes(input_rows)
    frames_by_key = {
        (str(frame["instrument_id"].iloc[0]), str(frame["timeframe"].iloc[0])): frame
        for frame in series_frames
        if not frame.empty
    }
    indicator_columns = tuple(indicator_profile.expected_output_columns())
    expected_base_by_key = {
        key: _expected_base_formula_frame(frame, indicator_profile)
        for key, frame in frames_by_key.items()
    }
    expected_derived_by_key = {
        key: _expected_derived_formula_frame(
            frame_key=key,
            frames_by_key=frames_by_key,
            expected_base_by_key=expected_base_by_key,
            indicator_columns=indicator_columns,
            derived_profile=derived_profile,
        )
        for key in frames_by_key
    }
    required_columns = {f"{family}:{column}" for family, column in _required_formula_sample_columns(indicator_profile, derived_profile)}

    for frame_key, frame in frames_by_key.items():
        expected_frames = {
            "base": expected_base_by_key[frame_key],
            "derived": expected_derived_by_key[frame_key],
        }
        for output_family, column in _required_formula_sample_columns(indicator_profile, derived_profile):
            rows_by_key = base_by_key if output_family == "base" else derived_by_key
            expected_frame = expected_frames[output_family]
            if column not in expected_frame.columns:
                failures.append(
                    {
                        "formula": column,
                        "family": output_family,
                        "failure": "missing_expected_formula_column",
                    }
                )
                continue
            expected_series = expected_frame[column].reindex(frame.index)
            positions = _sample_positions_for_expected_column(
                frame=frame,
                rows_by_key=rows_by_key,
                expected=expected_series,
                column=column,
            )
            if not positions:
                failures.append(
                    {
                        "formula": column,
                        "family": output_family,
                        "failure": "no_sampleable_rows",
                    }
                )
                continue
            for position in positions:
                key = _frame_key_at(frame, position)
                row = rows_by_key.get(key)
                if row is None:
                    failures.append({"key": key, "formula": column, "failure": "missing_sidecar_row"})
                    continue
                expected = expected_series.iloc[position]
                checked += 1
                checked_columns.add(f"{output_family}:{column}")
                if not _value_matches(row.get(column), expected):
                    failures.append(
                        {
                            "key": key,
                            "formula": column,
                            "family": output_family,
                            "observed": _json_value(row.get(column)),
                            "expected": _json_value(expected),
                        }
                    )
                if len(failures) >= FORMULA_SAMPLE_MAX_FAILURES:
                    break
            if len(failures) >= FORMULA_SAMPLE_MAX_FAILURES:
                break
        if len(failures) >= FORMULA_SAMPLE_MAX_FAILURES:
            break

    missing_columns = sorted(required_columns - checked_columns)
    for column in missing_columns:
        failures.append({"formula": column, "failure": "required_formula_not_checked"})
    if checked == 0:
        failures.append({"failure": "no_formula_samples_checked"})
    return qc_observation(
        run_id=run_id,
        check_id="independent_formula_samples",
        check_group="formula_sample",
        severity="blocker",
        status="fail" if failures else "pass",
        entity_key="continuous_front_indicator_sidecars",
        observed_value={
            "checked": checked,
            "checked_columns": sorted(checked_columns),
            "checked_columns_count": len(checked_columns),
            "required_columns_count": len(required_columns),
            "failures": len(failures),
        },
        expected_value={"required_columns": sorted(required_columns), "failures": 0},
        sample_rows=failures[:FORMULA_SAMPLE_MAX_FAILURES],
    )


def _verify_pandas_ta_parity(
    *,
    run_id: str,
    rules: tuple[IndicatorRollRule, ...],
    base_rows: list[dict[str, object]],
    indicator_value_columns: tuple[str, ...],
) -> dict[str, object]:
    pandas_ta_outputs = sorted(
        rule.output_column
        for rule in rules
        if rule.output_family == "base" and rule.group.pandas_ta_allowed
    )
    missing_outputs = [
        column
        for column in pandas_ta_outputs
        if column not in indicator_value_columns or any(column not in row for row in base_rows)
    ]
    wrong_adapters = sorted(
        rule.output_column
        for rule in rules
        if rule.output_family == "base"
        and rule.group.pandas_ta_allowed
        and not rule.group.adapter_id.startswith("pandas_ta")
    )
    failures = [
        {"failure": "missing_pandas_ta_output", "column": column}
        for column in missing_outputs
    ] + [
        {"failure": "unexpected_non_pandas_ta_adapter", "column": column}
        for column in wrong_adapters
    ]
    if not pandas_ta_outputs:
        failures.append({"failure": "no_pandas_ta_outputs_declared"})
    return qc_observation(
        run_id=run_id,
        check_id="pandas_ta_rule_output_parity",
        check_group="pandas_ta_parity",
        severity="blocker",
        status="fail" if failures else "pass",
        entity_key="continuous_front_indicator_frames",
        observed_value={"pandas_ta_outputs": len(pandas_ta_outputs), "failures": len(failures)},
        expected_value={"failures": 0},
        sample_rows=failures[:50],
    )


def _verify_lineage(
    *,
    run_id: str,
    source_versions_digest: str,
    output_versions_digest: str,
    runtime_evidence: dict[str, object],
    input_rows: list[dict[str, object]],
    base_rows: list[dict[str, object]],
    derived_rows: list[dict[str, object]],
    indicator_value_columns: tuple[str, ...],
    derived_value_columns: tuple[str, ...],
) -> dict[str, object]:
    failures: list[dict[str, object]] = []
    if not source_versions_digest:
        failures.append({"failure": "missing_source_versions_digest"})
    if not output_versions_digest:
        failures.append({"failure": "missing_output_versions_digest"})
    required_runtime_fields = (
        "spark_app_id",
        "spark_event_log_path",
        "input_delta_versions_hash",
        "output_delta_versions_hash",
        "rule_set_hash",
        "adapter_bundle_hash",
        "formula_kernel_hash",
        "job_config_hash",
        "code_artifact_hash",
        "dependency_lock_hash",
        "created_by_pipeline",
    )
    for field in required_runtime_fields:
        if not runtime_evidence.get(field):
            failures.append({"failure": "missing_runtime_evidence_field", "field": field})
    if runtime_evidence.get("created_by_pipeline") != "spark_delta_governed":
        failures.append(
            {
                "failure": "invalid_created_by_pipeline",
                "observed": runtime_evidence.get("created_by_pipeline"),
                "expected": "spark_delta_governed",
            }
        )
    input_hashes = {str(row.get("input_front_row_hash") or "") for row in input_rows}
    base_hashes = {str(row.get("indicator_row_hash") or "") for row in base_rows}
    for row in input_rows:
        if not row.get("input_front_row_hash"):
            failures.append({"key": _row_key(row), "failure": "missing_input_row_hash"})
    for row in base_rows:
        if not row.get("source_input_row_hash") or str(row.get("source_input_row_hash")) not in input_hashes:
            failures.append({"key": _row_key(row), "failure": "invalid_base_input_hash"})
        if row.get("indicator_row_hash") != _row_hash(row, indicator_value_columns):
            failures.append({"key": _row_key(row), "failure": "invalid_base_row_hash"})
    for row in derived_rows:
        if not row.get("source_input_row_hash") or str(row.get("source_input_row_hash")) not in input_hashes:
            failures.append({"key": _row_key(row), "failure": "invalid_derived_input_hash"})
        if not row.get("source_base_indicator_row_hash") or str(row.get("source_base_indicator_row_hash")) not in base_hashes:
            failures.append({"key": _row_key(row), "failure": "invalid_derived_base_hash"})
        if row.get("derived_row_hash") != _row_hash(row, derived_value_columns):
            failures.append({"key": _row_key(row), "failure": "invalid_derived_row_hash"})
        if len(failures) >= 50:
            break
    return qc_observation(
        run_id=run_id,
        check_id="sidecar_lineage_hash_chain",
        check_group="lineage",
        severity="blocker",
        status="fail" if failures else "pass",
        entity_key="continuous_front_indicator_sidecars",
        observed_value=len(failures),
        expected_value=0,
        sample_rows=failures[:50],
    )


def _verify_required_qc_groups(*, run_id: str, qc_rows: list[dict[str, object]]) -> dict[str, object]:
    present = {str(row.get("check_group")) for row in qc_rows}
    missing = sorted(REQUIRED_ACCEPTANCE_QC_GROUPS - present)
    return qc_observation(
        run_id=run_id,
        check_id="required_acceptance_qc_groups_present",
        check_group="anti_bypass",
        severity="blocker",
        status="fail" if missing else "pass",
        entity_key="continuous_front_indicator_acceptance_report",
        observed_value={"missing": missing},
        expected_value="all_required_acceptance_qc_groups_present",
        sample_rows={"missing": missing},
    )


def run_continuous_front_indicator_pandas_job(
    *,
    materialized_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_set_version: str,
    output_dir: Path | None = None,
    rule_set_version: str = DEFAULT_RULE_SET_VERSION,
    run_id: str = "continuous_front_indicator_refresh",
    calculation_app_id: str = "",
    event_log_path: str = "",
) -> dict[str, object]:
    resolved_output_dir = output_dir or materialized_output_dir
    manifest = _load_dataset_manifest(materialized_output_dir=materialized_output_dir, dataset_version=dataset_version)
    if str(manifest.get("series_mode")) != "continuous_front":
        raise RuntimeError("continuous_front indicator job requires research dataset series_mode=continuous_front")

    indicator_profile = default_indicator_profile()
    derived_profile = current_derived_indicator_profile()
    rules = default_indicator_roll_rules(
        indicator_profile=indicator_profile,
        derived_profile=derived_profile,
        rule_set_version=rule_set_version,
    )
    rule_hash = rule_set_hash(rules)
    adapter_hash = adapter_bundle_hash(rules)
    roll_policy_version, adjustment_policy_version = _policy_versions(manifest)
    created_at = utc_now_iso()

    bar_views = load_research_bar_views(dataset_output_dir=materialized_output_dir, dataset_version=dataset_version)
    ladder_rows = load_adjustment_ladder_rows(dataset_output_dir=materialized_output_dir, dataset_version=dataset_version)
    input_rows = build_cf_indicator_input_rows(
        bar_views=bar_views,
        adjustment_ladder_rows=ladder_rows,
        dataset_version=dataset_version,
        source_canonical_version=str(manifest.get("source_table") or ""),
        roll_policy_version=roll_policy_version,
        adjustment_policy_version=adjustment_policy_version,
        created_at_utc=created_at,
    )
    input_rows_by_key = {
        (str(row["instrument_id"]), str(row["timeframe"]), str(row["ts"]).replace("+00:00", "Z")): row
        for row in input_rows
    }
    indicator_value_columns = indicator_profile.expected_output_columns()
    derived_value_columns = derived_profile.output_columns
    from trading_advisor_3000.product_plane.research.derived_indicators import build_derived_indicator_frames
    from trading_advisor_3000.product_plane.research.indicators import build_indicator_frames

    base_frame_rows = build_indicator_frames(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        bar_views=bar_views,
        series_mode="continuous_front",
        profile=indicator_profile,
        adjustment_ladder_rows=ladder_rows,
    )
    derived_frame_rows = build_derived_indicator_frames(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_set_version,
        bar_views=bar_views,
        indicator_rows=base_frame_rows,
        series_mode="continuous_front",
        profile=derived_profile,
        adjustment_ladder_rows=ladder_rows,
    )
    base_rows = _base_sidecar_rows(
        base_rows=[row.to_dict() for row in base_frame_rows],
        input_rows_by_key=input_rows_by_key,
        dataset_version=dataset_version,
        roll_policy_version=roll_policy_version,
        adjustment_policy_version=adjustment_policy_version,
        indicator_set_version=indicator_set_version,
        rule_set_version=rule_set_version,
        adapter_hash=adapter_hash,
        value_columns=indicator_value_columns,
        max_cross_contract_window_bars=_max_strict_roll_window_bars(rules, output_family="base"),
        created_at_utc=created_at,
    )
    base_rows_by_key = {
        (str(row["instrument_id"]), str(row["timeframe"]), str(row["ts"]).replace("+00:00", "Z")): row
        for row in base_rows
    }
    derived_rows = _derived_sidecar_rows(
        derived_rows=[row.to_dict() for row in derived_frame_rows],
        base_rows_by_key=base_rows_by_key,
        input_rows_by_key=input_rows_by_key,
        dataset_version=dataset_version,
        roll_policy_version=roll_policy_version,
        adjustment_policy_version=adjustment_policy_version,
        indicator_set_version=indicator_set_version,
        derived_set_version=derived_set_version,
        rule_set_version=rule_set_version,
        adapter_hash=adapter_hash,
        value_columns=derived_value_columns,
        max_cross_contract_window_bars=_max_strict_roll_window_bars(rules, output_family="derived"),
        created_at_utc=created_at,
    )
    source_versions_json, source_versions_digest = _source_versions_hash(
        {
            "research_datasets": materialized_output_dir / "research_datasets.delta",
            "research_bar_views": materialized_output_dir / "research_bar_views.delta",
            "continuous_front_adjustment_ladder": materialized_output_dir / "continuous_front_adjustment_ladder.delta",
        }
    )
    output_versions_payload = {
        "cf_indicator_input_frame": len(input_rows),
        "indicator_roll_rules": len(rules),
        "continuous_front_indicator_frames": len(base_rows),
        "continuous_front_derived_indicator_frames": len(derived_rows),
    }
    output_versions_json = json.dumps(output_versions_payload, ensure_ascii=False, sort_keys=True)
    output_versions_digest = stable_hash(output_versions_payload)
    formula_kernel_hash = stable_hash({"rules": rule_hash, "adapters": adapter_hash})
    job_config_hash = stable_hash(
        {
            "dataset_version": dataset_version,
            "indicator_set_version": indicator_set_version,
            "derived_set_version": derived_set_version,
            "rule_set_version": rule_set_version,
        }
    )
    spark_app_id = str(calculation_app_id or "")
    spark_event_log_path = str(event_log_path or "")
    code_artifact_hash = stable_hash({"module": __name__, "version": "v1"})
    dependency_lock_hash = stable_hash(
        {
            "base_runtime": "pandas_ta_classic",
            "derived_runtime": "pandas",
            "storage_runtime": "delta",
            "adapter_bundle_hash": adapter_hash,
        }
    )
    runtime_evidence = {
        "spark_app_id": spark_app_id,
        "spark_event_log_path": spark_event_log_path,
        "input_delta_versions_hash": source_versions_digest,
        "output_delta_versions_hash": output_versions_digest,
        "rule_set_hash": rule_hash,
        "adapter_bundle_hash": adapter_hash,
        "formula_kernel_hash": formula_kernel_hash,
        "job_config_hash": job_config_hash,
        "code_artifact_hash": code_artifact_hash,
        "dependency_lock_hash": dependency_lock_hash,
        "created_by_pipeline": "spark_delta_governed",
    }
    qc_rows = [
        verify_input_projection_identity(run_id=run_id, rows=input_rows),
        verify_rule_coverage(
            run_id=run_id,
            output_columns=set(indicator_value_columns),
            rules=rules,
            output_family="base",
        ),
        verify_rule_coverage(
            run_id=run_id,
            output_columns=set(derived_value_columns),
            rules=rules,
            output_family="derived",
        ),
        qc_observation(
            run_id=run_id,
            check_id="sidecar_base_row_alignment",
            check_group="schema",
            severity="blocker",
            status="pass" if len(base_rows) == len(input_rows) else "fail",
            entity_key="continuous_front_indicator_frames",
            observed_value=len(base_rows),
            expected_value=len(input_rows),
        ),
        qc_observation(
            run_id=run_id,
            check_id="sidecar_derived_row_alignment",
            check_group="schema",
            severity="blocker",
            status="pass" if len(derived_rows) == len(input_rows) else "fail",
            entity_key="continuous_front_derived_indicator_frames",
            observed_value=len(derived_rows),
            expected_value=len(input_rows),
        ),
        qc_observation(
            run_id=run_id,
            check_id="registered_adapter_authorization",
            check_group="adapter_authorization",
            severity="blocker",
            status="pass",
            entity_key=rule_set_version,
            observed_value=sorted({rule.group.adapter_id for rule in rules}),
            expected_value="registered adapters only",
        ),
    ]
    qc_rows.extend(
        [
            _verify_prefix_invariance(
                run_id=run_id,
                bar_views=bar_views,
                ladder_rows=ladder_rows,
                dataset_version=dataset_version,
                source_canonical_version=str(manifest.get("source_table") or ""),
                roll_policy_version=roll_policy_version,
                adjustment_policy_version=adjustment_policy_version,
                indicator_set_version=indicator_set_version,
                derived_set_version=derived_set_version,
                rule_set_version=rule_set_version,
                adapter_hash=adapter_hash,
                indicator_profile=indicator_profile,
                derived_profile=derived_profile,
                base_rows=base_rows,
                derived_rows=derived_rows,
                indicator_value_columns=indicator_value_columns,
                derived_value_columns=derived_value_columns,
                max_base_cross_contract_window_bars=_max_strict_roll_window_bars(rules, output_family="base"),
                max_derived_cross_contract_window_bars=_max_strict_roll_window_bars(rules, output_family="derived"),
                build_indicator_frames_fn=build_indicator_frames,
                build_derived_indicator_frames_fn=build_derived_indicator_frames,
                created_at_utc=created_at,
            ),
            _verify_formula_samples(
                run_id=run_id,
                input_rows=input_rows,
                base_rows=base_rows,
                derived_rows=derived_rows,
                indicator_profile=indicator_profile,
                derived_profile=derived_profile,
            ),
            _verify_pandas_ta_parity(
                run_id=run_id,
                rules=rules,
                base_rows=base_rows,
                indicator_value_columns=indicator_value_columns,
            ),
            _verify_lineage(
                run_id=run_id,
                source_versions_digest=source_versions_digest,
                output_versions_digest=output_versions_digest,
                runtime_evidence=runtime_evidence,
                input_rows=input_rows,
                base_rows=base_rows,
                derived_rows=derived_rows,
                indicator_value_columns=indicator_value_columns,
                derived_value_columns=derived_value_columns,
            ),
        ]
    )
    qc_rows.append(_verify_required_qc_groups(run_id=run_id, qc_rows=qc_rows))
    publish_status = publish_status_from_qc(qc_rows)

    manifest_rows = [
        {
            "run_id": run_id,
            "dataset_version": dataset_version,
            "roll_policy_version": roll_policy_version,
            "adjustment_policy_version": adjustment_policy_version,
            "indicator_set_version": indicator_set_version,
            "derived_set_version": derived_set_version,
            "rule_set_version": rule_set_version,
            "input_delta_versions_json": source_versions_json,
            "input_delta_versions_hash": source_versions_digest,
            "output_delta_versions_json": output_versions_json,
            "output_delta_versions_hash": output_versions_digest,
            "rule_set_hash": rule_hash,
            "adapter_bundle_hash": adapter_hash,
            "formula_kernel_hash": formula_kernel_hash,
            "job_config_hash": job_config_hash,
            "spark_app_id": spark_app_id,
            "spark_event_log_path": spark_event_log_path,
            "calculation_app_id": calculation_app_id,
            "calculation_event_log_path": spark_event_log_path,
            "code_artifact_hash": code_artifact_hash,
            "dependency_lock_hash": dependency_lock_hash,
            "container_image_digest": None,
            "created_by_pipeline": "spark_delta_governed",
            "calculation_engines_json": {
                "storage": "delta",
                "orchestration": "dagster_asset_job",
                "adapter_orchestrator": "spark_delta_governed",
                "base_runtime": "pandas_ta_classic",
                "derived_runtime": "pandas",
                "base_adapters": sorted({rule.group.adapter_id for rule in rules if rule.output_family == "base"}),
                "derived_adapters": sorted({rule.group.adapter_id for rule in rules if rule.output_family == "derived"}),
            },
            "publish_status": publish_status,
            "created_at_utc": created_at,
        }
    ]
    acceptance_rows = [
        {
            "run_id": run_id,
            "dataset_version": dataset_version,
            "rule_set_version": rule_set_version,
            "blocker_fail_count": sum(
                1
                for row in qc_rows
                if str(row.get("severity")) == "blocker" and str(row.get("status")) != "pass"
            ),
            "schema_fail_count": fail_count(qc_rows, "schema"),
            "adapter_authorization_fail_count": fail_count(qc_rows, "adapter_authorization"),
            "prefix_invariance_fail_count": fail_count(qc_rows, "prefix_invariance"),
            "formula_sample_fail_count": fail_count(qc_rows, "formula_sample"),
            "pandas_ta_parity_fail_count": fail_count(qc_rows, "pandas_ta_parity"),
            "lineage_fail_count": fail_count(qc_rows, "lineage"),
            "publish_status": publish_status,
            "created_at_utc": created_at,
        }
    ]

    contract = continuous_front_indicator_store_contract(
        indicator_profile=indicator_profile,
        derived_profile=derived_profile,
    )
    output_paths = {
        "cf_indicator_input_frame": _write_table(
            output_dir=resolved_output_dir,
            table_name="cf_indicator_input_frame",
            rows=input_rows,
            contract=contract,
        ),
        "indicator_roll_rules": _write_table(
            output_dir=resolved_output_dir,
            table_name="indicator_roll_rules",
            rows=rules_to_rows(rules, created_at_utc=created_at),
            contract=contract,
        ),
        "continuous_front_indicator_frames": _write_table(
            output_dir=resolved_output_dir,
            table_name="continuous_front_indicator_frames",
            rows=base_rows,
            contract=contract,
        ),
        "continuous_front_derived_indicator_frames": _write_table(
            output_dir=resolved_output_dir,
            table_name="continuous_front_derived_indicator_frames",
            rows=derived_rows,
            contract=contract,
        ),
        "continuous_front_indicator_qc_observations": _write_table(
            output_dir=resolved_output_dir,
            table_name="continuous_front_indicator_qc_observations",
            rows=qc_rows,
            contract=contract,
        ),
        "continuous_front_indicator_run_manifest": _write_table(
            output_dir=resolved_output_dir,
            table_name="continuous_front_indicator_run_manifest",
            rows=manifest_rows,
            contract=contract,
        ),
        "continuous_front_indicator_acceptance_report": _write_table(
            output_dir=resolved_output_dir,
            table_name="continuous_front_indicator_acceptance_report",
            rows=acceptance_rows,
            contract=contract,
        ),
    }
    rows_by_table = {
        table_name: count_delta_table_rows(Path(path))
        for table_name, path in output_paths.items()
    }
    return {
        "success": publish_status == "accepted",
        "status": "PASS" if publish_status == "accepted" else "QUARANTINED",
        "publish_status": publish_status,
        "run_id": run_id,
        "dataset_version": dataset_version,
        "rule_set_version": rule_set_version,
        "rule_set_hash": rule_hash,
        "adapter_bundle_hash": adapter_hash,
        "output_paths": output_paths,
        "rows_by_table": rows_by_table,
        "qc_rows": qc_rows,
        "acceptance_report": acceptance_rows[0],
        "delta_manifest": {name: contract[name] for name in CF_INDICATOR_TABLES},
    }
