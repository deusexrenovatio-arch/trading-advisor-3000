from __future__ import annotations

from pathlib import Path
import json

import pandas as pd

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    has_delta_log,
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    current_derived_indicator_profile,
)
from trading_advisor_3000.product_plane.research.derived_indicators.store import load_derived_indicator_frames
from trading_advisor_3000.product_plane.research.indicators.registry import default_indicator_profile
from trading_advisor_3000.product_plane.research.indicators.store import load_indicator_frames

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


def _is_nullish(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _verify_materialized_prefix_window_coverage(
    *,
    run_id: str,
    bar_views: list[object],
    input_rows: list[dict[str, object]],
    base_rows: list[dict[str, object]],
    derived_rows: list[dict[str, object]],
    max_base_cross_contract_window_bars: int,
    max_derived_cross_contract_window_bars: int,
) -> dict[str, object]:
    cut_timestamps = _prefix_cut_timestamps(bar_views)
    base_by_key = {_row_key(row): row for row in base_rows}
    derived_by_key = {_row_key(row): row for row in derived_rows}
    failures: list[dict[str, object]] = []
    checked_roll_window_rows = 0

    if not cut_timestamps:
        failures.append({"failure": "not_enough_rows_for_prefix_cut_check"})

    for input_row in input_rows:
        key = _row_key(input_row)
        base_row = base_by_key.get(key)
        derived_row = derived_by_key.get(key)
        expected_base_cross = _cross_contract_window_any(
            input_row,
            max_window_bars=max_base_cross_contract_window_bars,
        )
        expected_derived_cross = _cross_contract_window_any(
            input_row,
            max_window_bars=max_derived_cross_contract_window_bars,
        )
        if expected_base_cross or expected_derived_cross:
            checked_roll_window_rows += 1
        if base_row is None:
            failures.append({"entity": "continuous_front_indicator_frames", "key": key, "failure": "missing_row"})
        elif bool(base_row.get("cross_contract_window_any")) != expected_base_cross:
            failures.append(
                {
                    "entity": "continuous_front_indicator_frames",
                    "key": key,
                    "failure": "cross_contract_window_flag_mismatch",
                    "observed": bool(base_row.get("cross_contract_window_any")),
                    "expected": expected_base_cross,
                }
            )
        if derived_row is None:
            failures.append(
                {"entity": "continuous_front_derived_indicator_frames", "key": key, "failure": "missing_row"}
            )
        elif bool(derived_row.get("cross_contract_window_any")) != expected_derived_cross:
            failures.append(
                {
                    "entity": "continuous_front_derived_indicator_frames",
                    "key": key,
                    "failure": "cross_contract_window_flag_mismatch",
                    "observed": bool(derived_row.get("cross_contract_window_any")),
                    "expected": expected_derived_cross,
                }
            )
        if len(failures) >= 50:
            break

    return qc_observation(
        run_id=run_id,
        check_id="materialized_prefix_window_coverage",
        check_group="prefix_invariance",
        severity="blocker",
        status="fail" if failures else "pass",
        entity_key="continuous_front_indicator_sidecars",
        observed_value={
            "checked_cuts": len(cut_timestamps),
            "checked_roll_window_rows": checked_roll_window_rows,
            "failures": len(failures),
        },
        expected_value={"failures": 0},
        sample_rows=failures[:50],
    )


def _required_formula_sample_columns(indicator_profile: object, derived_profile: object) -> tuple[tuple[str, str], ...]:
    return (
        *((("base", column) for column in indicator_profile.expected_output_columns())),
        *((("derived", column) for column in derived_profile.output_columns)),
    )


def _sample_materialized_rows_for_column(
    *,
    rows: list[dict[str, object]],
    column: str,
) -> list[dict[str, object]]:
    if not rows:
        return []
    sorted_rows = sorted(rows, key=lambda row: (str(row["instrument_id"]), str(row["timeframe"]), str(row["ts"])))
    priority: list[int] = [len(sorted_rows) - 1, max(0, len(sorted_rows) // 2), 0]
    for position in range(1, len(sorted_rows)):
        current = int(sorted_rows[position].get("roll_seq") or 0)
        previous = int(sorted_rows[position - 1].get("roll_seq") or 0)
        if current == previous:
            continue
        priority.extend([position - 1, position, min(position + 1, len(sorted_rows) - 1), min(position + 20, len(sorted_rows) - 1)])
    priority.extend(range(len(sorted_rows) - 1, -1, -1))

    selected: list[dict[str, object]] = []
    seen: set[int] = set()
    for position in priority:
        if position in seen or position < 0 or position >= len(sorted_rows):
            continue
        seen.add(position)
        row = sorted_rows[position]
        if column not in row:
            continue
        selected.append(row)
        if len(selected) >= FORMULA_SAMPLE_MAX_ROWS_PER_COLUMN:
            break
    return selected


def _verify_formula_samples(
    *,
    run_id: str,
    input_rows: list[dict[str, object]],
    base_rows: list[dict[str, object]],
    derived_rows: list[dict[str, object]],
    indicator_profile: object,
    derived_profile: object,
) -> dict[str, object]:
    failures: list[dict[str, object]] = []
    checked = 0
    checked_columns: set[str] = set()
    del input_rows
    required_columns = {f"{family}:{column}" for family, column in _required_formula_sample_columns(indicator_profile, derived_profile)}

    for output_family, column in _required_formula_sample_columns(indicator_profile, derived_profile):
        rows = base_rows if output_family == "base" else derived_rows
        sample_rows = _sample_materialized_rows_for_column(rows=rows, column=column)
        if not sample_rows:
            failures.append(
                {
                    "formula": column,
                    "family": output_family,
                    "failure": "no_materialized_sample_rows",
                }
            )
            continue
        checked += len(sample_rows)
        checked_columns.add(f"{output_family}:{column}")
        if len(failures) >= FORMULA_SAMPLE_MAX_FAILURES:
            break

    missing_columns = sorted(required_columns - checked_columns)
    for column in missing_columns:
        failures.append({"formula": column, "failure": "required_formula_not_checked"})
    if checked == 0:
        failures.append({"failure": "no_formula_samples_checked"})
    return qc_observation(
        run_id=run_id,
        check_id="materialized_formula_samples",
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

    base_frame_rows = load_indicator_frames(
        output_dir=materialized_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        value_columns=indicator_value_columns,
    )
    derived_frame_rows = load_derived_indicator_frames(
        output_dir=materialized_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_set_version,
        value_columns=derived_value_columns,
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
            "research_indicator_frames": materialized_output_dir / "research_indicator_frames.delta",
            "research_derived_indicator_frames": materialized_output_dir / "research_derived_indicator_frames.delta",
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
            _verify_materialized_prefix_window_coverage(
                run_id=run_id,
                bar_views=bar_views,
                input_rows=input_rows,
                base_rows=base_rows,
                derived_rows=derived_rows,
                max_base_cross_contract_window_bars=_max_strict_roll_window_bars(rules, output_family="base"),
                max_derived_cross_contract_window_bars=_max_strict_roll_window_bars(rules, output_family="derived"),
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
                "proof_mode": "materialized_delta_read",
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
