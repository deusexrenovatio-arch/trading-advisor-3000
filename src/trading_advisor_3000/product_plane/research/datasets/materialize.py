from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    delta_equals_predicate,
    read_delta_table_rows,
    replace_delta_table_rows,
)

from .manifest import ResearchDatasetManifest
from .splitters import (
    DatasetWindow,
    HoldoutSplitConfig,
    WalkForwardSplitConfig,
    build_holdout_window,
    build_walk_forward_windows,
    split_config_to_dict,
)
from .views import ResearchBarView, build_research_bar_views


def research_dataset_store_contract() -> dict[str, dict[str, object]]:
    return {
        "research_datasets": {
            "format": "delta",
            "partition_by": ["dataset_version"],
            "constraints": ["unique(dataset_version)"],
            "columns": {
                "dataset_version": "string",
                "dataset_name": "string",
                "source_table": "string",
                "series_mode": "string",
                "universe_id": "string",
                "timeframes_json": "json",
                "base_timeframe": "string",
                "start_ts": "timestamp",
                "end_ts": "timestamp",
                "warmup_bars": "int",
                "split_method": "string",
                "split_params_json": "json",
                "bars_hash": "string",
                "created_at": "timestamp",
                "code_version": "string",
                "notes_json": "json",
                "source_tables": "json",
                "continuous_front_policy": "json",
                "lineage_key": "string",
            },
        },
        "research_instrument_tree": {
            "format": "delta",
            "partition_by": ["dataset_version", "asset_group"],
            "constraints": ["unique(dataset_version, instrument_id)"],
            "columns": {
                "dataset_version": "string",
                "universe_id": "string",
                "asset_class": "string",
                "asset_group": "string",
                "internal_id": "string",
                "instrument_id": "string",
                "source_instrument_id": "string",
                "contract_ids_json": "json",
                "active_contract_ids_json": "json",
                "timeframes_json": "json",
                "row_count": "bigint",
                "first_ts": "timestamp",
                "last_ts": "timestamp",
                "source_bars_hash": "string",
                "lineage_key": "string",
                "created_at": "timestamp",
            },
        },
        "research_bar_views": {
            "format": "delta",
            "partition_by": ["dataset_version", "instrument_id", "timeframe"],
            "constraints": ["unique(dataset_version, contract_id, timeframe, ts)"],
            "columns": {
                "dataset_version": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "ts": "timestamp",
                "open": "double",
                "high": "double",
                "low": "double",
                "close": "double",
                "volume": "bigint",
                "open_interest": "bigint",
                "session_date": "date",
                "session_open_ts": "timestamp",
                "session_close_ts": "timestamp",
                "active_contract_id": "string",
                "series_id": "string",
                "series_mode": "string",
                "roll_epoch": "int",
                "roll_event_id": "string",
                "is_roll_bar": "boolean",
                "is_first_bar_after_roll": "boolean",
                "bars_since_roll": "int",
                "price_space": "string",
                "native_open": "double",
                "native_high": "double",
                "native_low": "double",
                "native_close": "double",
                "continuous_open": "double",
                "continuous_high": "double",
                "continuous_low": "double",
                "continuous_close": "double",
                "execution_open": "double",
                "execution_high": "double",
                "execution_low": "double",
                "execution_close": "double",
                "previous_contract_id": "string",
                "candidate_contract_id": "string",
                "adjustment_mode": "string",
                "cumulative_additive_offset": "double",
                "ratio_factor": "double",
                "ret_1": "double",
                "log_ret_1": "double",
                "true_range": "double",
                "hl_range": "double",
                "oc_range": "double",
                "bar_index": "int",
                "slice_role": "string",
            },
        },
    }

# Dataset row-object reloaders are intentionally not part of the active route.
def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash_rows(rows: list[CanonicalBar]) -> str:
    payload = [
        {
            "contract_id": row.contract_id,
            "instrument_id": row.instrument_id,
            "timeframe": row.timeframe.value,
            "ts": row.ts,
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "volume": row.volume,
            "open_interest": row.open_interest,
        }
        for row in rows
    ]
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _stable_hash_views(rows: list[ResearchBarView]) -> str:
    payload = [
        {
            "contract_id": row.contract_id,
            "instrument_id": row.instrument_id,
            "timeframe": row.timeframe,
            "ts": row.ts,
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "volume": row.volume,
            "open_interest": row.open_interest,
            "active_contract_id": row.active_contract_id,
            "series_id": row.series_id,
            "series_mode": row.series_mode,
            "roll_epoch": row.roll_epoch,
            "roll_event_id": row.roll_event_id,
            "is_roll_bar": row.is_roll_bar,
            "is_first_bar_after_roll": row.is_first_bar_after_roll,
            "bars_since_roll": row.bars_since_roll,
            "price_space": row.price_space,
            "native_close": row.native_close if row.native_close is not None else row.close,
            "continuous_close": row.continuous_close if row.continuous_close is not None else row.close,
            "execution_close": row.execution_close if row.execution_close is not None else row.close,
            "cumulative_additive_offset": row.cumulative_additive_offset,
            }
        for row in rows
    ]
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _timeframe_sort_key(value: str) -> tuple[int, str]:
    order = {
        "1m": 1,
        "5m": 5,
        "10m": 10,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "4h": 240,
        "1d": 1440,
        "1w": 10080,
    }
    return order.get(value, 1_000_000), value


def _instrument_metadata(instrument_id: str) -> dict[str, str]:
    normalized = instrument_id.strip().upper()
    bare = normalized[4:] if normalized.startswith("FUT_") else normalized
    commodity = {"BR", "NG", "GOLD", "SILV", "PLD", "PLT", "WHEAT"}
    index = {"RTS", "MIX", "MXI", "NASD", "SPYF", "RGBI"}
    if bare in commodity:
        asset_group = "commodity"
    elif bare in index:
        asset_group = "index"
    else:
        asset_group = "unknown"
    return {
        "asset_class": "futures",
        "asset_group": asset_group,
        "internal_id": normalized if normalized.startswith("FUT_") else f"FUT_{bare}",
    }


def build_research_instrument_tree(
    *,
    manifest: ResearchDatasetManifest,
    selected_views: list[ResearchBarView],
) -> list[dict[str, object]]:
    created_at = _utc_now_iso()
    grouped: dict[str, list[ResearchBarView]] = {}
    for row in selected_views:
        grouped.setdefault(row.instrument_id, []).append(row)

    rows: list[dict[str, object]] = []
    for instrument_id, instrument_rows in sorted(grouped.items()):
        ordered = sorted(instrument_rows, key=lambda item: (item.timeframe, item.ts, item.contract_id))
        metadata = _instrument_metadata(instrument_id)
        contract_ids = sorted({row.contract_id for row in ordered})
        active_contract_ids = sorted({row.active_contract_id for row in ordered if row.active_contract_id})
        timeframes = sorted({row.timeframe for row in ordered}, key=_timeframe_sort_key)
        lineage_key = hashlib.sha256(
            "|".join(
                [
                    manifest.lineage_key(),
                    str(metadata["internal_id"]),
                    *contract_ids,
                    *timeframes,
                ]
            ).encode("utf-8")
        ).hexdigest()[:16].upper()
        rows.append(
            {
                "dataset_version": manifest.dataset_version,
                "universe_id": manifest.universe_id,
                "asset_class": metadata["asset_class"],
                "asset_group": metadata["asset_group"],
                "internal_id": metadata["internal_id"],
                "instrument_id": instrument_id,
                "source_instrument_id": instrument_id,
                "contract_ids_json": contract_ids,
                "active_contract_ids_json": active_contract_ids,
                "timeframes_json": timeframes,
                "row_count": len(ordered),
                "first_ts": min(row.ts for row in ordered),
                "last_ts": max(row.ts for row in ordered),
                "source_bars_hash": _stable_hash_views(ordered),
                "lineage_key": lineage_key,
                "created_at": created_at,
            }
        )
    return rows


def _build_split_payload(
    *,
    selected_views: list[ResearchBarView],
    split_method: str,
    split_config: HoldoutSplitConfig | WalkForwardSplitConfig | None,
) -> dict[str, object]:
    analysis_rows = sorted(
        (row for row in selected_views if row.slice_role == "analysis"),
        key=lambda row: (row.ts, row.contract_id, row.timeframe, row.instrument_id),
    )
    analysis_count = len(analysis_rows)
    payload: dict[str, object] = {
        "analysis_row_count": analysis_count,
        "config": split_config_to_dict(split_config),
        "windows": [],
    }
    if analysis_count == 0:
        return payload

    def _row_ts(position: int) -> str:
        return analysis_rows[max(0, min(position, analysis_count - 1))].ts

    def _window_payload(window: DatasetWindow) -> dict[str, object]:
        return {
            **window.to_dict(),
            "analysis_start_ts": _row_ts(window.train_start),
            "analysis_end_ts": _row_ts(window.test_stop - 1),
            "train_start_ts": _row_ts(window.train_start),
            "train_end_ts": _row_ts(window.train_stop - 1),
            "test_start_ts": _row_ts(window.test_start),
            "test_end_ts": _row_ts(window.test_stop - 1),
        }

    if split_method == "full":
        window = DatasetWindow(
            window_id="full-01",
            train_start=0,
            train_stop=analysis_count,
            test_start=0,
            test_stop=analysis_count,
        )
        payload["windows"] = [
            _window_payload(window)
        ]
        return payload

    if split_method == "holdout":
        window = build_holdout_window(analysis_count, split_config if isinstance(split_config, HoldoutSplitConfig) else None)
        payload["windows"] = [_window_payload(window)]
        return payload

    config = split_config if isinstance(split_config, WalkForwardSplitConfig) else WalkForwardSplitConfig(
        train_size=max(1, math.floor(analysis_count * 0.6)),
        test_size=max(1, math.floor(analysis_count * 0.2)),
        step_size=max(1, math.floor(analysis_count * 0.2)),
    )
    payload["windows"] = [
        _window_payload(window)
        for window in build_walk_forward_windows(analysis_count, config)
    ]
    return payload


def build_research_dataset_manifest(
    *,
    manifest_seed: ResearchDatasetManifest,
    bars: list[CanonicalBar],
    selected_views: list[ResearchBarView],
    split_config: HoldoutSplitConfig | WalkForwardSplitConfig | None = None,
) -> ResearchDatasetManifest:
    selected_keys = {(view.contract_id, view.timeframe, view.ts) for view in selected_views}
    selected_bars = [
        row
        for row in bars
        if (row.contract_id, row.timeframe.value, row.ts) in selected_keys
    ]
    split_payload = _build_split_payload(
        selected_views=selected_views,
        split_method=manifest_seed.split_method,
        split_config=split_config,
    )
    notes = {
        **manifest_seed.notes,
        "lineage_key": manifest_seed.lineage_key(),
        "view_row_count": len(selected_views),
        "analysis_row_count": split_payload.get("analysis_row_count", 0),
        "split_manifests": split_payload.get("windows", []),
    }
    return ResearchDatasetManifest(
        dataset_version=manifest_seed.dataset_version,
        dataset_name=manifest_seed.dataset_name,
        source_table=manifest_seed.source_table,
        universe_id=manifest_seed.universe_id,
        timeframes=manifest_seed.timeframes,
        base_timeframe=manifest_seed.base_timeframe,
        start_ts=manifest_seed.start_ts,
        end_ts=manifest_seed.end_ts,
        series_mode=manifest_seed.series_mode,
        split_method=manifest_seed.split_method,
        warmup_bars=manifest_seed.warmup_bars,
        source_tables=manifest_seed.source_tables,
        continuous_front_policy=manifest_seed.continuous_front_policy,
        split_params={**split_payload, "series_mode": manifest_seed.series_mode},
        bars_hash=_stable_hash_rows(selected_bars),
        created_at=_utc_now_iso(),
        code_version=manifest_seed.code_version,
        notes=notes,
    )


def materialize_research_dataset(
    *,
    manifest_seed: ResearchDatasetManifest,
    bars: list[CanonicalBar],
    session_calendar: list[SessionCalendarEntry],
    roll_map: list[RollMapEntry],
    output_dir: Path,
    split_config: HoldoutSplitConfig | WalkForwardSplitConfig | None = None,
) -> dict[str, object]:
    contract = research_dataset_store_contract()
    output_dir.mkdir(parents=True, exist_ok=True)
    datasets_path = output_dir / "research_datasets.delta"
    instrument_tree_path = output_dir / "research_instrument_tree.delta"
    bar_views_path = output_dir / "research_bar_views.delta"

    selected_views = build_research_bar_views(
        dataset_version=manifest_seed.dataset_version,
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        manifest=manifest_seed,
    )
    manifest = build_research_dataset_manifest(
        manifest_seed=manifest_seed,
        bars=bars,
        selected_views=selected_views,
        split_config=split_config,
    )
    instrument_tree_rows = build_research_instrument_tree(
        manifest=manifest,
        selected_views=selected_views,
    )

    dataset_predicate = delta_equals_predicate({"dataset_version": manifest.dataset_version})
    replace_delta_table_rows(
        table_path=datasets_path,
        rows=[manifest.to_dict()],
        columns=contract["research_datasets"]["columns"],
        predicate=dataset_predicate,
    )
    replace_delta_table_rows(
        table_path=instrument_tree_path,
        rows=instrument_tree_rows,
        columns=contract["research_instrument_tree"]["columns"],
        predicate=dataset_predicate,
    )
    replace_delta_table_rows(
        table_path=bar_views_path,
        rows=[row.to_dict() for row in selected_views],
        columns=contract["research_bar_views"]["columns"],
        predicate=dataset_predicate,
    )
    return {
        "dataset_manifest": manifest.to_dict(),
        "instrument_tree_count": len(instrument_tree_rows),
        "bar_view_count": len(selected_views),
        "output_paths": {
            "research_datasets": datasets_path.as_posix(),
            "research_instrument_tree": instrument_tree_path.as_posix(),
            "research_bar_views": bar_views_path.as_posix(),
        },
        "delta_manifest": contract,
    }


def _decode_json_manifest_field(value: object) -> object:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _normalize_loaded_manifest_row(row: dict[str, object]) -> dict[str, object]:
    payload = dict(row)
    for field_name in (
        "timeframes_json",
        "split_params_json",
        "notes_json",
        "source_tables",
        "continuous_front_policy",
    ):
        if field_name in payload:
            payload[field_name] = _decode_json_manifest_field(payload[field_name])
    return payload


def load_materialized_research_dataset(*, output_dir: Path, dataset_version: str) -> dict[str, object]:
    datasets_path = output_dir / "research_datasets.delta"
    instrument_tree_path = output_dir / "research_instrument_tree.delta"
    bar_views_path = output_dir / "research_bar_views.delta"
    manifests = read_delta_table_rows(datasets_path)
    instrument_rows = read_delta_table_rows(instrument_tree_path) if (instrument_tree_path / "_delta_log").exists() else []
    bar_view_rows = read_delta_table_rows(bar_views_path)
    manifest_rows = [row for row in manifests if row.get("dataset_version") == dataset_version]
    if not manifest_rows:
        raise KeyError(f"dataset_version not found: {dataset_version}")
    return {
        "dataset_manifest": _normalize_loaded_manifest_row(manifest_rows[0]),
        "instrument_tree": [
            row
            for row in instrument_rows
            if row.get("dataset_version") == dataset_version
        ],
        "bar_views": [
            ResearchBarView.from_dict(row)
            for row in bar_view_rows
            if row.get("dataset_version") == dataset_version
        ],
    }
