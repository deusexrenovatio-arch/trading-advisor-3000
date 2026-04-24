from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows

from .manifest import ResearchDatasetManifest
from .splitters import (
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


def _build_split_payload(
    *,
    selected_views: list[ResearchBarView],
    split_method: str,
    split_config: HoldoutSplitConfig | WalkForwardSplitConfig | None,
) -> dict[str, object]:
    analysis_rows = [row for row in selected_views if row.slice_role == "analysis"]
    analysis_count = len(analysis_rows)
    payload: dict[str, object] = {
        "analysis_row_count": analysis_count,
        "config": split_config_to_dict(split_config),
        "windows": [],
    }
    if analysis_count == 0:
        return payload

    if split_method == "full":
        payload["windows"] = [
            {
                "window_id": "full-01",
                "analysis_start_ts": analysis_rows[0].ts,
                "analysis_end_ts": analysis_rows[-1].ts,
                "train_start": 0,
                "train_stop": analysis_count,
                "test_start": 0,
                "test_stop": analysis_count,
            }
        ]
        return payload

    if split_method == "holdout":
        window = build_holdout_window(analysis_count, split_config if isinstance(split_config, HoldoutSplitConfig) else None)
        payload["windows"] = [
            {
                **window.to_dict(),
                "analysis_start_ts": analysis_rows[0].ts,
                "analysis_end_ts": analysis_rows[-1].ts,
            }
        ]
        return payload

    config = split_config if isinstance(split_config, WalkForwardSplitConfig) else WalkForwardSplitConfig(
        train_size=max(1, math.floor(analysis_count * 0.6)),
        test_size=max(1, math.floor(analysis_count * 0.2)),
        step_size=max(1, math.floor(analysis_count * 0.2)),
    )
    payload["windows"] = [
        {
            **window.to_dict(),
            "analysis_start_ts": analysis_rows[max(0, min(window.train_start, analysis_count - 1))].ts,
            "analysis_end_ts": analysis_rows[max(0, min(window.test_stop - 1, analysis_count - 1))].ts,
        }
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

    existing_manifests = read_delta_table_rows(datasets_path) if (datasets_path / "_delta_log").exists() else []
    existing_views = read_delta_table_rows(bar_views_path) if (bar_views_path / "_delta_log").exists() else []
    write_delta_table_rows(
        table_path=datasets_path,
        rows=[*[row for row in existing_manifests if row.get("dataset_version") != manifest.dataset_version], manifest.to_dict()],
        columns=contract["research_datasets"]["columns"],
    )
    write_delta_table_rows(
        table_path=bar_views_path,
        rows=[
            *[row for row in existing_views if row.get("dataset_version") != manifest.dataset_version],
            *[row.to_dict() for row in selected_views],
        ],
        columns=contract["research_bar_views"]["columns"],
    )
    return {
        "dataset_manifest": manifest.to_dict(),
        "bar_view_count": len(selected_views),
        "output_paths": {
            "research_datasets": datasets_path.as_posix(),
            "research_bar_views": bar_views_path.as_posix(),
        },
        "delta_manifest": contract,
    }


def load_materialized_research_dataset(*, output_dir: Path, dataset_version: str) -> dict[str, object]:
    datasets_path = output_dir / "research_datasets.delta"
    bar_views_path = output_dir / "research_bar_views.delta"
    manifests = read_delta_table_rows(datasets_path)
    bar_view_rows = read_delta_table_rows(bar_views_path)
    manifest_rows = [row for row in manifests if row.get("dataset_version") == dataset_version]
    if not manifest_rows:
        raise KeyError(f"dataset_version not found: {dataset_version}")
    return {
        "dataset_manifest": manifest_rows[0],
        "bar_views": [
            ResearchBarView.from_dict(row)
            for row in bar_view_rows
            if row.get("dataset_version") == dataset_version
        ],
    }
