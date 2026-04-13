from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from dagster import AssetSelection, Definitions, asset, define_asset_job, materialize

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    ResearchDatasetManifest,
    load_materialized_research_dataset,
    materialize_research_dataset,
    phase2_research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.features import (
    materialize_feature_frames,
    phase2b_feature_store_contract,
    reload_feature_frames,
)
from trading_advisor_3000.product_plane.research.indicators import (
    materialize_indicator_frames,
    phase3_indicator_store_contract,
    reload_indicator_frames,
)

from .phase2a_assets import AssetSpec


PHASE2B_BOOTSTRAP_ASSETS = (
    "research_datasets",
    "research_bar_views",
    "research_indicator_frames",
    "research_feature_frames",
)

PHASE2B_LEGACY_ASSETS = (
    "feature_snapshots",
    "research_backtest_runs",
    "research_signal_candidates",
)

PHASE2B_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "research_datasets": tuple(),
    "research_bar_views": ("research_datasets",),
    "research_indicator_frames": ("research_datasets", "research_bar_views"),
    "research_feature_frames": ("research_datasets", "research_bar_views", "research_indicator_frames"),
}


def phase2b_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="research_datasets",
            description="Materialize versioned research dataset manifests from canonical bars/session calendar/roll map.",
            inputs=("canonical_bars_delta", "canonical_session_calendar_delta", "canonical_roll_map_delta"),
            outputs=("research_datasets_delta",),
        ),
        AssetSpec(
            key="research_bar_views",
            description="Materialize research-ready bar views for the selected dataset version.",
            inputs=("research_datasets_delta",),
            outputs=("research_bar_views_delta",),
        ),
        AssetSpec(
            key="research_indicator_frames",
            description="Bootstrap indicator partitions from materialized research bar views.",
            inputs=("research_datasets_delta", "research_bar_views_delta"),
            outputs=("research_indicator_frames_delta",),
        ),
        AssetSpec(
            key="research_feature_frames",
            description="Bootstrap derived feature partitions from research bar views and materialized indicators.",
            inputs=("research_datasets_delta", "research_bar_views_delta", "research_indicator_frames_delta"),
            outputs=("research_feature_frames_delta",),
        ),
        AssetSpec(
            key="feature_snapshots",
            description="Build point-in-time feature snapshots from canonical bars.",
            inputs=("canonical_bars_delta",),
            outputs=("feature_snapshots_delta",),
        ),
        AssetSpec(
            key="research_backtest_runs",
            description="Run deterministic backtests and store run metadata.",
            inputs=("feature_snapshots_delta",),
            outputs=("research_backtest_runs_delta",),
        ),
        AssetSpec(
            key="research_signal_candidates",
            description="Persist contract-safe signal candidates from backtests.",
            inputs=("feature_snapshots_delta", "research_backtest_runs_delta"),
            outputs=("research_signal_candidates_delta",),
        ),
    ]


def _phase2b_config_schema() -> dict[str, object]:
    return {
        "canonical_output_dir": str,
        "research_output_dir": str,
        "dataset_version": str,
        "dataset_name": str,
        "universe_id": str,
        "timeframes": [str],
        "base_timeframe": str,
        "start_ts": str,
        "end_ts": str,
        "warmup_bars": int,
        "split_method": str,
        "series_mode": str,
        "indicator_set_version": str,
        "indicator_profile_version": str,
        "feature_set_version": str,
        "feature_profile_version": str,
        "code_version": str,
    }


def _config_value(config: dict[str, object], key: str, default: object | None = None) -> object:
    value = config.get(key, default)
    if value is None:
        raise KeyError(f"missing phase2b config value: {key}")
    return value


def _canonical_table_path(config: dict[str, object], table_name: str) -> Path:
    return Path(str(_config_value(config, "canonical_output_dir"))).resolve() / f"{table_name}.delta"


def _load_canonical_context(config: dict[str, object]) -> tuple[list[CanonicalBar], list[SessionCalendarEntry], list[RollMapEntry]]:
    bars_path = _canonical_table_path(config, "canonical_bars")
    calendar_path = _canonical_table_path(config, "canonical_session_calendar")
    roll_map_path = _canonical_table_path(config, "canonical_roll_map")
    for path in (bars_path, calendar_path, roll_map_path):
        if not has_delta_log(path):
            raise RuntimeError(f"missing canonical delta table: {path.as_posix()}")

    bars = [CanonicalBar.from_dict(row) for row in read_delta_table_rows(bars_path)]
    session_calendar = [
        SessionCalendarEntry(
            instrument_id=str(row["instrument_id"]),
            timeframe=str(row["timeframe"]),
            session_date=str(row["session_date"]),
            session_open_ts=str(row["session_open_ts"]),
            session_close_ts=str(row["session_close_ts"]),
        )
        for row in read_delta_table_rows(calendar_path)
    ]
    roll_map = [
        RollMapEntry(
            instrument_id=str(row["instrument_id"]),
            session_date=str(row["session_date"]),
            active_contract_id=str(row["active_contract_id"]),
            reason=str(row["reason"]),
        )
        for row in read_delta_table_rows(roll_map_path)
    ]
    return bars, session_calendar, roll_map


def _seed_manifest(config: dict[str, object]) -> ResearchDatasetManifest:
    return ResearchDatasetManifest(
        dataset_version=str(_config_value(config, "dataset_version")),
        dataset_name=str(_config_value(config, "dataset_name", "research-bootstrap")),
        source_table="canonical_bars",
        universe_id=str(_config_value(config, "universe_id", "moex-futures")),
        timeframes=tuple(str(item) for item in _config_value(config, "timeframes")),
        base_timeframe=str(_config_value(config, "base_timeframe", "15m")),
        start_ts=str(_config_value(config, "start_ts", "")) or None,
        end_ts=str(_config_value(config, "end_ts", "")) or None,
        series_mode=str(_config_value(config, "series_mode", "contract")),  # type: ignore[arg-type]
        split_method=str(_config_value(config, "split_method", "holdout")),  # type: ignore[arg-type]
        warmup_bars=int(_config_value(config, "warmup_bars", 200)),
        continuous_front_policy=ContinuousFrontPolicy() if str(_config_value(config, "series_mode", "contract")) == "continuous_front" else None,
        code_version=str(_config_value(config, "code_version", "dagster-phase2b-bootstrap")),
    )


@asset(group_name="phase2b", config_schema=_phase2b_config_schema())
def research_datasets(context) -> dict[str, object]:
    config = dict(context.op_execution_context.op_config)
    research_output_dir = Path(str(_config_value(config, "research_output_dir"))).resolve()
    bars, session_calendar, roll_map = _load_canonical_context(config)
    report = materialize_research_dataset(
        manifest_seed=_seed_manifest(config),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=research_output_dir,
    )
    return {
        "canonical_output_dir": Path(str(_config_value(config, "canonical_output_dir"))).resolve().as_posix(),
        "research_output_dir": research_output_dir.as_posix(),
        "dataset_version": str(_config_value(config, "dataset_version")),
        "indicator_set_version": str(_config_value(config, "indicator_set_version", "indicators-v1")),
        "indicator_profile_version": str(_config_value(config, "indicator_profile_version", "core_v1")),
        "feature_set_version": str(_config_value(config, "feature_set_version", "features-v1")),
        "feature_profile_version": str(_config_value(config, "feature_profile_version", "core_v1")),
        "dataset_manifest": report["dataset_manifest"],
        "output_paths": report["output_paths"],
        "delta_manifest": {
            **phase2_research_dataset_store_contract(),
            **phase3_indicator_store_contract(),
            **phase2b_feature_store_contract(),
        },
    }


@asset(group_name="phase2b")
def research_bar_views(research_datasets: dict[str, object]) -> list[dict[str, object]]:
    research_output_dir = Path(str(research_datasets["research_output_dir"]))
    loaded = load_materialized_research_dataset(
        output_dir=research_output_dir,
        dataset_version=str(research_datasets["dataset_version"]),
    )
    return [row.to_dict() for row in loaded["bar_views"]]


@asset(group_name="phase2b")
def research_indicator_frames(
    research_datasets: dict[str, object],
    research_bar_views: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_bar_views
    research_output_dir = Path(str(research_datasets["research_output_dir"]))
    dataset_version = str(research_datasets["dataset_version"])
    indicator_set_version = str(research_datasets["indicator_set_version"])
    profile_version = str(research_datasets["indicator_profile_version"])
    materialize_indicator_frames(
        dataset_output_dir=research_output_dir,
        indicator_output_dir=research_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        profile_version=profile_version,
    )
    rows = reload_indicator_frames(
        indicator_output_dir=research_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    )
    return [row.to_dict() for row in rows]


@asset(group_name="phase2b")
def research_feature_frames(
    research_datasets: dict[str, object],
    research_bar_views: list[dict[str, object]],
    research_indicator_frames: list[dict[str, object]],
) -> list[dict[str, object]]:
    del research_bar_views
    del research_indicator_frames
    research_output_dir = Path(str(research_datasets["research_output_dir"]))
    dataset_version = str(research_datasets["dataset_version"])
    indicator_set_version = str(research_datasets["indicator_set_version"])
    feature_set_version = str(research_datasets["feature_set_version"])
    profile_version = str(research_datasets["feature_profile_version"])
    materialize_feature_frames(
        dataset_output_dir=research_output_dir,
        indicator_output_dir=research_output_dir,
        feature_output_dir=research_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
        profile_version=profile_version,
    )
    rows = reload_feature_frames(
        feature_output_dir=research_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
    )
    return [row.to_dict() for row in rows]


PHASE2B_ASSETS = (
    research_datasets,
    research_bar_views,
    research_indicator_frames,
    research_feature_frames,
)

phase2b_bootstrap_job = define_asset_job(
    name="phase2b_bootstrap_job",
    selection=AssetSelection.groups("phase2b"),
)

phase2b_definitions = Definitions(
    assets=list(PHASE2B_ASSETS),
    jobs=[phase2b_bootstrap_job],
)


def assert_phase2b_definitions_executable(definitions: Definitions | None = None) -> None:
    defs = definitions or phase2b_definitions
    repository = defs.get_repository_def()
    job = repository.get_job("phase2b_bootstrap_job")
    actual_nodes = set(job.graph.node_dict.keys())
    missing_nodes = sorted(set(PHASE2B_BOOTSTRAP_ASSETS) - actual_nodes)
    if missing_nodes:
        raise RuntimeError(
            "phase2b Dagster definitions are metadata-only or incomplete: "
            f"missing executable asset nodes: {', '.join(missing_nodes)}"
        )


def build_phase2b_definitions() -> Definitions:
    assert_phase2b_definitions_executable(phase2b_definitions)
    return phase2b_definitions


def _resolve_selected_assets(selection: Sequence[str] | None) -> list[str]:
    if selection is None:
        return list(PHASE2B_BOOTSTRAP_ASSETS)
    normalized = sorted({item.strip() for item in selection if item.strip()})
    if not normalized:
        raise ValueError("selection must include at least one asset key")
    unknown = [item for item in normalized if item not in PHASE2B_BOOTSTRAP_ASSETS]
    if unknown:
        raise ValueError(f"unknown phase2b bootstrap asset selection: {', '.join(sorted(unknown))}")
    return normalized


def _resolve_expected_materialization(selection: Sequence[str]) -> list[str]:
    resolved: set[str] = set()

    def _visit(asset_name: str) -> None:
        if asset_name in resolved:
            return
        for dependency in PHASE2B_DEPENDENCIES.get(asset_name, tuple()):
            _visit(dependency)
        resolved.add(asset_name)

    for asset_name in selection:
        _visit(asset_name)
    return [name for name in PHASE2B_BOOTSTRAP_ASSETS if name in resolved]


def materialize_phase2b_bootstrap_assets(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path,
    dataset_version: str,
    timeframes: Sequence[str],
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    assert_phase2b_definitions_executable()
    selected_assets = _resolve_selected_assets(selection)
    expected_materialized_assets = _resolve_expected_materialization(selected_assets)

    result = materialize(
        assets=list(PHASE2B_ASSETS),
        selection=expected_materialized_assets,
        run_config={
            "ops": {
                "research_datasets": {
                    "config": {
                        "canonical_output_dir": canonical_output_dir.resolve().as_posix(),
                        "research_output_dir": research_output_dir.resolve().as_posix(),
                        "dataset_version": dataset_version,
                        "dataset_name": "research-bootstrap",
                        "universe_id": "moex-futures",
                        "timeframes": list(timeframes),
                        "base_timeframe": "15m",
                        "start_ts": "",
                        "end_ts": "",
                        "warmup_bars": 200,
                        "split_method": "holdout",
                        "series_mode": "contract",
                        "indicator_set_version": indicator_set_version,
                        "indicator_profile_version": indicator_profile_version,
                        "feature_set_version": feature_set_version,
                        "feature_profile_version": feature_profile_version,
                        "code_version": "dagster-phase2b-bootstrap",
                    }
                }
            }
        },
        raise_on_error=raise_on_error,
    )

    output_paths = {
        "research_datasets": (research_output_dir / "research_datasets.delta").as_posix(),
        "research_bar_views": (research_output_dir / "research_bar_views.delta").as_posix(),
        "research_indicator_frames": (research_output_dir / "research_indicator_frames.delta").as_posix(),
        "research_feature_frames": (research_output_dir / "research_feature_frames.delta").as_posix(),
    }
    report: dict[str, object] = {
        "success": bool(result.success),
        "selected_assets": selected_assets,
        "materialized_assets": expected_materialized_assets,
        "output_paths": output_paths,
    }
    if not result.success:
        report["rows_by_table"] = {}
        return report

    rows_by_table: dict[str, int] = {}
    for asset_name in expected_materialized_assets:
        table_path = Path(output_paths[asset_name])
        if not has_delta_log(table_path):
            raise RuntimeError(f"missing `_delta_log` for `{asset_name}` at {table_path.as_posix()}")
        rows_by_table[asset_name] = len(read_delta_table_rows(table_path))
    report["rows_by_table"] = rows_by_table
    return report
