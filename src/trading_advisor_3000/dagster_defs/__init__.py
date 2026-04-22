from __future__ import annotations

from .phase2a_assets import (
    AssetSpec,
    PHASE2A_ASSETS,
    assert_phase2a_definitions_executable,
    build_phase2a_definitions,
    materialize_phase2a_assets,
    phase2a_asset_specs,
    phase2a_definitions,
    phase2a_materialization_job,
    phase2a_output_paths,
)
from .moex_historical_assets import (
    MOEX_BASELINE_DAILY_SCHEDULE_NAME,
    MOEX_BASELINE_UPDATE_JOB_NAME,
    MOEX_HISTORICAL_ASSETS,
    MOEX_HISTORICAL_CUTOVER_JOB_NAME,
    assert_moex_historical_definitions_executable,
    build_moex_historical_dagster_binding_artifact,
    build_moex_historical_definitions,
    build_moex_historical_run_config,
    execute_moex_historical_cutover_job,
    materialize_moex_historical_assets,
    moex_baseline_daily_update_schedule,
    moex_baseline_update,
    moex_baseline_update_job,
    moex_historical_asset_specs,
    moex_historical_cutover_job,
    moex_historical_cutover_preview_schedule,
    moex_historical_definitions,
    moex_historical_output_paths,
)

_PHASE2B_EXPORTS: tuple[str, ...] = ()
try:
    from .phase2b_assets import (
        PHASE2B_GOVERNED_ASSETS,
        assert_phase2b_definitions_executable,
        build_phase2b_definitions,
        materialize_phase2b_assets,
        phase2b_asset_specs,
        phase2b_governed_definitions,
        phase2b_governed_materialization_job,
        phase2b_output_paths,
    )

    _PHASE2B_EXPORTS = (
        "PHASE2B_GOVERNED_ASSETS",
        "assert_phase2b_definitions_executable",
        "build_phase2b_definitions",
        "materialize_phase2b_assets",
        "phase2b_asset_specs",
        "phase2b_governed_definitions",
        "phase2b_governed_materialization_job",
        "phase2b_output_paths",
    )
except (ImportError, ModuleNotFoundError) as exc:
    if isinstance(exc, ModuleNotFoundError) and exc.name != "trading_advisor_3000.product_plane.research.scoring":
        raise

__all__ = [
    "AssetSpec",
    "MOEX_BASELINE_DAILY_SCHEDULE_NAME",
    "MOEX_BASELINE_UPDATE_JOB_NAME",
    "PHASE2A_ASSETS",
    "MOEX_HISTORICAL_ASSETS",
    "MOEX_HISTORICAL_CUTOVER_JOB_NAME",
    "assert_moex_historical_definitions_executable",
    "assert_phase2a_definitions_executable",
    "build_moex_historical_dagster_binding_artifact",
    "build_moex_historical_definitions",
    "build_moex_historical_run_config",
    "build_phase2a_definitions",
    "execute_moex_historical_cutover_job",
    "materialize_moex_historical_assets",
    "materialize_phase2a_assets",
    "moex_baseline_daily_update_schedule",
    "moex_baseline_update",
    "moex_baseline_update_job",
    "moex_historical_asset_specs",
    "moex_historical_cutover_job",
    "moex_historical_cutover_preview_schedule",
    "moex_historical_definitions",
    "moex_historical_output_paths",
    "phase2a_asset_specs",
    "phase2a_definitions",
    "phase2a_materialization_job",
    "phase2a_output_paths",
]
__all__.extend(_PHASE2B_EXPORTS)
