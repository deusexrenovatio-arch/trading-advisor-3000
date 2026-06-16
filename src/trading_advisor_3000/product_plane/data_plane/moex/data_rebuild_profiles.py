from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.moex.runtime_instances import (
    validate_moex_runtime_run_id,
)

DATA_LAYER_REBUILD_STAGE_NAMES = (
    "raw",
    "economics_raw",
    "sessions",
    "canonical",
    "economics_canonical",
    "continuous_front",
    "research_bar",
    "indicator",
    "derived",
    "indicator_sidecar",
)
FORBIDDEN_REBUILD_STAGE_NAMES = ("strategy", "backtest", "projection", "execution")
MOEX_REBUILD_SOURCE_MODES = ("full_raw_ingest", "existing_raw_delta")
MOEX_REBUILD_PUBLISH_MODES = ("promote", "staging_only")
MOEX_REBUILD_DOWNSTREAM_MODES = ("invalidate", "none")

_STALE_TARGET_ORDER = (
    "continuous_front",
    "research_bar",
    "indicator",
    "derived",
    "indicator_sidecar",
    "strategy",
    "backtest",
    "projection",
    "execution",
)
_STAGE_ORDER = {
    "raw": 0,
    "economics_raw": 1,
    "sessions": 2,
    "canonical": 3,
    "economics_canonical": 4,
    "continuous_front": 5,
    "research_bar": 6,
    "indicator": 7,
    "derived": 8,
    "indicator_sidecar": 9,
}


@dataclass(frozen=True)
class MoexDataRebuildProfile:
    name: str
    stage_names: tuple[str, ...]
    source_mode: str
    requires_raw_ingest: bool = False
    description: str = ""


def resolve_moex_data_layer_stages(stage_names: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(
        str(stage_name).strip() for stage_name in stage_names if str(stage_name).strip()
    )
    if not normalized:
        return tuple()

    forbidden = sorted(set(normalized).intersection(FORBIDDEN_REBUILD_STAGE_NAMES))
    if forbidden:
        raise ValueError(
            "stages outside the MOEX data-layer rebuild scope cannot be selected: "
            f"{', '.join(forbidden)}"
        )

    unknown = sorted(set(normalized).difference(DATA_LAYER_REBUILD_STAGE_NAMES))
    if unknown:
        raise ValueError(f"unknown MOEX data-layer rebuild stages: {', '.join(unknown)}")

    return tuple(sorted(dict.fromkeys(normalized), key=lambda stage: _STAGE_ORDER[stage]))


MOEX_DATA_REBUILD_PROFILES: dict[str, MoexDataRebuildProfile] = {
    "full_raw_to_canonical": MoexDataRebuildProfile(
        name="full_raw_to_canonical",
        stage_names=resolve_moex_data_layer_stages(("raw", "sessions", "canonical")),
        source_mode="full_raw_ingest",
        requires_raw_ingest=True,
        description="Rebuild raw MOEX history, manual session intervals, and canonical bars.",
    ),
    "money_math_bootstrap": MoexDataRebuildProfile(
        name="money_math_bootstrap",
        stage_names=resolve_moex_data_layer_stages(
            (
                "economics_raw",
                "economics_canonical",
                "continuous_front",
                "research_bar",
            )
        ),
        source_mode="existing_raw_delta",
        requires_raw_ingest=False,
        description=(
            "One-time MOEX money-math bootstrap: refresh economics side tables, "
            "then rebuild continuous-front and research bar views without rewriting bars."
        ),
    ),
    "canonical_from_existing_raw": MoexDataRebuildProfile(
        name="canonical_from_existing_raw",
        stage_names=resolve_moex_data_layer_stages(("sessions", "canonical")),
        source_mode="existing_raw_delta",
        requires_raw_ingest=False,
        description=(
            "Rebuild session intervals and canonical bars from an existing raw Delta table."
        ),
    ),
    "cf_rebuild": MoexDataRebuildProfile(
        name="cf_rebuild",
        stage_names=resolve_moex_data_layer_stages(("continuous_front",)),
        source_mode="existing_raw_delta",
        description="Rebuild only the continuous-front layer.",
    ),
    "research_bar_rebuild": MoexDataRebuildProfile(
        name="research_bar_rebuild",
        stage_names=resolve_moex_data_layer_stages(("research_bar",)),
        source_mode="existing_raw_delta",
        description="Rebuild only research datasets, instrument tree, and bar views.",
    ),
    "indicator_rebuild": MoexDataRebuildProfile(
        name="indicator_rebuild",
        stage_names=resolve_moex_data_layer_stages(("indicator",)),
        source_mode="existing_raw_delta",
        description="Rebuild only base research indicator frames.",
    ),
    "derived_rebuild": MoexDataRebuildProfile(
        name="derived_rebuild",
        stage_names=resolve_moex_data_layer_stages(("derived",)),
        source_mode="existing_raw_delta",
        description="Rebuild only derived research indicator frames.",
    ),
    "data_layer_rebuild": MoexDataRebuildProfile(
        name="data_layer_rebuild",
        stage_names=resolve_moex_data_layer_stages(
            (
                "continuous_front",
                "research_bar",
                "indicator",
                "derived",
                "indicator_sidecar",
            )
        ),
        source_mode="existing_raw_delta",
        description=(
            "Rebuild the data layer after current canonical data is already refreshed, "
            "including post-derived continuous-front indicator sidecars."
        ),
    ),
    "invalidate_downstream_only": MoexDataRebuildProfile(
        name="invalidate_downstream_only",
        stage_names=tuple(),
        source_mode="existing_raw_delta",
        description="Write stale markers without rebuilding any data-layer stage.",
    ),
}
MOEX_DATA_REBUILD_PROFILE_NAMES = tuple(MOEX_DATA_REBUILD_PROFILES)


def resolve_moex_data_rebuild_profile(profile_name: str) -> MoexDataRebuildProfile:
    resolved_name = str(profile_name).strip()
    try:
        return MOEX_DATA_REBUILD_PROFILES[resolved_name]
    except KeyError as exc:
        allowed = ", ".join(MOEX_DATA_REBUILD_PROFILE_NAMES)
        raise ValueError(
            "unknown MOEX data rebuild profile "
            f"`{resolved_name or '<empty>'}`; allowed profiles: {allowed}"
        ) from exc


def dependent_stale_targets_for_stages(stage_names: Sequence[str]) -> tuple[str, ...]:
    stages = resolve_moex_data_layer_stages(stage_names)
    if not stages:
        return FORBIDDEN_REBUILD_STAGE_NAMES
    lowest_changed_index = min(_STAGE_ORDER[stage] for stage in stages)
    if lowest_changed_index <= _STAGE_ORDER["economics_canonical"]:
        first_target = "continuous_front"
    else:
        changed_stage = min(stages, key=lambda stage: _STAGE_ORDER[stage])
        if changed_stage == "continuous_front":
            first_target = "research_bar"
        elif changed_stage == "research_bar":
            first_target = "indicator"
        elif changed_stage == "indicator":
            first_target = "derived"
        elif changed_stage == "derived":
            first_target = "indicator_sidecar"
        else:
            first_target = "strategy"
    start_index = _STALE_TARGET_ORDER.index(first_target)
    return _STALE_TARGET_ORDER[start_index:]


def _path_mapping_to_manifest(raw: Mapping[str, str | Path]) -> dict[str, str]:
    return {str(key): Path(value).resolve().as_posix() for key, value in raw.items()}


def build_moex_data_rebuild_manifest(
    *,
    profile: MoexDataRebuildProfile,
    run_id: str,
    publish_mode: str,
    downstream_mode: str,
    input_roots: Mapping[str, str | Path] | None = None,
    staged_outputs: Mapping[str, str | Path] | None = None,
    promoted_outputs: Mapping[str, str | Path] | None = None,
    row_counts: Mapping[str, int] | None = None,
) -> dict[str, object]:
    resolved_run_id = validate_moex_runtime_run_id(
        run_id, name="build_moex_data_rebuild_manifest.run_id"
    )
    resolved_publish_mode = publish_mode.strip() or "promote"
    if resolved_publish_mode not in MOEX_REBUILD_PUBLISH_MODES:
        raise ValueError(
            f"unknown MOEX data rebuild publish_mode `{resolved_publish_mode}`; "
            f"allowed modes: {', '.join(MOEX_REBUILD_PUBLISH_MODES)}"
        )
    resolved_downstream_mode = downstream_mode.strip() or "invalidate"
    if resolved_downstream_mode not in MOEX_REBUILD_DOWNSTREAM_MODES:
        raise ValueError(
            f"unknown MOEX data rebuild downstream_mode `{resolved_downstream_mode}`; "
            f"allowed modes: {', '.join(MOEX_REBUILD_DOWNSTREAM_MODES)}"
        )
    invalidated_outputs = (
        list(dependent_stale_targets_for_stages(profile.stage_names))
        if resolved_downstream_mode == "invalidate"
        else []
    )
    return {
        "schema_version": "moex_data_rebuild_manifest.v1",
        "profile_name": profile.name,
        "run_id": resolved_run_id,
        "created_at_utc": datetime.now(tz=UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "source_mode": profile.source_mode,
        "publish_mode": resolved_publish_mode,
        "downstream_mode": resolved_downstream_mode,
        "stage_names": list(profile.stage_names),
        "requires_raw_ingest": profile.requires_raw_ingest,
        "input_roots": _path_mapping_to_manifest(input_roots or {}),
        "staged_outputs": _path_mapping_to_manifest(staged_outputs or {}),
        "promoted_outputs": _path_mapping_to_manifest(promoted_outputs or {}),
        "row_counts": {str(key): int(value) for key, value in dict(row_counts or {}).items()},
        "invalidated_outputs": invalidated_outputs,
    }


def write_moex_data_rebuild_manifest(path: Path, manifest: Mapping[str, object]) -> Path:
    resolved = path.resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(
        json.dumps(dict(manifest), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return resolved
