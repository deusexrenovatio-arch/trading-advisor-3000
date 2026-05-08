from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class HotDeltaTable:
    name: str
    filename: str
    family: str
    access_policy: str = "filtered_or_batched_python_or_spark"


HOT_DELTA_TABLES: tuple[HotDeltaTable, ...] = (
    HotDeltaTable("raw_moex_history", "raw_moex_history.delta", "raw_moex"),
    HotDeltaTable("canonical_bars", "canonical_bars.delta", "canonical"),
    HotDeltaTable("canonical_bar_provenance", "canonical_bar_provenance.delta", "canonical"),
    HotDeltaTable(
        "canonical_session_calendar", "canonical_session_calendar.delta", "canonical_sidecar"
    ),
    HotDeltaTable("canonical_roll_map", "canonical_roll_map.delta", "canonical_sidecar"),
    HotDeltaTable("canonical_contracts", "canonical_contracts.delta", "canonical_sidecar"),
    HotDeltaTable("canonical_instruments", "canonical_instruments.delta", "canonical_sidecar"),
    HotDeltaTable("research_bar_views", "research_bar_views.delta", "research"),
    HotDeltaTable("research_indicator_frames", "research_indicator_frames.delta", "research"),
    HotDeltaTable(
        "research_derived_indicator_frames", "research_derived_indicator_frames.delta", "research"
    ),
    HotDeltaTable("continuous_front_bars", "continuous_front_bars.delta", "continuous_front"),
    HotDeltaTable(
        "continuous_front_adjustment_ladder",
        "continuous_front_adjustment_ladder.delta",
        "continuous_front",
    ),
    HotDeltaTable(
        "continuous_front_indicator_frames",
        "continuous_front_indicator_frames.delta",
        "continuous_front",
    ),
    HotDeltaTable(
        "continuous_front_derived_indicator_frames",
        "continuous_front_derived_indicator_frames.delta",
        "continuous_front",
    ),
    HotDeltaTable(
        "continuous_front_indicator_acceptance_report",
        "continuous_front_indicator_acceptance_report.delta",
        "continuous_front",
    ),
    HotDeltaTable(
        "continuous_front_indicator_run_manifest",
        "continuous_front_indicator_run_manifest.delta",
        "continuous_front",
    ),
)

HOT_DELTA_TABLE_NAMES = frozenset(table.name for table in HOT_DELTA_TABLES)
HOT_DELTA_TABLE_FILENAMES = frozenset(table.filename for table in HOT_DELTA_TABLES)

SMALL_MANIFEST_DELTA_TABLE_FILENAMES = frozenset(
    {
        "research_datasets.delta",
        "research_instrument_tree.delta",
        "research_strategy_families.delta",
        "research_strategy_templates.delta",
        "research_strategy_template_modules.delta",
        "research_strategy_instances.delta",
        "research_strategy_instance_modules.delta",
    }
)


def delta_table_filename(table_path: Path | str) -> str:
    return Path(table_path).name


def is_hot_delta_table_path(table_path: Path | str) -> bool:
    return delta_table_filename(table_path) in HOT_DELTA_TABLE_FILENAMES


def is_small_manifest_delta_table_path(table_path: Path | str) -> bool:
    return delta_table_filename(table_path) in SMALL_MANIFEST_DELTA_TABLE_FILENAMES
