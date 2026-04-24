from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContinuousFrontPolicy:
    roll_source: str = "canonical_roll_map"
    active_contract_field: str = "active_contract_id"
    require_point_in_time_alignment: bool = True
    preserve_roll_gap_columns: bool = True

