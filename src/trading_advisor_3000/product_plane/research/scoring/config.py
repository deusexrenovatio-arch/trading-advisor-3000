from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class StrategyScoringProfile:
    profile_version: str
    capital_rub: int
    min_annual_return_pct: float
    max_negative_months_4y: int
    min_active_assets_count: int
    min_signals_per_week: float
    require_repeatable: bool
    min_sharpe_ratio: float
    max_drawdown_pct: float
    research_timeframes: tuple[str, ...]
    strategy_families: tuple[str, ...]


def _require_text(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty string")
    return value.strip()


def _require_bool(value: object, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean")
    return value


def _require_int(value: object, *, name: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _require_float(value: object, *, name: str, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be number")
    normalized = float(value)
    if minimum is not None and normalized < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return normalized


def _require_text_list(value: object, *, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} must be non-empty list")
    normalized = tuple(_require_text(item, name=f"{name}[]") for item in value)
    return normalized


def load_strategy_scoring_profile(path: Path) -> StrategyScoringProfile:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"strategy scoring profile must be object: {path.as_posix()}")

    criteria = payload.get("criteria")
    if not isinstance(criteria, dict):
        raise ValueError("criteria must be object")

    profile = StrategyScoringProfile(
        profile_version=_require_text(payload.get("profile_version"), name="profile_version"),
        capital_rub=_require_int(payload.get("capital_rub"), name="capital_rub", minimum=1),
        min_annual_return_pct=_require_float(
            criteria.get("min_annual_return_pct"),
            name="criteria.min_annual_return_pct",
            minimum=0,
        ),
        max_negative_months_4y=_require_int(
            criteria.get("max_negative_months_4y"),
            name="criteria.max_negative_months_4y",
            minimum=0,
        ),
        min_active_assets_count=_require_int(
            criteria.get("min_active_assets_count"),
            name="criteria.min_active_assets_count",
            minimum=1,
        ),
        min_signals_per_week=_require_float(
            criteria.get("min_signals_per_week"),
            name="criteria.min_signals_per_week",
            minimum=0,
        ),
        require_repeatable=_require_bool(
            criteria.get("require_repeatable"),
            name="criteria.require_repeatable",
        ),
        min_sharpe_ratio=_require_float(
            criteria.get("min_sharpe_ratio"),
            name="criteria.min_sharpe_ratio",
        ),
        max_drawdown_pct=_require_float(
            criteria.get("max_drawdown_pct"),
            name="criteria.max_drawdown_pct",
            minimum=0,
        ),
        research_timeframes=_require_text_list(payload.get("research_timeframes"), name="research_timeframes"),
        strategy_families=_require_text_list(payload.get("strategy_families"), name="strategy_families"),
    )

    if "1d" not in profile.research_timeframes:
        raise ValueError("research_timeframes must include `1d`")
    if "5m" in profile.research_timeframes:
        raise ValueError("research_timeframes must not include `5m` for medium-term profile")

    return profile
