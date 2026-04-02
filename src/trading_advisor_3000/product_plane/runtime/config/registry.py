from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.product_plane.contracts import DecisionCandidate, Mode, Timeframe


ALLOWED_STATUSES = {"draft", "shadow", "active", "deprecated"}


@dataclass(frozen=True)
class StrategyVersion:
    strategy_version_id: str
    status: str
    allowed_contracts: frozenset[str]
    allowed_timeframes: frozenset[Timeframe]
    allowed_modes: frozenset[Mode]
    activated_from: str

    def __post_init__(self) -> None:
        if self.status not in ALLOWED_STATUSES:
            raise ValueError(f"unsupported strategy status: {self.status}")
        if not self.strategy_version_id.strip():
            raise ValueError("strategy_version_id must be non-empty")

    def to_dict(self) -> dict[str, object]:
        return {
            "strategy_version_id": self.strategy_version_id,
            "status": self.status,
            "allowed_contracts": sorted(self.allowed_contracts),
            "allowed_timeframes": sorted(item.value for item in self.allowed_timeframes),
            "allowed_modes": sorted(item.value for item in self.allowed_modes),
            "activated_from": self.activated_from,
        }


class StrategyRegistry:
    def __init__(self) -> None:
        self._versions: dict[str, StrategyVersion] = {}

    def register(self, version: StrategyVersion) -> None:
        self._versions[version.strategy_version_id] = version

    def activate(self, strategy_version_id: str, *, activated_from: str) -> None:
        existing = self._versions.get(strategy_version_id)
        if existing is None:
            raise ValueError(f"strategy is not registered: {strategy_version_id}")
        self._versions[strategy_version_id] = StrategyVersion(
            strategy_version_id=existing.strategy_version_id,
            status="active",
            allowed_contracts=existing.allowed_contracts,
            allowed_timeframes=existing.allowed_timeframes,
            allowed_modes=existing.allowed_modes,
            activated_from=activated_from,
        )

    def get(self, strategy_version_id: str) -> StrategyVersion | None:
        return self._versions.get(strategy_version_id)

    def allows(self, candidate: DecisionCandidate) -> tuple[bool, str]:
        version = self._versions.get(candidate.strategy_version_id)
        if version is None:
            return False, "strategy_not_registered"
        if version.status != "active":
            return False, "strategy_not_active"
        if candidate.contract_id not in version.allowed_contracts:
            return False, "contract_not_allowed"
        if candidate.timeframe not in version.allowed_timeframes:
            return False, "timeframe_not_allowed"
        if candidate.mode not in version.allowed_modes:
            return False, "mode_not_allowed"
        return True, "ok"

    def snapshot(self) -> list[dict[str, object]]:
        return [item.to_dict() for item in sorted(self._versions.values(), key=lambda row: row.strategy_version_id)]
