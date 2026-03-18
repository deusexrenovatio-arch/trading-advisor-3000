from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.app.contracts import Mode


@dataclass(frozen=True)
class ExecutionAdapterSpec:
    adapter_id: str
    route: str
    supports_live: bool
    supports_paper: bool
    description: str = ""
    capabilities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.adapter_id, str) or not self.adapter_id.strip():
            raise ValueError("adapter_id must be non-empty string")
        if not isinstance(self.route, str) or not self.route.strip():
            raise ValueError("route must be non-empty string")

    def supports_mode(self, mode: Mode) -> bool:
        if mode == Mode.LIVE:
            return self.supports_live
        if mode == Mode.PAPER:
            return self.supports_paper
        return False

    def to_dict(self) -> dict[str, object]:
        return {
            "adapter_id": self.adapter_id,
            "route": self.route,
            "supports_live": self.supports_live,
            "supports_paper": self.supports_paper,
            "description": self.description,
            "capabilities": list(self.capabilities),
        }


class ExecutionAdapterCatalog:
    def __init__(self, *, specs: list[ExecutionAdapterSpec] | None = None) -> None:
        self._specs_by_id: dict[str, ExecutionAdapterSpec] = {}
        for spec in specs or []:
            self.register(spec)

    def register(self, spec: ExecutionAdapterSpec) -> None:
        if spec.adapter_id in self._specs_by_id:
            raise ValueError(f"adapter already registered: {spec.adapter_id}")
        self._specs_by_id[spec.adapter_id] = spec

    def get(self, adapter_id: str) -> ExecutionAdapterSpec | None:
        return self._specs_by_id.get(adapter_id)

    def list_specs(self) -> list[ExecutionAdapterSpec]:
        return [self._specs_by_id[key] for key in sorted(self._specs_by_id)]

    def to_dict(self) -> list[dict[str, object]]:
        return [item.to_dict() for item in self.list_specs()]


def default_execution_adapter_catalog() -> ExecutionAdapterCatalog:
    return ExecutionAdapterCatalog(
        specs=[
            ExecutionAdapterSpec(
                adapter_id="stocksharp-sidecar-stub",
                route="stocksharp->stub",
                supports_live=True,
                supports_paper=True,
                description="local sidecar simulation path",
                capabilities=("submit", "cancel", "replace"),
            ),
            ExecutionAdapterSpec(
                adapter_id="stocksharp-sidecar",
                route="stocksharp->quik->finam",
                supports_live=True,
                supports_paper=False,
                description="production sidecar transport path",
                capabilities=("submit", "cancel", "replace"),
            ),
        ]
    )
