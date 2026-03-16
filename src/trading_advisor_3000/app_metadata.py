from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class AppMetadata:
    name: str
    shell_mode: str
    domain_logic_enabled: bool


def build_app_metadata() -> dict[str, object]:
    metadata = AppMetadata(
        name="trading-advisor-3000",
        shell_mode="ai-delivery-shell",
        domain_logic_enabled=False,
    )
    return asdict(metadata)
