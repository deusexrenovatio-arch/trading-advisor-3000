from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AssetSpec:
    key: str
    description: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


def phase2a_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="raw_market_backfill",
            description="Ingest raw backfill rows for whitelist contracts.",
            inputs=("raw_backfill_source",),
            outputs=("raw_market_backfill_delta",),
        ),
        AssetSpec(
            key="canonical_bars",
            description="Build canonical OHLCV bars from raw backfill.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_bars_delta",),
        ),
    ]
