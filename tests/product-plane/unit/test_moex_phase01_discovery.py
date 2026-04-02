from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.moex.foundation import discover_coverage, load_mapping_registry, load_universe
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import CandleBorder, select_interval_borders


class _CoverageClient:
    def fetch_candleborders(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        required_intervals: set[int],
    ) -> dict[int, CandleBorder]:
        assert engine == "futures"
        assert market == "forts"
        assert board == "RFUD"
        assert secid
        return {
            interval: CandleBorder(
                interval=interval,
                begin="2026-03-20 10:00:00",
                end="2026-04-02 12:00:00",
            )
            for interval in required_intervals
        }


def test_select_interval_borders_extracts_required_intervals() -> None:
    payload = {
        "borders": {
            "columns": ["begin", "end", "interval"],
            "data": [
                ["2026-01-01 10:00:00", "2026-04-02 12:00:00", 1],
                ["2026-01-01 10:00:00", "2026-04-02 12:00:00", 10],
                ["2026-01-01 10:00:00", "2026-04-02 12:00:00", 60],
            ],
        }
    }
    selected = select_interval_borders(payload, required_intervals={1, 10, 60})
    assert sorted(selected) == [1, 10, 60]
    assert selected[1].begin == "2026-01-01 10:00:00"
    assert selected[60].end == "2026-04-02 12:00:00"


def test_select_interval_borders_fails_when_required_interval_missing() -> None:
    payload = {
        "borders": {
            "columns": ["begin", "end", "interval"],
            "data": [["2026-01-01 10:00:00", "2026-04-02 12:00:00", 10]],
        }
    }
    try:
        select_interval_borders(payload, required_intervals={1, 10})
    except ValueError as exc:
        assert "missing required intervals" in str(exc)
    else:
        raise AssertionError("expected missing interval failure")


def test_discover_coverage_tracks_native_source_identity() -> None:
    universe = load_universe(Path("configs/moex_phase01/universe/moex-futures-priority.v1.yaml"))
    mappings = load_mapping_registry(Path("configs/moex_phase01/instrument_mapping_registry.v1.yaml"))
    coverage = discover_coverage(
        client=_CoverageClient(),
        universe=universe,
        mappings=mappings,
        timeframes={"5m", "15m", "1h", "4h", "1d", "1w"},
        discovered_at_utc="2026-04-02T10:00:00Z",
        ingest_till_utc="2026-04-02T12:00:00Z",
        bootstrap_window_days=1461,
        expand_contract_chain=False,
        contract_discovery_step_days=14,
    )
    active_mappings = [row for row in mappings if row.is_active]
    assert len(coverage) == len(active_mappings) * 5
    assert {row.source_interval for row in coverage} == {1, 7, 10, 24, 60}
    assert {row.source_timeframe for row in coverage} == {"1m", "10m", "1h", "1d", "1w"}

    by_key = {(row.internal_id, row.source_interval): row for row in coverage}
    assert by_key[("FUT_BR", 1)].requested_target_timeframes == "5m"
    assert by_key[("FUT_BR", 10)].requested_target_timeframes == "15m"
    assert by_key[("FUT_BR", 60)].requested_target_timeframes == "1h,4h"
    assert by_key[("FUT_BR", 24)].requested_target_timeframes == "1d"
    assert by_key[("FUT_BR", 7)].requested_target_timeframes == "1w"
