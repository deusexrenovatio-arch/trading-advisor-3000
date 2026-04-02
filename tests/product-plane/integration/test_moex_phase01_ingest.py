from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex.foundation import load_mapping_registry, run_phase01_foundation
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import CandleBorder, MoexCandle


class _FakeMoexClient:
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
        return {
            interval: CandleBorder(
                interval=interval,
                begin="2026-03-20 10:00:00",
                end="2026-04-02 12:00:00",
            )
            for interval in required_intervals
        }

    def fetch_candles(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        interval: int,
        date_from,
        date_till,
    ) -> list[MoexCandle]:
        del engine, market, board, secid, interval, date_from, date_till
        return [
            MoexCandle(
                open=100.0,
                high=101.0,
                low=99.5,
                close=100.5,
                volume=50,
                begin="2026-04-01 10:00:00",
                end="2026-04-01 10:09:59",
            ),
            MoexCandle(
                open=100.5,
                high=101.5,
                low=100.4,
                close=101.2,
                volume=75,
                begin="2026-04-01 10:10:00",
                end="2026-04-01 10:19:59",
            ),
            # same key timestamp with changed values; must be filtered by watermark
            MoexCandle(
                open=100.0,
                high=102.0,
                low=98.0,
                close=99.0,
                volume=70,
                begin="2026-04-01 10:00:00",
                end="2026-04-01 10:09:59",
            ),
        ]


def test_phase01_foundation_ingest_is_idempotent_and_watermark_safe(tmp_path: Path) -> None:
    mapping_registry = Path("configs/moex_phase01/instrument_mapping_registry.v1.yaml")
    universe = Path("configs/moex_phase01/universe/moex-futures-priority.v1.yaml")
    client = _FakeMoexClient()
    active_internal_ids = {
        row.internal_id
        for row in load_mapping_registry(mapping_registry)
        if row.is_active
    }
    expected_rows = len(active_internal_ids) * 2
    expected_stale = len(active_internal_ids)

    pass1 = run_phase01_foundation(
        mapping_registry_path=mapping_registry,
        universe_path=universe,
        output_dir=tmp_path,
        run_id="phase01-pass1",
        timeframes={"15m"},
        bootstrap_window_days=3,
        ingest_till_utc="2026-04-02T12:00:00Z",
        client=client,
    )
    assert pass1.incremental_rows == expected_rows
    assert pass1.stale_rows == expected_stale

    pass2 = run_phase01_foundation(
        mapping_registry_path=mapping_registry,
        universe_path=universe,
        output_dir=tmp_path,
        run_id="phase01-pass2",
        timeframes={"15m"},
        bootstrap_window_days=3,
        ingest_till_utc="2026-04-02T12:00:00Z",
        client=client,
    )
    assert pass2.incremental_rows == 0
    assert pass2.deduplicated_rows >= expected_rows
    assert pass2.stale_rows >= expected_stale

    raw_rows = read_delta_table_rows(Path(pass2.raw_table_path))
    assert len(raw_rows) == expected_rows
    assert {row["internal_id"] for row in raw_rows} == active_internal_ids
    assert {row["timeframe"] for row in raw_rows} == {"10m"}
    assert {row["source_interval"] for row in raw_rows} == {10}
    requested_target_sets: set[str] = set()
    for row in raw_rows:
        provenance = row["provenance_json"]
        if isinstance(provenance, str):
            provenance = json.loads(provenance)
        requested_target_sets.add(provenance["requested_target_timeframes"])
    assert requested_target_sets == {"15m"}
