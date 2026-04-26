from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar, DecisionCandidate
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.product_plane.research import run_research_from_bars


def _instrument_map() -> dict[str, str]:
    return {"BR-6.26": "BR", "Si-6.26": "Si"}


def _build_bars(*, bars_per_contract: int = 72) -> list[CanonicalBar]:
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    specs = (
        ("BR-6.26", "BR", 82.0, 0.22),
        ("Si-6.26", "Si", 91_800.0, 55.0),
    )
    bars: list[CanonicalBar] = []
    for contract_id, instrument_id, base_close, step in specs:
        for index in range(bars_per_contract):
            ts = (start + timedelta(minutes=15 * index)).isoformat().replace("+00:00", "Z")
            if index < bars_per_contract // 3:
                close = base_close + (index * step)
            elif index < (2 * bars_per_contract) // 3:
                close = base_close + ((bars_per_contract // 3) * step) - ((index - (bars_per_contract // 3)) * step * 1.15)
            else:
                close = (
                    base_close
                    + ((bars_per_contract // 3) * step)
                    - ((bars_per_contract // 3) * step * 1.15)
                    + ((index - ((2 * bars_per_contract) // 3)) * step * 1.35)
                )
            open_price = close - (0.35 * step)
            high = max(open_price, close) + (0.75 * step)
            low = min(open_price, close) - (0.85 * step)
            volume = 1_000 + (index * 20) + (120 if index % 7 == 0 else 0)
            if instrument_id == "Si":
                volume += 300
            bars.append(
                CanonicalBar.from_dict(
                    {
                        "contract_id": contract_id,
                        "instrument_id": instrument_id,
                        "timeframe": "15m",
                        "ts": ts,
                        "open": round(open_price, 6),
                        "high": round(high, 6),
                        "low": round(low, 6),
                        "close": round(close, 6),
                        "volume": int(volume),
                        "open_interest": 20_000 + index,
                    }
                )
            )
    return sorted(bars, key=lambda item: (item.contract_id, item.timeframe.value, item.ts))


def test_materialized_research_backtest_is_reproducible_and_writes_outputs(tmp_path: Path) -> None:
    bars = _build_bars()
    instrument_map = _instrument_map()

    run_a = run_research_from_bars(
        bars=bars,
        instrument_by_contract=instrument_map,
        strategy_version_id="ma-cross-v1",
        dataset_version="bars-whitelist-v1",
        output_dir=tmp_path / "run-a",
    )
    run_b = run_research_from_bars(
        bars=bars,
        instrument_by_contract=instrument_map,
        strategy_version_id="ma-cross-v1",
        dataset_version="bars-whitelist-v1",
        output_dir=tmp_path / "run-b",
    )

    assert run_a["backtest_run"]["backtest_run_id"] == run_b["backtest_run"]["backtest_run_id"]
    assert run_a["signal_contract_rows"] == run_b["signal_contract_rows"]
    assert run_a["delta_manifest"]["research_derived_indicator_frames"]["format"] == "delta"
    assert run_a["primary_path"] == "materialized_research"

    for path_text in run_a["output_paths"].values():
        path = Path(path_text)
        assert path.exists()
        assert (path / "_delta_log").exists()

    assert run_a["signal_contracts"] > 0
    for row in run_a["signal_contract_rows"]:
        contract = DecisionCandidate.from_dict(row)
        assert contract.to_dict() == row


def test_materialized_research_backtest_supports_walk_forward_costs_and_strategy_metrics(tmp_path: Path) -> None:
    bars = _build_bars()
    report = run_research_from_bars(
        bars=bars,
        instrument_by_contract=_instrument_map(),
        strategy_version_id="ma-cross-v1",
        dataset_version="bars-whitelist-v1",
        output_dir=tmp_path / "run-wf",
        backtest_config={
            "walk_forward_windows": 2,
            "commission_per_trade": 0.25,
            "slippage_bps": 5.0,
            "session_hours_utc": (9, 23),
        },
    )

    metrics = report["strategy_metrics"]
    assert metrics["walk_forward_windows"] == 2
    assert metrics["commission_total"] >= 0.0
    assert metrics["slippage_total"] >= 0.0
    assert "avg_score" in metrics
    assert "avg_risk_reward" in metrics

    candidate_rows = read_delta_table_rows(Path(str(report["output_paths"]["signal_candidates"])))
    assert candidate_rows
    assert all("window_id" in row for row in candidate_rows)
    assert all("estimated_commission" in row for row in candidate_rows)
    assert all("estimated_slippage" in row for row in candidate_rows)


