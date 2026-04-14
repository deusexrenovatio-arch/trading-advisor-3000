from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.product_plane.runtime.analytics import run_system_shadow_replay


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


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_phase3_integrated_replay_produces_traceable_runtime_bound_outcomes(tmp_path: Path) -> None:
    bars = _build_bars()
    report = run_system_shadow_replay(
        bars=bars,
        instrument_by_contract=_instrument_map(),
        strategy_version_id="trend-follow-v1",
        dataset_version="bars-whitelist-v1",
        output_dir=tmp_path,
        telegram_channel="@ta3000_signals",
        horizon_bars=2,
        runtime_allowed_contracts={"BR-6.26"},
    )

    assert report["signal_candidates"] > 0
    assert report["runtime_signal_candidates"] > 0
    assert report["runtime_signal_candidates"] < report["signal_candidates"]
    assert report["runtime_report"]["accepted_unique_signals"] == report["runtime_signal_candidates"]
    assert report["analytics_outcomes"] == report["forward_observations"]
    assert report["forward_observations"] == report["runtime_signal_candidates"]

    assert report["runtime_payload"]["publications"]
    assert all(row["mode"] == "shadow" for row in report["analytics_rows"])
    assert "analytics_signal_outcomes" in report["delta_manifest"]
    assert "research_forward_observations" in report["delta_manifest"]
    assert set(report["runtime_signal_ids"]) == {row["signal_id"] for row in report["runtime_payload"]["publications"]}

    research_rows = read_delta_table_rows(Path(str(report["output_paths"]["signal_candidates"])))
    forward_rows = _load_jsonl(Path(str(report["output_paths"]["research_forward_observations"])))
    expected_candidate_ids = set(report["runtime_candidate_ids"])
    actual_forward_candidate_ids = {str(row["candidate_id"]) for row in forward_rows}
    assert actual_forward_candidate_ids == expected_candidate_ids
    assert expected_candidate_ids <= {str(row["candidate_id"]) for row in research_rows}

    for path_text in report["output_paths"].values():
        assert Path(path_text).exists()
