from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.derived_indicators import build_derived_indicator_frames
from trading_advisor_3000.product_plane.research.indicators import build_indicator_frames


EXPECTED_CONTRACT_INDICATOR_SHA256 = "f86dc0c14d1ba479cffb6efaf642ee6b2bd278f0237eb4806dfe80430313abcc"
EXPECTED_CONTRACT_DERIVED_SHA256 = "14e664819f1bd578f99b674aa05f79285b8d7f1d2c867f8c9bbe53158a23d9d9"


def _normalize(value: object) -> object:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 12)
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    return value


def _bar(*, ts_index: int, close: float, timeframe: str) -> ResearchBarView:
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    if timeframe == "15m":
        ts = start + timedelta(minutes=15 * ts_index)
    elif timeframe == "1h":
        ts = start + timedelta(hours=ts_index)
    else:
        raise AssertionError(f"unexpected timeframe: {timeframe}")

    ts_text = ts.isoformat().replace("+00:00", "Z")
    wave = ((ts_index % 17) - 8) * 0.035
    volume = 1000 + ts_index * 7 + (ts_index % 5) * 13
    return ResearchBarView(
        dataset_version="legacy-compat-v1",
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe=timeframe,
        ts=ts_text,
        open=close - 0.21 + wave,
        high=close + 0.55 + abs(wave),
        low=close - 0.61 - abs(wave),
        close=close,
        volume=volume,
        open_interest=20000 + ts_index * 3,
        session_date=ts_text[:10],
        session_open_ts=f"{ts_text[:10]}T09:00:00Z",
        session_close_ts=f"{ts_text[:10]}T23:45:00Z",
        active_contract_id="BR-6.26",
        ret_1=None if ts_index == 0 else 0.001,
        log_ret_1=None if ts_index == 0 else 0.0009995,
        true_range=1.16 + abs(wave),
        hl_range=1.16 + abs(wave),
        oc_range=0.21,
        bar_index=ts_index,
        slice_role="analysis",
    )


def _payload_hash(rows: list[object]) -> str:
    payload = []
    for row in rows:
        data = row.to_dict()
        data.pop("created_at", None)
        payload.append(_normalize(data))
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def test_contract_mode_indicator_and_derived_outputs_match_legacy_hashes() -> None:
    bars_15m = [
        _bar(ts_index=index, close=80.0 + index * 0.07 + ((index % 11) - 5) * 0.025, timeframe="15m")
        for index in range(260)
    ]
    bars_1h = [
        _bar(ts_index=index, close=81.0 + index * 0.11 + ((index % 9) - 4) * 0.04, timeframe="1h")
        for index in range(220)
    ]
    bars = [*bars_15m, *bars_1h]

    indicators = build_indicator_frames(
        dataset_version="legacy-compat-v1",
        indicator_set_version="indicators-v1",
        bar_views=bars,
        series_mode="contract",
    )
    derived = build_derived_indicator_frames(
        dataset_version="legacy-compat-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=bars,
        indicator_rows=indicators,
        series_mode="contract",
    )

    assert len(indicators) == 480
    assert len(derived) == 480
    assert _payload_hash(indicators) == EXPECTED_CONTRACT_INDICATOR_SHA256
    assert _payload_hash(derived) == EXPECTED_CONTRACT_DERIVED_SHA256
