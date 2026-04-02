from __future__ import annotations

from trading_advisor_3000.product_plane.contracts import CanonicalBar


def run_data_quality_checks(
    bars: list[CanonicalBar],
    *,
    whitelist_contracts: set[str],
) -> list[str]:
    errors: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    last_ts: dict[tuple[str, str], str] = {}

    for index, bar in enumerate(bars):
        if bar.contract_id not in whitelist_contracts:
            errors.append(f"row[{index}] contract outside whitelist: {bar.contract_id}")

        key = (bar.contract_id, bar.timeframe.value, bar.ts)
        if key in seen:
            errors.append(f"duplicate key: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
        seen.add(key)

        sequence_key = (bar.contract_id, bar.timeframe.value)
        previous_ts = last_ts.get(sequence_key)
        if previous_ts is not None and bar.ts < previous_ts:
            errors.append(f"non-monotonic timeline: {bar.contract_id}/{bar.timeframe.value}")
        last_ts[sequence_key] = bar.ts

    return errors
