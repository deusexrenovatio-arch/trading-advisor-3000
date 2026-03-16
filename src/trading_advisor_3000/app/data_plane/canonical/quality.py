from __future__ import annotations

from trading_advisor_3000.app.contracts import CanonicalBar


def run_data_quality_checks(
    bars: list[CanonicalBar],
    *,
    whitelist_contracts: set[str],
) -> list[str]:
    errors: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    last_close: dict[tuple[str, str], str] = {}

    for index, bar in enumerate(bars):
        if bar.contract_id not in whitelist_contracts:
            errors.append(f"row[{index}] contract outside whitelist: {bar.contract_id}")

        key = (bar.contract_id, bar.timeframe.value, bar.ts_open)
        if key in seen:
            errors.append(f"duplicate key: {bar.contract_id}/{bar.timeframe.value}/{bar.ts_open}")
        seen.add(key)

        sequence_key = (bar.contract_id, bar.timeframe.value)
        previous_close = last_close.get(sequence_key)
        if previous_close is not None and bar.ts_open < previous_close:
            errors.append(f"non-monotonic timeline: {bar.contract_id}/{bar.timeframe.value}")
        last_close[sequence_key] = bar.ts_close

    return errors
