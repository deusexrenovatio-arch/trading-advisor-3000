from __future__ import annotations

from trading_advisor_3000.app.contracts import CanonicalBar


def build_canonical_bars(rows: list[dict[str, object]]) -> list[CanonicalBar]:
    dedup: dict[tuple[str, str, str], CanonicalBar] = {}
    for row in rows:
        bar = CanonicalBar.from_dict(row)
        key = (bar.contract_id, bar.timeframe.value, bar.ts_open)
        dedup[key] = bar
    return sorted(dedup.values(), key=lambda item: (item.contract_id, item.timeframe.value, item.ts_open))
