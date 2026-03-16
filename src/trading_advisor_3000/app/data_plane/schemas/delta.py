from __future__ import annotations


def phase2a_delta_schema_manifest() -> dict[str, dict[str, object]]:
    return {
        "raw_market_backfill": {
            "format": "delta",
            "partition_by": ["contract_id", "timeframe"],
            "columns": {
                "contract_id": "string",
                "timeframe": "string",
                "ts_open": "timestamp",
                "ts_close": "timestamp",
                "open": "double",
                "high": "double",
                "low": "double",
                "close": "double",
                "volume": "bigint",
            },
        },
        "canonical_bars": {
            "format": "delta",
            "partition_by": ["contract_id", "timeframe"],
            "constraints": ["unique(contract_id, timeframe, ts_open)"],
            "columns": {
                "contract_id": "string",
                "timeframe": "string",
                "ts_open": "timestamp",
                "ts_close": "timestamp",
                "open": "double",
                "high": "double",
                "low": "double",
                "close": "double",
                "volume": "bigint",
            },
        },
    }
