from __future__ import annotations


def phase2a_delta_schema_manifest() -> dict[str, dict[str, object]]:
    return {
        "raw_market_backfill": {
            "format": "delta",
            "partition_by": ["contract_id", "timeframe"],
            "columns": {
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "ts_open": "timestamp",
                "ts_close": "timestamp",
                "open": "double",
                "high": "double",
                "low": "double",
                "close": "double",
                "volume": "bigint",
                "open_interest": "bigint",
            },
        },
        "canonical_bars": {
            "format": "delta",
            "partition_by": ["contract_id", "timeframe"],
            "constraints": ["unique(contract_id, timeframe, ts)"],
            "columns": {
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "ts": "timestamp",
                "open": "double",
                "high": "double",
                "low": "double",
                "close": "double",
                "volume": "bigint",
                "open_interest": "bigint",
            },
        },
        "canonical_instruments": {
            "format": "delta",
            "partition_by": [],
            "constraints": ["unique(instrument_id)"],
            "columns": {
                "instrument_id": "string",
            },
        },
        "canonical_contracts": {
            "format": "delta",
            "partition_by": ["instrument_id"],
            "constraints": ["unique(contract_id)"],
            "columns": {
                "contract_id": "string",
                "instrument_id": "string",
                "first_seen_ts": "timestamp",
                "last_seen_ts": "timestamp",
            },
        },
        "canonical_session_calendar": {
            "format": "delta",
            "partition_by": ["instrument_id", "session_date"],
            "constraints": ["unique(instrument_id, timeframe, session_date)"],
            "columns": {
                "instrument_id": "string",
                "timeframe": "string",
                "session_date": "date",
                "session_open_ts": "timestamp",
                "session_close_ts": "timestamp",
            },
        },
        "canonical_roll_map": {
            "format": "delta",
            "partition_by": ["instrument_id", "session_date"],
            "constraints": ["unique(instrument_id, session_date)"],
            "columns": {
                "instrument_id": "string",
                "session_date": "date",
                "active_contract_id": "string",
                "reason": "string",
            },
        },
    }
