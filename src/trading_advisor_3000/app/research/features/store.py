from __future__ import annotations


def phase2b_feature_store_contract() -> dict[str, dict[str, object]]:
    return {
        "feature_snapshots": {
            "format": "delta",
            "partition_by": ["contract_id", "timeframe", "feature_set_version"],
            "constraints": ["unique(contract_id, timeframe, ts, feature_set_version)"],
            "columns": {
                "snapshot_id": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "ts": "timestamp",
                "feature_set_version": "string",
                "regime": "string",
                "atr": "double",
                "ema_fast": "double",
                "ema_slow": "double",
                "donchian_high": "double",
                "donchian_low": "double",
                "rvol": "double",
                "features_json": "json",
            },
        },
        "research_backtest_runs": {
            "format": "delta",
            "partition_by": ["strategy_version_id", "dataset_version"],
            "columns": {
                "backtest_run_id": "string",
                "strategy_version_id": "string",
                "dataset_version": "string",
                "started_at": "timestamp",
                "finished_at": "timestamp",
                "status": "string",
                "params_hash": "string",
                "candidate_count": "bigint",
            },
        },
        "research_signal_candidates": {
            "format": "delta",
            "partition_by": ["strategy_version_id", "contract_id", "timeframe"],
            "columns": {
                "candidate_id": "string",
                "backtest_run_id": "string",
                "strategy_version_id": "string",
                "contract_id": "string",
                "timeframe": "string",
                "ts_signal": "timestamp",
                "side": "string",
                "entry_ref": "double",
                "stop_ref": "double",
                "target_ref": "double",
                "score": "double",
            },
        },
    }
