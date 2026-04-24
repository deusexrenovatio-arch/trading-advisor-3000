from __future__ import annotations

import json
import platform
import sys
from pathlib import Path
from typing import Any

from deltalake import DeltaTable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows
from trading_advisor_3000.product_plane.research.registry_store import research_registry_store_contract
from trading_advisor_3000.product_plane.research.backtests import backtest_store_contract, results_store_contract
from trading_advisor_3000.product_plane.research.datasets import research_dataset_store_contract
from trading_advisor_3000.product_plane.research.features import research_feature_store_contract
from trading_advisor_3000.product_plane.research.indicators import indicator_store_contract


def jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(item) for item in value]
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return jsonable(value.to_dict())
    return value


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(jsonable(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def delta_row_count(path: Path) -> int:
    return len(read_delta_table_rows(path)) if has_delta_log(path) else 0


def delta_version_count(path: Path) -> int:
    log_dir = path / "_delta_log"
    if not log_dir.exists():
        return 0
    return len(list(log_dir.glob("*.json")))


def runtime_profile() -> dict[str, str]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "processor": platform.processor() or "unknown",
    }


def print_summary(report: dict[str, Any]) -> None:
    print(json.dumps(jsonable(report), ensure_ascii=False))


def research_contract_manifest() -> dict[str, dict[str, object]]:
    return {
        **{
            table_name: research_registry_store_contract()[table_name]
            for table_name in (
                "research_strategy_families",
                "research_strategy_templates",
                "research_strategy_template_modules",
                "research_strategy_instances",
                "research_strategy_instance_modules",
            )
        },
        **research_dataset_store_contract(),
        **indicator_store_contract(),
        **research_feature_store_contract(),
        **backtest_store_contract(),
        **results_store_contract(),
    }


def validate_research_contracts(
    *,
    output_paths: dict[str, object],
    materialized_assets: list[str],
    rows_by_table: dict[str, object],
) -> dict[str, object]:
    manifest = research_contract_manifest()
    validated_tables: list[str] = []
    warnings: list[str] = []
    errors: list[str] = []
    actual_row_counts: dict[str, int] = {}

    for table_name in materialized_assets:
        expected = manifest.get(table_name)
        if expected is None:
            errors.append(f"missing contract manifest for `{table_name}`")
            continue
        path_value = output_paths.get(table_name)
        if not path_value:
            errors.append(f"missing output path for `{table_name}`")
            continue
        table_path = Path(str(path_value))
        if not has_delta_log(table_path):
            errors.append(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
            continue

        delta_table = DeltaTable(str(table_path))
        actual_columns = list(delta_table.to_pyarrow_table().schema.names)
        expected_columns = list(dict(expected["columns"]).keys())
        missing_columns = [column for column in expected_columns if column not in actual_columns]
        extra_columns = [column for column in actual_columns if column not in expected_columns]
        if missing_columns or extra_columns:
            details: list[str] = []
            if missing_columns:
                details.append(f"missing columns: {', '.join(missing_columns)}")
            if extra_columns:
                details.append(f"extra columns: {', '.join(extra_columns)}")
            errors.append(f"schema mismatch for `{table_name}` ({'; '.join(details)})")

        actual_row_count = len(read_delta_table_rows(table_path))
        actual_row_counts[table_name] = actual_row_count
        reported_row_count = rows_by_table.get(table_name)
        if reported_row_count is not None and int(reported_row_count) != actual_row_count:
            errors.append(
                f"row count mismatch for `{table_name}`: report={int(reported_row_count)} actual={actual_row_count}"
            )
        if actual_row_count == 0:
            warnings.append(f"`{table_name}` materialized with 0 rows")
        validated_tables.append(table_name)

    return {
        "status": "failed" if errors else "passed",
        "validated_tables": validated_tables,
        "warnings": warnings,
        "errors": errors,
        "row_counts": actual_row_counts,
    }
