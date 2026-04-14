from __future__ import annotations

import json
import platform
import sys
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows


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
