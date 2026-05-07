from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TMP = ROOT / ".tmp"
SRC = ROOT / "src"
TMP.mkdir(exist_ok=True)
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        marker = _marker_for_path(Path(str(item.fspath)))
        if marker and not item.get_closest_marker(marker):
            item.add_marker(getattr(pytest.mark, marker))


def _marker_for_path(path: Path) -> str:
    normalized_parts = {part.lower() for part in path.parts}
    normalized_text = path.as_posix().lower()

    if "integration" in normalized_parts:
        return "integration"
    if "contracts" in normalized_parts or "architecture" in normalized_parts:
        return "contract"
    if "/tests/process/" in normalized_text:
        return "contract"
    return "unit"
