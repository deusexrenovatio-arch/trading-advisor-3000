from __future__ import annotations

import ast
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.hot_delta_tables import (
    HOT_DELTA_TABLE_FILENAMES,
)


SOURCE_ROOT = Path(__file__).parents[3] / "src" / "trading_advisor_3000"

UNBOUNDED_READ_APIS = {"read_delta_table_frame", "read_delta_table_rows"}
FULL_WRITE_APIS = {"write_delta_table_rows"}
BOUNDED_READ_KEYWORDS = {"filters", "limit"}

HOT_TABLE_ALIASES = {
    filename.removesuffix(".delta"): filename
    for filename in HOT_DELTA_TABLE_FILENAMES
}
HOT_TABLE_ALIASES.update(
    {
        "canonical_bar_provenance": "canonical_bar_provenance.delta",
        "canonical_provenance": "canonical_bar_provenance.delta",
        "raw_moex": "raw_moex_history.delta",
    }
)


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _string_value(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _hot_table_filename(node: ast.AST, aliases: dict[str, str]) -> str | None:
    text = _string_value(node)
    if text is not None:
        return aliases.get(text) or (Path(text).name if Path(text).name in HOT_DELTA_TABLE_FILENAMES else None)

    if isinstance(node, ast.Name):
        return aliases.get(node.id)

    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        return _hot_table_filename(node.right, aliases) or _hot_table_filename(node.left, aliases)

    if isinstance(node, ast.Call):
        call_name = _call_name(node.func)
        if call_name == "Path" and node.args:
            return _hot_table_filename(node.args[0], aliases)
        if call_name == "_canonical_table_path" and len(node.args) >= 2:
            table_name = _string_value(node.args[1])
            if table_name:
                return aliases.get(table_name)

    return None


def _is_bounded_read(call: ast.Call) -> bool:
    return any(keyword.arg in BOUNDED_READ_KEYWORDS and keyword.value is not None for keyword in call.keywords)


def test_hot_delta_table_inventory_is_explicit() -> None:
    expected = {
        "raw_moex_history.delta",
        "canonical_bars.delta",
        "canonical_bar_provenance.delta",
        "canonical_session_calendar.delta",
        "canonical_roll_map.delta",
        "research_bar_views.delta",
        "research_indicator_frames.delta",
        "research_derived_indicator_frames.delta",
        "continuous_front_bars.delta",
        "continuous_front_adjustment_ladder.delta",
        "continuous_front_indicator_frames.delta",
        "continuous_front_derived_indicator_frames.delta",
    }

    assert expected.issubset(HOT_DELTA_TABLE_FILENAMES)


def test_hot_delta_tables_do_not_use_full_python_reads_or_writes() -> None:
    violations: list[str] = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        aliases = dict(HOT_TABLE_ALIASES)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                filename = _hot_table_filename(node.value, aliases)
                if filename:
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            aliases[target.id] = filename
            if not isinstance(node, ast.Call):
                continue

            call_name = _call_name(node.func)
            table_arg = node.args[0] if node.args else next(
                (keyword.value for keyword in node.keywords if keyword.arg == "table_path"),
                None,
            )
            if table_arg is None:
                continue

            filename = _hot_table_filename(table_arg, aliases)
            if filename is None:
                continue

            if call_name in UNBOUNDED_READ_APIS and not _is_bounded_read(node):
                relative = path.relative_to(SOURCE_ROOT.parent)
                violations.append(f"{relative}:{node.lineno}: unbounded {call_name}({filename})")
            if call_name in FULL_WRITE_APIS:
                relative = path.relative_to(SOURCE_ROOT.parent)
                violations.append(f"{relative}:{node.lineno}: full {call_name}({filename})")

    assert violations == []
