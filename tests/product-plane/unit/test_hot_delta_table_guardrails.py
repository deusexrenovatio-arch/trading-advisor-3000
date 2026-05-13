from __future__ import annotations

import ast
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.hot_delta_tables import (
    HOT_DELTA_TABLE_FILENAMES,
)

SOURCE_ROOT = Path(__file__).parents[3] / "src" / "trading_advisor_3000"

UNBOUNDED_READ_APIS = {
    "iter_delta_table_row_batches",
    "read_delta_table_frame",
    "read_delta_table_rows",
}
FULL_WRITE_APIS = {"write_delta_table_rows"}
BOUNDED_READ_KEYWORDS = {"filters", "limit"}

HOT_TABLE_ALIASES = {
    filename.removesuffix(".delta"): filename for filename in HOT_DELTA_TABLE_FILENAMES
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
        return aliases.get(text) or (
            Path(text).name if Path(text).name in HOT_DELTA_TABLE_FILENAMES else None
        )

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
    return any(
        keyword.arg in BOUNDED_READ_KEYWORDS and _has_real_bound_value(keyword.value)
        for keyword in call.keywords
    )


def _has_real_bound_value(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant) and node.value is None:
        return False
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)) and not node.elts:
        return False
    if isinstance(node, ast.Dict) and not node.keys:
        return False
    return True


class _HotDeltaAccessVisitor(ast.NodeVisitor):
    def __init__(self, *, source_path: Path) -> None:
        self.source_path = source_path
        self.violations: list[str] = []
        self._aliases_stack: list[dict[str, str]] = [dict(HOT_TABLE_ALIASES)]

    @property
    def aliases(self) -> dict[str, str]:
        return self._aliases_stack[-1]

    def _relative_source_path(self) -> Path:
        return self.source_path.relative_to(SOURCE_ROOT.parent)

    def _visit_scoped_body(self, body: list[ast.stmt]) -> None:
        self._aliases_stack.append(dict(self.aliases))
        try:
            for statement in body:
                self.visit(statement)
        finally:
            self._aliases_stack.pop()

    def _set_alias_targets(self, targets: list[ast.expr], filename: str | None) -> None:
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            if filename:
                self.aliases[target.id] = filename
            else:
                self.aliases.pop(target.id, None)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_scoped_body(node.body)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_scoped_body(node.body)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._visit_scoped_body(node.body)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        self._set_alias_targets(node.targets, _hot_table_filename(node.value, self.aliases))

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is None:
            return
        self.visit(node.value)
        self._set_alias_targets([node.target], _hot_table_filename(node.value, self.aliases))

    def visit_Call(self, node: ast.Call) -> None:
        call_name = _call_name(node.func)
        table_arg = (
            node.args[0]
            if node.args
            else next(
                (keyword.value for keyword in node.keywords if keyword.arg == "table_path"),
                None,
            )
        )
        if table_arg is not None:
            filename = _hot_table_filename(table_arg, self.aliases)
            if filename is not None:
                if call_name in UNBOUNDED_READ_APIS and not _is_bounded_read(node):
                    self.violations.append(
                        f"{self._relative_source_path()}:{node.lineno}: "
                        f"unbounded {call_name}({filename})"
                    )
                if call_name in FULL_WRITE_APIS:
                    self.violations.append(
                        f"{self._relative_source_path()}:{node.lineno}: "
                        f"full {call_name}({filename})"
                    )
        self.generic_visit(node)


def test_bounded_read_requires_real_filter_or_limit_value() -> None:
    def is_bounded(source: str) -> bool:
        call = ast.parse(source).body[0].value
        assert isinstance(call, ast.Call)
        return _is_bounded_read(call)

    assert not is_bounded("read_delta_table_rows(path)")
    assert not is_bounded("read_delta_table_rows(path, filters=None)")
    assert not is_bounded("read_delta_table_rows(path, filters=[])")
    assert is_bounded("read_delta_table_rows(path, filters=filters)")
    assert is_bounded("read_delta_table_rows(path, filters=[('id', '=', 'a')])")
    assert is_bounded("read_delta_table_rows(path, limit=1)")


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
        visitor = _HotDeltaAccessVisitor(source_path=path)
        visitor.visit(tree)
        violations.extend(visitor.violations)

    assert violations == []
