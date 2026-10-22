"""Bounded Python symbol fingerprints, independent of application packaging.

This is a syntactic numerical boundary, not an algebraic equivalence checker.
Unknown project edges fail closed. Native IO APIs are explicit leaf boundaries.
"""

from __future__ import annotations

import ast
import builtins
import copy
import hashlib
import importlib.util
import json
import symtable
from pathlib import Path

FORMAT = "research-symbol-ast.v1"
PACKAGE = "trading_advisor_3000"

_EXTERNAL_ROOTS = frozenset(
    {
        "bisect",
        "collections",
        "contextvars",
        "dataclasses",
        "datetime",
        "enum",
        "fnmatch",
        "functools",
        "hashlib",
        "importlib",
        "inspect",
        "itertools",
        "json",
        "math",
        "numbers",
        "pathlib",
        "re",
        "statistics",
        "struct",
        "typing",
        "zoneinfo",
        "numpy",
        "pandas",
        "pandas_ta_classic",
        "pyarrow",
        "pyspark",
        "talib",
    }
)


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


class ContractError(ValueError):
    """An unproven compatibility boundary must not authorize reuse."""


class _Canonical(ast.NodeTransformer):
    def visit_ClassDef(self, node):
        if node.keywords:
            raise ContractError("unbound class metaclass")
        for decorator in node.decorator_list:
            function = decorator.func if isinstance(decorator, ast.Call) else decorator
            if not isinstance(function, ast.Name) or function.id != "dataclass":
                raise ContractError("order-sensitive class decorator")
        for base in node.bases:
            base = base.value if isinstance(base, ast.Subscript) else base
            if not isinstance(base, ast.Name) or base.id not in {
                "object",
                "Sequence",
                "Mapping",
                "Generic",
                "Enum",
                "str",
                "int",
                "tuple",
                "dict",
                "list",
                "Exception",
                "RuntimeError",
            }:
                raise ContractError("unbound class base order")
        methods = [
            item for item in node.body if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        method_names = {item.name for item in methods}
        if len(method_names) != len(methods):
            raise ContractError("ambiguous class method binding")
        for method in methods:
            if any(
                not isinstance(item, ast.Name)
                or item.id not in {"classmethod", "staticmethod", "property"}
                for item in method.decorator_list
            ):
                raise ContractError("order-sensitive method decorator")
            defaults = [*method.args.defaults, *(item for item in method.args.kw_defaults if item)]
            if any(
                isinstance(item, ast.Name) and item.id in method_names
                for default in defaults
                for item in ast.walk(default)
            ):
                raise ContractError("order-sensitive class method default")
        if any(
            not isinstance(
                item,
                (
                    ast.FunctionDef,
                    ast.AsyncFunctionDef,
                    ast.Assign,
                    ast.AnnAssign,
                    ast.Expr,
                    ast.Pass,
                ),
            )
            for item in node.body
        ):
            raise ContractError("imperative class body")
        for item in node.body:
            if isinstance(item, (ast.Assign, ast.AnnAssign)):
                targets = item.targets if isinstance(item, ast.Assign) else [item.target]
                if any(
                    isinstance(part, ast.Name) and part.id in method_names
                    for target in targets
                    for part in ast.walk(target)
                ):
                    raise ContractError("ambiguous class field/method binding")
            if isinstance(item, ast.Expr) and not (
                isinstance(item.value, ast.Constant) and isinstance(item.value.value, str)
            ):
                raise ContractError("imperative class expression")
            if isinstance(item, (ast.Assign, ast.AnnAssign)) and any(
                isinstance(part, ast.Name)
                and isinstance(part.ctx, ast.Load)
                and part.id in method_names
                for part in ast.walk(item)
            ):
                raise ContractError("order-sensitive class data binding")
        if any(
            isinstance(item, ast.Attribute) and item.attr == "__dict__" for item in ast.walk(node)
        ):
            raise ContractError("reflective class order")
        # Keep data field order; independent method declarations have no order.
        node.body = [item for item in node.body if item not in methods] + sorted(
            methods, key=lambda item: item.name
        )
        return self.generic_visit(node)

    def visit_FunctionDef(self, node):
        self.generic_visit(node)
        node.returns = None
        node.type_comment = None
        for arg in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
            arg.annotation = None
        for arg in (node.args.vararg, node.args.kwarg):
            if arg:
                arg.annotation = None
        return node

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_AnnAssign(self, node):
        # Dataclass field order, types and defaults remain part of the model.
        return self.generic_visit(node)

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            return None
        return self.generic_visit(node)


def _observation_only(node: ast.AST) -> ast.AST:
    """Check the sole admitted observer API; remove complete side effects only."""
    if not isinstance(node, ast.FunctionDef) or node.name != "_compute_cached_profile_frame":
        return node
    observer = {"diagnostics", "operations"}

    def names(statement):
        return {item.id for item in ast.walk(statement) if isinstance(item, ast.Name)}

    def erase(statements):
        result = []
        for statement in statements:
            if isinstance(statement, ast.Assign) and names(statement.targets[0]) & observer:
                if len(statement.targets) != 1:
                    raise ContractError("observation has multiple targets")
                target = statement.targets[0]
                direct = (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.value, ast.Name)
                    and isinstance(target.slice, ast.Constant)
                    and isinstance(target.slice.value, str)
                )
                alias = isinstance(target, ast.Name) and target.id == "operations"
                if not (direct and target.value.id == "diagnostics" or alias):
                    raise ContractError("unsupported observation target")
                calls = [item for item in ast.walk(statement.value) if isinstance(item, ast.Call)]
                for call in calls:
                    if isinstance(call.func, ast.Name) and call.func.id == "int":
                        continue
                    if not (
                        isinstance(call.func, ast.Attribute)
                        and isinstance(call.func.value, ast.Name)
                        and call.func.value.id == "diagnostics"
                        and call.func.attr in {"get", "setdefault"}
                    ):
                        raise ContractError("observation contains non-observer call")
                if not names(statement.value) <= {"diagnostics", "int"}:
                    raise ContractError("observation alias or increment has unknown input")
                continue
            if isinstance(statement, ast.If) and "operations" in names(statement.test):
                expected = ast.parse(
                    "if spec.operation_key not in operations:\n"
                    "    operations.append(spec.operation_key)\n"
                ).body[0]
                if ast.dump(statement, include_attributes=False) != ast.dump(
                    expected, include_attributes=False
                ):
                    raise ContractError("observation branch influences computation")
                continue
            if isinstance(statement, (ast.For, ast.If, ast.With, ast.Try)):
                for field in ("body", "orelse", "finalbody"):
                    if hasattr(statement, field):
                        setattr(statement, field, erase(getattr(statement, field)))
                for handler in getattr(statement, "handlers", ()):
                    handler.body = erase(handler.body)
            result.append(statement)
        return result

    node.body = erase(node.body)
    if any(isinstance(item, ast.Name) and item.id in observer for item in ast.walk(node)):
        raise ContractError("observer value enters computation")
    return node


class SymbolGraph:
    def __init__(self, package: Path, leaves: tuple[str, ...] = ()):
        self.package = package
        self.leaves = set(leaves)
        self.modules = {}
        self.selected = {}

    def _path(self, module):
        relative = module.removeprefix(PACKAGE).lstrip(".").replace(".", "/")
        path = self.package / relative
        module_file, package_file = path.with_suffix(".py"), path / "__init__.py"
        if module_file.is_file() and package_file.is_file():
            raise ContractError(f"ambiguous module/package binding: {module}")
        return module_file if module_file.is_file() else package_file

    def _module(self, module):
        if module in self.modules:
            return self.modules[module]
        path = self._path(module)
        if not path.is_file():
            raise ContractError(f"unresolved project module: {module}")
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        definitions, imports, ambiguous = {}, {}, set()
        bound = set()
        parent = module if path.name == "__init__.py" else module.rpartition(".")[0]
        for node in tree.body:
            declared = []
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                declared = [node.name]
                definitions[node.name] = node
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                for target in node.targets if isinstance(node, ast.Assign) else [node.target]:
                    for name in ast.walk(target):
                        if isinstance(name, ast.Name):
                            declared.append(name.id)
                            definitions[name.id] = node
            elif isinstance(node, ast.ImportFrom):
                base = (
                    importlib.util.resolve_name("." * node.level + (node.module or ""), parent)
                    if node.level
                    else node.module
                )
                for alias in node.names:
                    if alias.name == "*":
                        raise ContractError(f"star import in numerical owner: {module}")
                    imports[alias.asname or alias.name] = f"{base}.{alias.name}"
                    declared.append(alias.asname or alias.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imports[alias.asname or alias.name.split(".")[0]] = (
                        alias.name if alias.asname else alias.name.split(".")[0]
                    )
                    declared.append(alias.asname or alias.name.split(".")[0])
            else:
                if isinstance(node, ast.Expr) and not (
                    isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
                ):
                    raise ContractError(f"unsupported module side effect: {module}")
                # A conditional/repeated module binding has no single static authority.
                for item in ast.walk(node):
                    if isinstance(item, (ast.FunctionDef, ast.ClassDef)):
                        ambiguous.add(item.name)
                    elif isinstance(item, ast.Name) and isinstance(item.ctx, (ast.Store, ast.Del)):
                        ambiguous.add(item.id)
                    elif isinstance(item, (ast.ImportFrom, ast.Import)):
                        ambiguous.update(
                            alias.asname or alias.name.split(".")[0] for alias in item.names
                        )
            ambiguous.update(bound.intersection(declared))
            bound.update(declared)
        self.modules[module] = definitions, imports, ambiguous
        return definitions, imports, ambiguous

    def _module_uses(self, target, alias, node):
        parents = {
            child: parent for parent in ast.walk(node) for child in ast.iter_child_nodes(parent)
        }
        for item in ast.walk(node):
            if (
                not isinstance(item, ast.Name)
                or not isinstance(item.ctx, ast.Load)
                or item.id != alias
            ):
                continue
            reference, identity = item, target
            while (
                isinstance(parents.get(reference), ast.Attribute)
                and parents[reference].value is reference
            ):
                reference = parents[reference]
                identity += "." + reference.attr
            if identity == target or self._path(identity).is_file():
                raise ContractError(f"dynamic project module edge: {identity}")
            self._include(identity)

    def _external(self, identity, target):
        if target.split(".")[0] not in _EXTERNAL_ROOTS:
            raise ContractError(f"unbound external numerical provider: {target}")
        self.selected["external:" + identity] = target

    def _include(self, identity):
        if identity in self.selected:
            return
        if identity in self.leaves:
            self.selected[identity] = "native-boundary.v1"
            return
        module, _, name = identity.rpartition(".")
        definitions, imports, ambiguous = self._module(module)
        if name in ambiguous:
            raise ContractError(f"ambiguous project binding: {identity}")
        if name in imports:
            target = imports[name]
            if target == PACKAGE or target.startswith(PACKAGE + "."):
                self._include(target)
            else:
                self._external(identity, target)
            return
        if name not in definitions:
            raise ContractError(f"unresolved project symbol: {identity}")
        original = _observation_only(copy.deepcopy(definitions[name]))
        node = _Canonical().visit(copy.deepcopy(original))
        ast.fix_missing_locations(node)
        self.selected[identity] = ast.dump(node, include_attributes=False)
        source = ast.unparse(original)
        table = symtable.symtable(source, "<kernel>", "exec")
        global_names = set()

        def scan(scope):
            for symbol in scope.get_symbols():
                if symbol.is_referenced() and symbol.is_global():
                    global_names.add(symbol.get_name())
            for child in scope.get_children():
                scan(child)

        scan(table)
        for item in ast.walk(original):
            for field in ("annotation", "returns"):
                annotation = getattr(item, field, None)
                if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
                    try:
                        parsed = ast.parse(annotation.value, mode="eval")
                    except SyntaxError as exc:
                        raise ContractError(f"unresolved model annotation: {identity}") from exc
                    global_names.update(
                        part.id for part in ast.walk(parsed) if isinstance(part, ast.Name)
                    )
        for dependency in sorted(global_names):
            if dependency in ambiguous:
                raise ContractError(f"ambiguous project binding: {module}.{dependency}")
            if dependency in definitions:
                self._include(f"{module}.{dependency}")
            elif dependency in imports and (
                imports[dependency] == PACKAGE or imports[dependency].startswith(PACKAGE + ".")
            ):
                target = imports[dependency]
                if self._path(target).is_file():
                    # Module aliases must resolve concrete attributes, never getattr(module,...).
                    self._module_uses(target, dependency, original)
                else:
                    self._include(target)
            elif dependency in imports:
                self._external(f"{module}.{dependency}", imports[dependency])
            elif (
                dependency not in imports
                and dependency not in vars(builtins)
                and dependency not in {"__name__", "__file__", "__package__"}
            ):
                raise ContractError(f"unresolved numerical global: {module}.{dependency}")
        # Project imports local to a function are also dependencies.
        for item in ast.walk(original):
            if isinstance(item, ast.ImportFrom):
                parent = (
                    module
                    if self._path(module).name == "__init__.py"
                    else module.rpartition(".")[0]
                )
                base = (
                    importlib.util.resolve_name("." * item.level + (item.module or ""), parent)
                    if item.level
                    else item.module
                )
                if base and not base.startswith(PACKAGE):
                    for alias in item.names:
                        self._external(
                            f"{identity}:local:{alias.asname or alias.name}", f"{base}.{alias.name}"
                        )
                if base and base.startswith(PACKAGE):
                    for alias in item.names:
                        target = f"{base}.{alias.name}"
                        if self._path(target).is_file():
                            self._module_uses(target, alias.asname or alias.name, original)
                        else:
                            self._include(target)
            elif isinstance(item, ast.Import):
                for alias in item.names:
                    if not (alias.name == PACKAGE or alias.name.startswith(PACKAGE + ".")):
                        self._external(f"{identity}:local:{alias.asname or alias.name}", alias.name)
                    if alias.name == PACKAGE or alias.name.startswith(PACKAGE + "."):
                        self._module_uses(
                            alias.name if alias.asname else PACKAGE,
                            alias.asname or PACKAGE,
                            original,
                        )
            elif isinstance(item, (ast.Global, ast.Nonlocal)):
                raise ContractError(f"mutable numerical binding: {identity}")
            elif isinstance(item, ast.Call) and (
                isinstance(item.func, ast.Name)
                and item.func.id
                in {"eval", "exec", "__import__", "globals", "locals", "vars", "import_module"}
                or isinstance(item.func, ast.Attribute)
                and item.func.attr == "import_module"
            ):
                raise ContractError(f"dynamic numerical dispatch: {identity}")
            elif isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                arguments = {
                    arg.arg
                    for arg in (*item.args.posonlyargs, *item.args.args, *item.args.kwonlyargs)
                }
                callbacks = {
                    call.func.id
                    for call in ast.walk(item)
                    if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                } & arguments
                admitted = {"cls"} if isinstance(original, ast.ClassDef) else set()
                if identity in {
                    PACKAGE + ".product_plane.research.bar_usage_policy._missing_policy_columns",
                    PACKAGE
                    + ".product_plane.research.bar_usage_policy.group_outputs_by_bar_usage_rule",
                }:
                    # Both existing callers pass the statically included rule resolver.
                    admitted.add("resolver")
                if callbacks - admitted:
                    raise ContractError(f"unbound numerical callback: {identity}")

    def fingerprint(self, roots: tuple[str, ...]) -> str:
        for root in roots:
            self._include(root)
        return digest({"format": FORMAT, "symbols": self.selected})


def engine_identity() -> str:
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8-sig"))
    # The compiler itself intentionally uses ordered visitor aliases. Fingerprint
    # its actual implementation, without applying numerical-model restrictions.
    for node in ast.walk(tree):
        if isinstance(getattr(node, "body", None), list):
            node.body = [
                item
                for item in node.body
                if not (
                    isinstance(item, ast.Expr)
                    and isinstance(item.value, ast.Constant)
                    and isinstance(item.value.value, str)
                )
            ]
    definitions = {
        node.name: ast.dump(node, include_attributes=False)
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef))
    }
    constants = {
        ast.unparse(node.targets[0]): ast.dump(node, include_attributes=False)
        for node in tree.body
        if isinstance(node, ast.Assign)
    }
    imports = sorted(
        ast.dump(node, include_attributes=False)
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    )
    return digest(
        {
            "format": FORMAT,
            "package": PACKAGE,
            "constants": constants,
            "imports": imports,
            "definitions": definitions,
        }
    )
