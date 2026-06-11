from __future__ import annotations

import argparse
import ast
import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path

PRODUCT_PLANE_IMPORT_PREFIX = "trading_advisor_3000.product_plane"
PRODUCT_PLANE_PATH = Path("src") / "trading_advisor_3000" / "product_plane"

MARKET_DATA_FOUNDATION = "Market Data Foundation"
RESEARCH_DATA_FACTORY = "Research Data Factory"
STRATEGY_FACTORY = "Strategy Factory"
RUNTIME_PLANE = "Runtime Plane"
EXECUTION_PLANE = "Execution Plane"
CONTRACTS = "Contracts"
INTERFACES = "Interfaces"
PRODUCT_UTILITIES = "Product Utilities"
PRODUCT_PLANE_ROOT = "Product Plane Root"
UNKNOWN = "Unknown Product Plane Module"

PUBLIC_API = "public_api"
TOLERATED_BRIDGE = "tolerated_bridge"
REVIEW_REQUIRED = "review_required"
INTERNAL = "internal"


@dataclass(frozen=True)
class ImportRecord:
    file: str
    line: int
    origin_module: str
    target_module: str
    imported_module: str
    classification: str
    reason: str


@dataclass(frozen=True)
class ParseError:
    file: str
    error: str


def _relative_python_files(product_root: Path) -> list[Path]:
    if not product_root.exists():
        return []
    return sorted(
        path.relative_to(product_root)
        for path in product_root.rglob("*.py")
        if path.is_file() and "__pycache__" not in path.parts
    )


def module_for_product_plane_path(relative_path: Path) -> str:
    parts = relative_path.parts
    if not parts:
        return UNKNOWN

    top = parts[0]
    if relative_path.name == "__init__.py" and len(parts) == 1:
        return PRODUCT_PLANE_ROOT
    if top == "contracts":
        return CONTRACTS
    if top == "data_plane":
        return MARKET_DATA_FOUNDATION
    if top == "runtime":
        return RUNTIME_PLANE
    if top == "execution":
        return EXECUTION_PLANE
    if top == "interfaces":
        return INTERFACES
    if top in {"common", "config", "domain"}:
        return PRODUCT_UTILITIES
    if top != "research":
        return UNKNOWN

    return _module_for_research_path(relative_path)


def _module_for_research_path(relative_path: Path) -> str:
    parts = relative_path.parts
    if len(parts) == 1:
        return RESEARCH_DATA_FACTORY

    second = parts[1].removesuffix(".py")
    if second in {
        "datasets",
        "derived_indicators",
        "indicators",
        "continuous_front_indicators",
        "io",
    }:
        return RESEARCH_DATA_FACTORY

    if second in {"backtests", "forward", "scoring", "strategies"}:
        return STRATEGY_FACTORY

    if second == "jobs":
        if len(parts) >= 3 and parts[2].removesuffix(".py") == "continuous_front_refresh":
            return RESEARCH_DATA_FACTORY
        return STRATEGY_FACTORY

    if second in {
        "campaigns",
        "ids",
        "registry_store",
        "strategy_space",
    }:
        return STRATEGY_FACTORY

    if second in {
        "bar_usage_policy",
        "continuous_front",
        "dependencies",
    }:
        return RESEARCH_DATA_FACTORY

    return RESEARCH_DATA_FACTORY


def _module_name_to_relative_path(module_name: str) -> Path | None:
    if module_name == PRODUCT_PLANE_IMPORT_PREFIX:
        return Path("__init__.py")
    prefix = PRODUCT_PLANE_IMPORT_PREFIX + "."
    if not module_name.startswith(prefix):
        return None
    suffix = module_name.removeprefix(prefix)
    if not suffix:
        return Path("__init__.py")
    return Path(*suffix.split("."))


def module_for_import_name(module_name: str) -> str:
    relative_path = _module_name_to_relative_path(module_name)
    if relative_path is None:
        return UNKNOWN
    if relative_path == Path("__init__.py"):
        return PRODUCT_PLANE_ROOT

    parts = relative_path.parts
    if len(parts) == 1:
        return module_for_product_plane_path(Path(parts[0]) / "__init__.py")
    return module_for_product_plane_path(relative_path)


def _resolve_relative_import(module: str | None, level: int, file_path: Path) -> str | None:
    if level == 0:
        return module

    current_package = list(file_path.parent.parts)
    keep_count = len(current_package) - level + 1
    if keep_count < 0:
        return None

    target_parts = current_package[:keep_count]
    if module:
        target_parts.extend(module.split("."))
    if not target_parts:
        return PRODUCT_PLANE_IMPORT_PREFIX
    return PRODUCT_PLANE_IMPORT_PREFIX + "." + ".".join(target_parts)


def _iter_product_plane_imports(tree: ast.AST, file_path: Path) -> list[tuple[int, str]]:
    imports: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == PRODUCT_PLANE_IMPORT_PREFIX or alias.name.startswith(
                    PRODUCT_PLANE_IMPORT_PREFIX + "."
                ):
                    imports.append((node.lineno, alias.name))
            continue

        if isinstance(node, ast.ImportFrom):
            resolved = _resolve_relative_import(node.module, node.level, file_path)
            if resolved is None:
                continue
            if resolved == PRODUCT_PLANE_IMPORT_PREFIX or resolved.startswith(
                PRODUCT_PLANE_IMPORT_PREFIX + "."
            ):
                imports.append((node.lineno, resolved))

    return imports


def _is_market_data_public_import(module_name: str) -> bool:
    allowed_prefixes = (
        f"{PRODUCT_PLANE_IMPORT_PREFIX}.data_plane.delta_runtime",
        f"{PRODUCT_PLANE_IMPORT_PREFIX}.data_plane.canonical",
        f"{PRODUCT_PLANE_IMPORT_PREFIX}.data_plane.schemas",
        f"{PRODUCT_PLANE_IMPORT_PREFIX}.data_plane.hot_delta_tables",
        f"{PRODUCT_PLANE_IMPORT_PREFIX}.data_plane.moex.storage_roots",
        f"{PRODUCT_PLANE_IMPORT_PREFIX}.data_plane.moex.session_schedule",
        f"{PRODUCT_PLANE_IMPORT_PREFIX}.data_plane.moex.runtime_instances",
    )
    return module_name.startswith(allowed_prefixes)


def classify_import(
    origin_module: str,
    target_module: str,
    imported_module: str,
) -> tuple[str, str]:
    if origin_module == target_module:
        return INTERNAL, "same target module"
    if target_module in {CONTRACTS, PRODUCT_UTILITIES, PRODUCT_PLANE_ROOT}:
        return PUBLIC_API, "shared product-plane boundary"
    if origin_module == INTERFACES:
        return PUBLIC_API, "interface layer may call public module APIs"

    if origin_module == MARKET_DATA_FOUNDATION:
        return REVIEW_REQUIRED, "foundation should not depend on downstream modules"

    if origin_module == RESEARCH_DATA_FACTORY and target_module == MARKET_DATA_FOUNDATION:
        if _is_market_data_public_import(imported_module):
            return PUBLIC_API, "research data consumes public market-data outputs/helpers"
        return REVIEW_REQUIRED, "research data may be reaching into market-data internals"

    if origin_module == STRATEGY_FACTORY and target_module == RESEARCH_DATA_FACTORY:
        return PUBLIC_API, "strategy consumes research-ready frames or manifests"

    if origin_module == STRATEGY_FACTORY and target_module == MARKET_DATA_FOUNDATION:
        if _is_market_data_public_import(imported_module):
            return (
                TOLERATED_BRIDGE,
                "storage helper bridge before a narrower public adapter exists",
            )
        return (
            REVIEW_REQUIRED,
            "strategy should consume research-ready products, not market internals",
        )

    if origin_module == RUNTIME_PLANE and target_module == EXECUTION_PLANE:
        return (
            REVIEW_REQUIRED,
            "runtime should use a narrow execution adapter or order-intent contract",
        )

    if origin_module == RUNTIME_PLANE and target_module in {
        MARKET_DATA_FOUNDATION,
        RESEARCH_DATA_FACTORY,
        STRATEGY_FACTORY,
    }:
        return REVIEW_REQUIRED, "runtime should consume projected candidates and contracts only"

    if origin_module == EXECUTION_PLANE and target_module == RUNTIME_PLANE:
        return REVIEW_REQUIRED, "execution should not depend on runtime internals"

    if target_module == UNKNOWN or origin_module == UNKNOWN:
        return REVIEW_REQUIRED, "module mapping is unknown and needs classification"

    return REVIEW_REQUIRED, "cross-module dependency needs explicit public-boundary review"


def collect_import_records(
    repo_root: Path,
    *,
    include_internal: bool = False,
) -> tuple[list[ImportRecord], list[ParseError]]:
    product_root = repo_root / PRODUCT_PLANE_PATH
    records: list[ImportRecord] = []
    parse_errors: list[ParseError] = []

    for relative_path in _relative_python_files(product_root):
        source_path = product_root / relative_path
        try:
            tree = ast.parse(source_path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            parse_errors.append(ParseError(file=relative_path.as_posix(), error=str(exc)))
            continue

        origin_module = module_for_product_plane_path(relative_path)
        for line, imported_module in _iter_product_plane_imports(tree, relative_path):
            target_module = module_for_import_name(imported_module)
            classification, reason = classify_import(
                origin_module=origin_module,
                target_module=target_module,
                imported_module=imported_module,
            )
            if classification == INTERNAL and not include_internal:
                continue
            records.append(
                ImportRecord(
                    file=(PRODUCT_PLANE_PATH / relative_path).as_posix(),
                    line=line,
                    origin_module=origin_module,
                    target_module=target_module,
                    imported_module=imported_module,
                    classification=classification,
                    reason=reason,
                )
            )

    return records, parse_errors


def summarize(records: list[ImportRecord], parse_errors: list[ParseError]) -> dict[str, object]:
    classification_counts = Counter(record.classification for record in records)
    edge_counts = Counter((record.origin_module, record.target_module) for record in records)
    review_required = classification_counts[REVIEW_REQUIRED]

    return {
        "status": "report_only",
        "record_count": len(records),
        "parse_error_count": len(parse_errors),
        "review_required_count": review_required,
        "counts_by_classification": dict(sorted(classification_counts.items())),
        "edges": [
            {
                "origin_module": origin,
                "target_module": target,
                "count": count,
            }
            for (origin, target), count in sorted(edge_counts.items())
        ],
    }


def build_payload(repo_root: Path, *, include_internal: bool = False) -> dict[str, object]:
    records, parse_errors = collect_import_records(repo_root, include_internal=include_internal)
    return {
        "report": "product_plane_module_import_inventory",
        "report_only": True,
        "repo_root": repo_root.resolve().as_posix(),
        "source": "docs/architecture/product-plane/product-plane-module-charters.md",
        "summary": summarize(records, parse_errors),
        "records": [asdict(record) for record in records],
        "parse_errors": [asdict(error) for error in parse_errors],
    }


def render_markdown(payload: dict[str, object]) -> str:
    summary = payload["summary"]
    if not isinstance(summary, dict):
        raise TypeError("summary must be a dictionary")

    counts = summary["counts_by_classification"]
    edges = summary["edges"]
    records = payload["records"]
    if not isinstance(counts, dict) or not isinstance(edges, list) or not isinstance(records, list):
        raise TypeError("payload has invalid report shape")

    lines = [
        "# Product Plane Module Import Inventory",
        "",
        "Status: `report_only`",
        "",
        "Source boundary document: "
        "`docs/architecture/product-plane/product-plane-module-charters.md`",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Import records | {summary['record_count']} |",
        f"| Review required | {summary['review_required_count']} |",
        f"| Parse errors | {summary['parse_error_count']} |",
        "",
        "## Counts By Classification",
        "",
        "| Classification | Count |",
        "| --- | ---: |",
    ]
    for classification, count in counts.items():
        lines.append(f"| `{classification}` | {count} |")

    lines.extend(
        [
            "",
            "## Cross-Module Edges",
            "",
            "| From | To | Count |",
            "| --- | --- | ---: |",
        ]
    )
    for edge in edges:
        lines.append(
            "| " + f"{edge['origin_module']} | {edge['target_module']} | {edge['count']} |"
        )

    lines.extend(
        [
            "",
            "## Review Sample",
            "",
            "| File | Line | From | To | Import | Classification | Reason |",
            "| --- | ---: | --- | --- | --- | --- | --- |",
        ]
    )
    review_records = [
        record
        for record in records
        if isinstance(record, dict) and record.get("classification") == REVIEW_REQUIRED
    ]
    for record in review_records[:80]:
        lines.append(
            "| "
            + f"`{record['file']}` | {record['line']} | {record['origin_module']} | "
            + f"{record['target_module']} | `{record['imported_module']}` | "
            + f"`{record['classification']}` | {record['reason']} |"
        )

    lines.extend(
        [
            "",
            "Report-only note: review-required rows are not build failures. They are the",
            "first inventory for classifying public APIs, tolerated bridges, and future",
            "boundary violations.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(
    repo_root: Path,
    *,
    output_format: str,
    output: Path | None = None,
    include_internal: bool = False,
) -> int:
    payload = build_payload(repo_root, include_internal=include_internal)
    if output_format == "json":
        rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    elif output_format == "markdown":
        rendered = render_markdown(payload)
    else:
        raise ValueError(f"unsupported output format: {output_format}")

    if output is None:
        print(rendered, end="")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
        print(f"product-plane module import inventory: wrote {output.as_posix()}")

    summary = payload["summary"]
    if isinstance(summary, dict) and summary.get("parse_error_count", 0):
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report product-plane imports against the module-charter boundary map."
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--output", default=None)
    parser.add_argument("--include-internal", action="store_true")
    args = parser.parse_args()

    output = None if args.output is None else Path(args.output)
    sys.exit(
        run(
            Path(args.repo_root).resolve(),
            output_format=args.format,
            output=output,
            include_internal=args.include_internal,
        )
    )


if __name__ == "__main__":
    main()
