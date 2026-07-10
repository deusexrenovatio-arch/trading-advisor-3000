from __future__ import annotations

import argparse
import ast
import csv
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable, Sequence

DEFAULT_MATRIX = Path("docs/agent/audits/test-audit-matrix.csv")
DEFAULT_SUMMARY = Path("docs/agent/audits/test-audit-summary.md")
DEFAULT_UPDATES_DIR = Path("docs/agent/audits/test-audit-updates")

MATRIX_COLUMNS = (
    "nodeid",
    "collection_state",
    "work_package",
    "test_kind",
    "audit_status",
    "decision",
    "problem_codes",
    "skip_policy",
    "static_signals",
    "behavior_contract",
    "evidence",
    "reviewed_by",
)

MANUAL_COLUMNS = (
    "collection_state",
    "audit_status",
    "decision",
    "problem_codes",
    "skip_policy",
    "behavior_contract",
    "evidence",
    "reviewed_by",
)
AUDIT_UPDATE_COLUMNS = ("nodeid", *MANUAL_COLUMNS)

AUDIT_STATUSES = {"pending", "in_review", "reviewed", "fixed", "verified"}
DECISIONS = {"", "good", "rewrite", "delete", "infra-proof"}
COLLECTION_STATES = {"active", "retired"}
SKIP_POLICIES = {
    "none",
    "unreviewed",
    "local-infra",
    "linux-docker-proof",
    "optional-capability",
    "external-service",
    "invalid",
}
PROBLEM_CODES = {
    "none",
    "implementation-coupled",
    "tautological",
    "duplicate",
    "weak-assertion",
    "wrong-seam",
    "mock-heavy",
    "source-structure",
    "skip-unowned",
    "environment-mismatch",
    "nondeterministic",
    "slow-unbounded",
}

WORK_PACKAGE_NAMES = {
    "C": "Contracts and bootstrap",
    "CF": "Continuous front and sidecar",
    "D": "Generic data and Delta",
    "G1": "Governance gates and architecture",
    "G2": "Process telemetry and publication",
    "M1": "MOEX source, raw, and economics",
    "M2": "MOEX canonical, quality, and operations",
    "R1": "Research data and indicators",
    "R2": "Research strategy and backtest",
    "X": "Runtime, execution, and observability",
}

G2_PROCESS_FILES = {
    "test_agent_process_telemetry.py",
    "test_build_project_cockpit.py",
    "test_build_publication_lifecycle_evidence.py",
    "test_measure_dev_loop.py",
    "test_phase_tz_compiler.py",
    "test_process_reports.py",
    "test_run_f1d_sidecar_immutable_evidence.py",
    "test_run_f1e_real_broker_process.py",
    "test_run_surface_pr_matrix.py",
    "test_shell_delivery_operational_proving.py",
    "test_skill_update_decision.py",
    "test_sync_project_map_items.py",
    "test_sync_skills_catalog.py",
    "test_truth_recomposition.py",
}

M2_TOKENS = (
    "canonical",
    "cf_catch_up",
    "nightly_backfill",
    "operational_hardening",
    "reconciliation",
)
R2_TOKENS = (
    "backtest",
    "campaign",
    "strategy",
    "ranking",
    "research_pipeline",
    "research_registry_store",
    "scoring",
    "nested_validation",
    "vectorized_research",
    "strategy_evaluation_profile",
    "stg01",
    "stg02",
)
X_TOKENS = (
    "runtime",
    "execution",
    "live_",
    "observability",
    "shadow_",
    "review_",
    "recovery_and_idempotency",
    "dotnet_sidecar",
)
D_TOKENS = (
    "historical_data",
    "delta_runtime",
    "hot_delta",
    "data_inspector",
    "product_plane_dagster_definitions",
    "product_plane_namespace_bridge",
    "provider_extension_seams",
    "stage_timing_reports",
)


def _normalize_nodeid(nodeid: str) -> str:
    return nodeid.strip().replace("\\", "/")


def _test_path(nodeid: str) -> str:
    return _normalize_nodeid(nodeid).split("::", 1)[0]


def _test_name(nodeid: str) -> str:
    return _normalize_nodeid(nodeid).rsplit("::", 1)[-1].split("[", 1)[0]


def classify_work_package(nodeid: str) -> str:
    path = _test_path(nodeid).lower()
    filename = Path(path).name

    if "/contracts/" in path or filename in {
        "test_app_plane_metadata.py",
        "test_product_plane_bootstrap_acceptance.py",
    }:
        return "C"
    if "continuous_front" in path:
        return "CF"
    if path.startswith("tests/architecture/"):
        return "G1"
    if path.startswith("tests/process/"):
        return "G2" if filename in G2_PROCESS_FILES else "G1"
    if "moex" in path:
        if any(token in path for token in M2_TOKENS):
            return "M2"
        if filename == "test_moex_dagster_cutover.py":
            return "M2"
        return "M1"
    if (
        "research" in path
        or "strategy_evaluation_profile" in path
        or "stg01" in path
        or "stg02" in path
    ):
        return "R2" if any(token in path for token in R2_TOKENS) else "R1"
    if any(token in path for token in D_TOKENS):
        return "D"
    if any(token in path for token in X_TOKENS):
        return "X"
    return "X"


def classify_test_kind(nodeid: str) -> str:
    path = _test_path(nodeid)
    if path.startswith("tests/architecture/"):
        return "architecture"
    if path.startswith("tests/process/"):
        return "process"
    if "/contracts/" in path:
        return "contract"
    if "/integration/" in path:
        return "integration"
    if "/unit/" in path or "/continuous_front_indicators/" in path:
        return "unit"
    return "acceptance"


def _dotted_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _contains_skip(nodes: Iterable[ast.AST]) -> bool:
    skip_suffixes = (".skip", ".skipif", ".importorskip", ".skipUnless", ".skipIf")
    for root in nodes:
        for node in ast.walk(root):
            if not isinstance(node, ast.Call):
                continue
            name = _dotted_name(node.func)
            if name in {"skip", "skipif", "importorskip"} or name.endswith(skip_suffixes):
                return True
    return False


def _contains_source_structure_check(node: ast.AST) -> bool:
    constants = [
        value.lower()
        for value in (item.value for item in ast.walk(node) if isinstance(item, ast.Constant))
        if isinstance(value, str)
    ]
    source_path_hint = any(
        value.endswith(".py") or value in {"src", "scripts"} or value.startswith("src/")
        for value in constants
    )
    for item in ast.walk(node):
        if not isinstance(item, ast.Call):
            continue
        name = _dotted_name(item.func)
        if name in {"inspect.getsource", "inspect.getsourcelines"}:
            return True
        if source_path_hint and (name.endswith(".read_text") or name in {"open", "Path.open"}):
            return True
    return False


def detect_file_signals(test_file: Path) -> dict[str, tuple[str, ...]]:
    try:
        tree = ast.parse(test_file.read_text(encoding="utf-8"), filename=str(test_file))
    except (OSError, SyntaxError, UnicodeDecodeError):
        return {}

    test_functions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    ]
    module_nodes = [
        node
        for node in tree.body
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    module_skip = _contains_skip(module_nodes)

    result: dict[str, tuple[str, ...]] = {}
    for function in test_functions:
        signals: set[str] = set()
        if module_skip or _contains_skip([*function.decorator_list, function]):
            signals.add("skip-site")
        if _contains_source_structure_check(function):
            signals.add("source-structure")
        result[function.name] = tuple(sorted(signals))
    return result


def collect_static_signals(repo_root: Path, nodeids: Sequence[str]) -> dict[str, tuple[str, ...]]:
    by_path: dict[str, list[str]] = {}
    for nodeid in nodeids:
        by_path.setdefault(_test_path(nodeid), []).append(nodeid)

    signals: dict[str, tuple[str, ...]] = {}
    for relative_path, path_nodeids in by_path.items():
        file_signals = detect_file_signals(repo_root / relative_path)
        for nodeid in path_nodeids:
            signals[nodeid] = file_signals.get(_test_name(nodeid), ())
    return signals


def _default_row(nodeid: str, signals: Sequence[str]) -> dict[str, str]:
    normalized = _normalize_nodeid(nodeid)
    signal_text = "|".join(sorted(set(signals)))
    return {
        "nodeid": normalized,
        "collection_state": "active",
        "work_package": classify_work_package(normalized),
        "test_kind": classify_test_kind(normalized),
        "audit_status": "pending",
        "decision": "",
        "problem_codes": "",
        "skip_policy": "unreviewed" if "skip-site" in signals else "none",
        "static_signals": signal_text,
        "behavior_contract": "",
        "evidence": "",
        "reviewed_by": "",
    }


def merge_rows(
    nodeids: Sequence[str],
    existing_rows: Sequence[dict[str, str]],
    *,
    static_signals_by_nodeid: dict[str, Sequence[str]],
    update_rows: Sequence[dict[str, str]] = (),
) -> list[dict[str, str]]:
    normalized_nodeids = sorted({_normalize_nodeid(nodeid) for nodeid in nodeids})
    existing_by_nodeid = {
        _normalize_nodeid(row.get("nodeid", "")): row for row in existing_rows if row.get("nodeid")
    }
    merged: list[dict[str, str]] = []

    for nodeid in normalized_nodeids:
        signals = static_signals_by_nodeid.get(nodeid, ())
        row = _default_row(nodeid, signals)
        previous = existing_by_nodeid.get(nodeid)
        if previous is not None:
            for column in MANUAL_COLUMNS:
                if column in previous:
                    row[column] = previous[column]
            row["collection_state"] = "active"
            if (
                "skip-site" in signals
                and row["audit_status"] == "pending"
                and row["skip_policy"] == "none"
            ):
                row["skip_policy"] = "unreviewed"
        merged.append(row)

    current = set(normalized_nodeids)
    for previous in existing_rows:
        nodeid = _normalize_nodeid(previous.get("nodeid", ""))
        if nodeid and nodeid not in current:
            merged.append({column: previous.get(column, "") for column in MATRIX_COLUMNS})

    merged_by_nodeid = {row["nodeid"]: row for row in merged}
    for update in update_rows:
        nodeid = _normalize_nodeid(update.get("nodeid", ""))
        row = merged_by_nodeid.get(nodeid)
        if row is None:
            continue
        for column in MANUAL_COLUMNS:
            row[column] = update.get(column, "")
        if nodeid in current:
            row["collection_state"] = "active"

    return sorted(merged, key=lambda row: row["nodeid"])


def _problem_code_errors(row: dict[str, str]) -> list[str]:
    value = row.get("problem_codes", "")
    if not value:
        return []
    unknown = sorted(set(value.split("|")) - PROBLEM_CODES)
    return [f"unknown problem_codes={','.join(unknown)}"] if unknown else []


def _is_complete(row: dict[str, str]) -> bool:
    status = row.get("audit_status")
    decision = row.get("decision")
    if row.get("collection_state") == "retired":
        return status == "verified" and decision == "delete"
    if decision == "good":
        return status in {"reviewed", "verified"}
    if decision in {"rewrite", "infra-proof"}:
        return status == "verified"
    return False


def validate_rows(
    rows: Sequence[dict[str, str]],
    nodeids: Sequence[str],
    *,
    require_complete: bool = False,
) -> list[str]:
    errors: list[str] = []
    current = {_normalize_nodeid(nodeid) for nodeid in nodeids}
    counts = Counter(_normalize_nodeid(row.get("nodeid", "")) for row in rows)
    duplicates = sorted(nodeid for nodeid, count in counts.items() if nodeid and count > 1)
    if duplicates:
        errors.append(f"duplicate nodeids: {', '.join(duplicates[:5])}")

    active = {
        _normalize_nodeid(row.get("nodeid", ""))
        for row in rows
        if row.get("collection_state") == "active"
    }
    missing = sorted(current - active)
    stale = sorted(active - current)
    if missing:
        errors.append(f"missing nodeids: {', '.join(missing[:5])}")
    if stale:
        errors.append(f"stale nodeids: {', '.join(stale[:5])}")

    for row in rows:
        nodeid = _normalize_nodeid(row.get("nodeid", "")) or "<blank>"
        state = row.get("collection_state", "")
        status = row.get("audit_status", "")
        decision = row.get("decision", "")
        skip_policy = row.get("skip_policy", "")
        signals = set(filter(None, row.get("static_signals", "").split("|")))

        if state not in COLLECTION_STATES:
            errors.append(f"{nodeid}: invalid collection_state={state!r}")
        if status not in AUDIT_STATUSES:
            errors.append(f"{nodeid}: invalid audit_status={status!r}")
        if decision not in DECISIONS:
            errors.append(f"{nodeid}: invalid decision={decision!r}")
        if skip_policy not in SKIP_POLICIES:
            errors.append(f"{nodeid}: invalid skip_policy={skip_policy!r}")
        errors.extend(f"{nodeid}: {error}" for error in _problem_code_errors(row))

        completed = status in {"reviewed", "fixed", "verified"}
        if completed:
            for field in (
                "decision",
                "problem_codes",
                "behavior_contract",
                "evidence",
                "reviewed_by",
            ):
                if not row.get(field, "").strip():
                    errors.append(f"{nodeid}: completed audit requires {field}")
            if "skip-site" in signals and skip_policy == "unreviewed":
                errors.append(f"{nodeid}: completed audit requires resolved skip_policy")
            if decision == "good" and row.get("problem_codes") != "none":
                errors.append(f"{nodeid}: good decision requires problem_codes=none")
            if decision == "infra-proof" and skip_policy in {"none", "unreviewed", "invalid"}:
                errors.append(f"{nodeid}: infra-proof requires an owned skip_policy")

        if state == "retired" and not (status == "verified" and decision == "delete"):
            errors.append(f"{nodeid}: retired rows require verified delete decision")
        if state == "retired" and nodeid in current:
            errors.append(f"{nodeid}: collected test cannot be retired")
        if state == "active" and decision == "delete" and status == "verified":
            errors.append(f"{nodeid}: verified delete must be retired from collection")
        if require_complete and state == "active" and not _is_complete(row):
            if decision in {"rewrite", "delete", "infra-proof"}:
                errors.append(f"{nodeid}: remediation is not verified")
            else:
                errors.append(f"{nodeid}: audit is not complete")

    return errors


def read_matrix(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != MATRIX_COLUMNS:
            raise ValueError(
                f"matrix schema drift: expected {','.join(MATRIX_COLUMNS)}; "
                f"found {','.join(reader.fieldnames or [])}"
            )
        return [{column: row.get(column, "") for column in MATRIX_COLUMNS} for row in reader]


def read_update_rows(updates_dir: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not updates_dir.exists():
        return [], []

    updates: list[dict[str, str]] = []
    errors: list[str] = []
    owners: dict[str, str] = {}
    for path in sorted(updates_dir.glob("*.csv")):
        package = path.stem
        if package not in WORK_PACKAGE_NAMES:
            errors.append(f"{path.name}: unknown work package")
            continue
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != AUDIT_UPDATE_COLUMNS:
                errors.append(f"{path.name}: update schema drift")
                continue
            package_nodeids: list[str] = []
            for source_row in reader:
                row = {column: source_row.get(column, "") for column in AUDIT_UPDATE_COLUMNS}
                nodeid = _normalize_nodeid(row["nodeid"])
                row["nodeid"] = nodeid
                package_nodeids.append(nodeid)
                actual_package = classify_work_package(nodeid)
                if actual_package != package:
                    errors.append(f"{nodeid}: belongs to {actual_package}, not {package}")
                previous_owner = owners.get(nodeid)
                if previous_owner is not None:
                    errors.append(
                        f"{nodeid}: duplicate update in {previous_owner}.csv and {path.name}"
                    )
                owners[nodeid] = package
                updates.append(row)
            if package_nodeids != sorted(package_nodeids):
                errors.append(f"{path.name}: nodeids must be sorted")
    return updates, errors


def write_matrix(path: Path, rows: Sequence[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MATRIX_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def render_summary(rows: Sequence[dict[str, str]]) -> str:
    active = [row for row in rows if row["collection_state"] == "active"]
    retired = [row for row in rows if row["collection_state"] == "retired"]
    completed = sum(_is_complete(row) for row in active)
    lines = [
        "<!-- generated-by: scripts/sync_test_audit_matrix.py -->",
        "# Test Audit Summary",
        "",
        f"- Current pytest tests: `{len(active)}`",
        f"- Retired audited tests: `{len(retired)}`",
        f"- Completed audits: `{completed}/{len(active)}`",
        "",
        "| Block | Scope | Active | Pending | Reviewed | Fixed | Verified |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for work_package, name in WORK_PACKAGE_NAMES.items():
        package_rows = [row for row in active if row["work_package"] == work_package]
        status_counts = Counter(row["audit_status"] for row in package_rows)
        lines.append(
            f"| {work_package} | {name} | {len(package_rows)} | "
            f"{status_counts['pending']} | {status_counts['reviewed']} | "
            f"{status_counts['fixed']} | {status_counts['verified']} |"
        )

    decision_counts = Counter(row["decision"] or "unclassified" for row in active)
    signal_counts = Counter(
        signal for row in active for signal in filter(None, row["static_signals"].split("|"))
    )
    lines.extend(
        [
            "",
            "## Decisions",
            "",
            *[f"- `{decision}`: {count}" for decision, count in sorted(decision_counts.items())],
            "",
            "## Static Signals",
            "",
            *(
                [f"- `{signal}`: {count}" for signal, count in sorted(signal_counts.items())]
                or ["- None"]
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _nodeids_from_text(text: str) -> list[str]:
    return sorted(
        {
            normalized
            for line in text.splitlines()
            if "::" in line and (normalized := _normalize_nodeid(line)).startswith("tests/")
        }
    )


def collect_nodeids(repo_root: Path, collection_file: Path | None = None) -> list[str]:
    if collection_file is not None:
        nodeids = _nodeids_from_text(collection_file.read_text(encoding="utf-8"))
    else:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "--collect-only", "-q"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stdout + "\n" + result.stderr)
        nodeids = _nodeids_from_text(result.stdout)
    if not nodeids:
        raise ValueError("pytest collection produced no test nodeids")
    return nodeids


def _resolved(repo_root: Path, value: Path) -> Path:
    return value if value.is_absolute() else repo_root / value


def run(
    *,
    repo_root: Path,
    matrix_path: Path,
    summary_path: Path,
    updates_dir: Path,
    collection_file: Path | None,
    check: bool,
    require_complete: bool,
) -> int:
    nodeids = collect_nodeids(repo_root, collection_file)
    existing = read_matrix(matrix_path)
    update_rows, update_errors = read_update_rows(updates_dir)
    if update_errors:
        for error in update_errors:
            print(f"test audit matrix: ERROR: {error}", file=sys.stderr)
        return 1

    known_nodeids = set(nodeids) | {row["nodeid"] for row in existing}
    unknown_updates = sorted(
        row["nodeid"] for row in update_rows if row["nodeid"] not in known_nodeids
    )
    if unknown_updates:
        for nodeid in unknown_updates:
            print(f"test audit matrix: ERROR: unknown update nodeid: {nodeid}", file=sys.stderr)
        return 1

    signals = collect_static_signals(repo_root, nodeids)
    expected = merge_rows(
        nodeids,
        existing,
        static_signals_by_nodeid=signals,
        update_rows=update_rows,
    )
    errors = validate_rows(
        existing if check else expected, nodeids, require_complete=require_complete
    )

    if check:
        if existing != expected:
            errors.append("matrix drift: run sync_test_audit_matrix.py --write")
        expected_summary = render_summary(expected)
        if (
            not summary_path.exists()
            or summary_path.read_text(encoding="utf-8") != expected_summary
        ):
            errors.append("summary drift: run sync_test_audit_matrix.py --write")

    if errors:
        for error in errors:
            print(f"test audit matrix: ERROR: {error}", file=sys.stderr)
        return 1

    if not check:
        write_matrix(matrix_path, expected)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(render_summary(expected), encoding="utf-8")

    action = "check" if check else "write"
    active_count = sum(row["collection_state"] == "active" for row in expected)
    print(f"test audit matrix: {action} OK (active={active_count} rows={len(expected)})")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Synchronize the durable per-nodeid TA3000 test audit matrix."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--updates-dir", type=Path, default=DEFAULT_UPDATES_DIR)
    parser.add_argument("--collection-file", type=Path)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    collection_file = None if args.collection_file is None else args.collection_file.resolve()
    sys.exit(
        run(
            repo_root=repo_root,
            matrix_path=_resolved(repo_root, args.matrix),
            summary_path=_resolved(repo_root, args.summary),
            updates_dir=_resolved(repo_root, args.updates_dir),
            collection_file=collection_file,
            check=args.check,
            require_complete=args.require_complete,
        )
    )


if __name__ == "__main__":
    main()
