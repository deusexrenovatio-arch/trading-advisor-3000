#!/usr/bin/env python3
"""Single fail-closed entrypoint for package intake and governed phase continuation."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from codex_from_package import choose_latest_package, main as package_main
from codex_phase_orchestrator import main as orchestrator_main
from repo_mutation_lock import RepoMutationLockError, repo_mutation_lock


DEFAULT_INBOX = Path("docs/codex/packages/inbox")
DEFAULT_ROUTE_STATE = Path(".runlogs/codex-governed-entry/last-route.json")
DEFAULT_AMBIGUITY_REPORT = Path(".runlogs/codex-governed-entry/module-ambiguity-report.json")
DEFAULT_STACKED_FOLLOWUP_CONTRACT = Path(".runlogs/codex-governed-entry/stacked-followup-contract.json")
LEGACY_ENTRY_ROUTE_MODE = "legacy-wrapper"
EXPLICIT_ENTRY_ROUTE_MODE = "explicit-dual-mode"
LEGACY_SESSION_MODE = "legacy-full"
TRACKED_SESSION_MODE = "tracked_session"
DEFAULT_SNAPSHOT_MODE = "route-report"
ROUTE_SIGNAL = "entry:route-selection"
STACKED_FOLLOWUP_ROUTE = "stacked-followup"


class GovernedEntryError(RuntimeError):
    """Raised when the governed route cannot be chosen safely."""


@dataclass(frozen=True)
class ModuleRoute:
    slug: str
    parent_brief: Path
    execution_contract: Path
    current_phase: Path


@dataclass
class RouteDecision:
    route: str
    reason: str
    package_path: Path | None
    module: ModuleRoute | None
    module_resolution: str | None = None
    module_candidates: list[str] | None = None
    ambiguity_report_path: Path | None = None
    continuation_contract_path: Path | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def _read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def _find_heading(lines: list[str], heading: str) -> int:
    for idx, raw in enumerate(lines):
        if raw.strip() == heading:
            return idx
    return -1


def _section_lines(path: Path, heading: str) -> list[str]:
    lines = _read_lines(path)
    start = _find_heading(lines, heading)
    if start < 0:
        raise GovernedEntryError(f"missing heading `{heading}` in {path.as_posix()}")
    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            end = idx
            break
    return lines[start + 1 : end]


def _single_bullet_value(path: Path, heading: str) -> str:
    for raw in _section_lines(path, heading):
        stripped = raw.strip()
        if stripped.startswith("- "):
            return stripped[2:].strip()
    raise GovernedEntryError(f"missing bullet value for `{heading}` in {path.as_posix()}")


def discover_active_modules(repo_root: Path) -> list[ModuleRoute]:
    modules_root = repo_root / "docs/codex/modules"
    if not modules_root.exists():
        return []

    candidates: list[ModuleRoute] = []
    for parent_path in sorted(modules_root.glob("*.parent.md")):
        next_phase = _single_bullet_value(parent_path, "## Next Phase To Execute")
        if next_phase.lower() == "none":
            continue
        current_phase = resolve_path(repo_root, next_phase)
        if not current_phase.exists():
            raise GovernedEntryError(
                f"parent brief points to missing phase brief: {parent_path.as_posix()} -> {next_phase}"
            )
        slug = parent_path.name[: -len(".parent.md")]
        contract_path = repo_root / "docs/codex/contracts" / f"{slug}.execution-contract.md"
        if not contract_path.exists():
            raise GovernedEntryError(
                f"missing execution contract for active module `{slug}`: {contract_path.as_posix()}"
            )
        candidates.append(
            ModuleRoute(
                slug=slug,
                parent_brief=parent_path.resolve(),
                execution_contract=contract_path.resolve(),
                current_phase=current_phase.resolve(),
            )
        )

    return candidates


def discover_active_module(repo_root: Path) -> ModuleRoute | None:
    candidates = discover_active_modules(repo_root)
    if not candidates:
        return None
    if len(candidates) > 1:
        joined = ", ".join(item.slug for item in candidates)
        raise GovernedEntryError("multiple active modules require an explicit route choice: " + joined)
    return candidates[0]


def _phase_index(path: Path) -> int:
    match = re.search(r"\.phase-(\d+)\.md$", path.name)
    if not match:
        return 9999
    return int(match.group(1))


def _normalize_surfaces(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for raw in values:
        item = str(raw).strip()
        if item and item not in cleaned:
            cleaned.append(item)
    return cleaned


def write_module_ambiguity_report(
    *,
    path: Path,
    requested_route: str,
    candidates: list[ModuleRoute],
    module_slug: str | None,
    module_priority: str | None,
) -> None:
    payload = {
        "updated_at": utc_now().isoformat().replace("+00:00", "Z"),
        "route_signal": "entry:multi-module-ambiguity",
        "requested_route": requested_route,
        "requested_module_slug": module_slug or "",
        "requested_priority_rule": module_priority or "",
        "reason": "multiple active modules require explicit selection before continuation can proceed",
        "candidates": [
            {
                "slug": item.slug,
                "execution_contract": item.execution_contract.as_posix(),
                "parent_brief": item.parent_brief.as_posix(),
                "current_phase": item.current_phase.as_posix(),
                "phase_index": _phase_index(item.current_phase),
            }
            for item in candidates
        ],
        "resolution_contract": {
            "allowed_selection_modes": [
                "module-slug",
                "priority-rule",
            ],
            "priority_rules": ["phase-order", "slug-lexical"],
            "recommended_flags": [
                "--module-slug <slug>",
                "--module-priority phase-order",
            ],
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def resolve_active_module_selection(
    *,
    candidates: list[ModuleRoute],
    requested_route: str,
    module_slug: str | None,
    module_priority: str | None,
    ambiguity_report_path: Path,
) -> tuple[ModuleRoute, str, Path | None]:
    if not candidates:
        raise GovernedEntryError("no active governed module is available for continuation")

    if module_slug:
        matched = [item for item in candidates if item.slug.lower() == module_slug.lower()]
        if len(matched) == 1:
            return matched[0], f"explicit module slug `{matched[0].slug}` was provided", None
        write_module_ambiguity_report(
            path=ambiguity_report_path,
            requested_route=requested_route,
            candidates=candidates,
            module_slug=module_slug,
            module_priority=module_priority,
        )
        available = ", ".join(item.slug for item in candidates)
        raise GovernedEntryError(
            "requested module slug was not resolved safely; ambiguity report written to "
            f"{ambiguity_report_path.as_posix()} (available: {available})"
        )

    if len(candidates) == 1:
        return candidates[0], "single active module pointer was detected", None

    if module_priority == "phase-order":
        ordered = sorted(candidates, key=lambda item: (_phase_index(item.current_phase), item.slug))
        selected = ordered[0]
        return selected, "priority rule `phase-order` selected the next module", None
    if module_priority == "slug-lexical":
        selected = sorted(candidates, key=lambda item: item.slug)[0]
        return selected, "priority rule `slug-lexical` selected the next module", None
    if module_priority:
        raise GovernedEntryError(
            "unknown module priority rule; expected one of phase-order|slug-lexical"
        )

    write_module_ambiguity_report(
        path=ambiguity_report_path,
        requested_route=requested_route,
        candidates=candidates,
        module_slug=module_slug,
        module_priority=module_priority,
    )
    joined = ", ".join(item.slug for item in candidates)
    raise GovernedEntryError(
        "multiple active modules require explicit selection; ambiguity report written to "
        f"{ambiguity_report_path.as_posix()} (candidates: {joined})"
    )


def _run_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed


def _git_stdout(repo_root: Path, *args: str) -> str:
    completed = _run_git(repo_root, *args)
    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout).strip()
        raise GovernedEntryError(f"git {' '.join(args)} failed: {stderr}")
    return completed.stdout.strip()


def _git_is_ancestor(repo_root: Path, ancestor: str, descendant: str) -> bool:
    completed = _run_git(repo_root, "merge-base", "--is-ancestor", ancestor, descendant)
    if completed.returncode == 0:
        return True
    if completed.returncode == 1:
        return False
    stderr = (completed.stderr or completed.stdout).strip()
    raise GovernedEntryError(f"git merge-base --is-ancestor failed: {stderr}")


def build_stacked_followup_contract(
    *,
    repo_root: Path,
    decision: RouteDecision,
    contract_path: Path,
    predecessor_ref: str,
    source_branch: str,
    new_base_ref: str,
    carry_surfaces: list[str],
    temporary_downgrade_surfaces: list[str],
    mutation_lock_timeout_sec: float = 5.0,
) -> Path:
    if decision.module is None:
        raise GovernedEntryError("stacked-followup route requires a module context")
    predecessor_ref = predecessor_ref.strip()
    source_branch = source_branch.strip()
    new_base_ref = new_base_ref.strip()
    carry_surfaces = _normalize_surfaces(carry_surfaces)
    temporary_downgrade_surfaces = _normalize_surfaces(temporary_downgrade_surfaces)
    if not predecessor_ref:
        raise GovernedEntryError("stacked-followup route requires --predecessor-ref")
    if not source_branch:
        raise GovernedEntryError("stacked-followup route requires --source-branch")
    if not new_base_ref:
        raise GovernedEntryError("stacked-followup route requires --new-base-ref")
    if not carry_surfaces:
        raise GovernedEntryError("stacked-followup route requires at least one --carry-surface")

    predecessor_sha = _git_stdout(repo_root, "rev-parse", f"{predecessor_ref}^{{commit}}")
    source_sha = _git_stdout(repo_root, "rev-parse", f"{source_branch}^{{commit}}")
    base_sha = _git_stdout(repo_root, "rev-parse", f"{new_base_ref}^{{commit}}")
    merged_into_base = _git_is_ancestor(repo_root, predecessor_sha, base_sha)
    if not merged_into_base:
        raise GovernedEntryError(
            "stacked-followup predecessor is not merged into the declared base ref; "
            "cannot continue without a merged predecessor context"
        )

    payload = {
        "updated_at": utc_now().isoformat().replace("+00:00", "Z"),
        "route_signal": "entry:stacked-followup-contract",
        "route": STACKED_FOLLOWUP_ROUTE,
        "module": {
            "slug": decision.module.slug,
            "execution_contract": decision.module.execution_contract.as_posix(),
            "parent_brief": decision.module.parent_brief.as_posix(),
            "current_phase": decision.module.current_phase.as_posix(),
        },
        "predecessor_merge_context": {
            "ref": predecessor_ref,
            "resolved_sha": predecessor_sha,
            "merged_into_new_base": merged_into_base,
        },
        "new_base_contract": {
            "ref": new_base_ref,
            "resolved_sha": base_sha,
            "source_branch": source_branch,
            "source_branch_sha": source_sha,
        },
        "surface_contract": {
            "allowed_to_carry_forward": carry_surfaces,
            "temporary_downgrade_surfaces": temporary_downgrade_surfaces,
            "forbidden_after_recomposition": temporary_downgrade_surfaces,
        },
        "recomposition_contract": {
            "status": "required",
            "helper": "python scripts/truth_recomposition.py build --followup-contract <path> --merged-surface <surface> --candidate-surface <surface> --output <path>",
            "validator": "python scripts/truth_recomposition.py validate --report <path>",
        },
    }
    try:
        with repo_mutation_lock(
            repo_root=repo_root,
            owner="codex_governed_entry:stacked-followup-contract",
            timeout_sec=mutation_lock_timeout_sec,
        ):
            contract_path.parent.mkdir(parents=True, exist_ok=True)
            contract_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except RepoMutationLockError as exc:
        raise GovernedEntryError(str(exc)) from exc
    return contract_path


def decide_route(
    *,
    repo_root: Path,
    requested_route: str,
    explicit_package: Path | None,
    explicit_contract: Path | None,
    explicit_parent: Path | None,
    inbox: Path,
    module_slug: str | None,
    module_priority: str | None,
    ambiguity_report_path: Path,
) -> RouteDecision:
    if explicit_contract or explicit_parent:
        if not (explicit_contract and explicit_parent):
            raise GovernedEntryError("continue route requires both execution contract and parent brief")
        current_phase = resolve_path(repo_root, _single_bullet_value(explicit_parent, "## Next Phase To Execute"))
        if not current_phase.exists():
            raise GovernedEntryError(f"current phase not found: {current_phase.as_posix()}")
        slug = explicit_parent.name[: -len(".parent.md")] if explicit_parent.name.endswith(".parent.md") else explicit_parent.stem
        return RouteDecision(
            route=STACKED_FOLLOWUP_ROUTE if requested_route == STACKED_FOLLOWUP_ROUTE else "continue",
            reason="explicit module continuation arguments were provided",
            package_path=None,
            module=ModuleRoute(
                slug=slug,
                parent_brief=explicit_parent.resolve(),
                execution_contract=explicit_contract.resolve(),
                current_phase=current_phase.resolve(),
            ),
            module_resolution="explicit execution contract and parent brief arguments were provided",
            module_candidates=[slug],
        )

    if explicit_package is not None and requested_route in {"auto", "package"}:
        return RouteDecision(
            route="package",
            reason="explicit package path was provided",
            package_path=explicit_package.resolve(),
            module=None,
        )

    active_modules = discover_active_modules(repo_root)
    if active_modules:
        selected_module, module_resolution, report_path = resolve_active_module_selection(
            candidates=active_modules,
            requested_route=requested_route,
            module_slug=module_slug,
            module_priority=module_priority,
            ambiguity_report_path=ambiguity_report_path,
        )
        return RouteDecision(
            route=STACKED_FOLLOWUP_ROUTE if requested_route == STACKED_FOLLOWUP_ROUTE else "continue",
            reason="an active module with a next phase pointer exists, so continuation wins over package intake",
            package_path=None,
            module=selected_module,
            module_resolution=module_resolution,
            module_candidates=sorted(item.slug for item in active_modules),
            ambiguity_report_path=report_path,
        )

    if requested_route == STACKED_FOLLOWUP_ROUTE:
        raise GovernedEntryError(
            "stacked-followup route requires an active module or explicit execution contract/parent brief"
        )

    latest_package = choose_latest_package(inbox)
    if latest_package is not None:
        return RouteDecision(
            route="package",
            reason="no active module exists, so the newest inbox package becomes the governed entry",
            package_path=latest_package.resolve(),
            module=None,
        )

    raise GovernedEntryError("no governed route could be chosen: no active module and no package in the inbox")


def write_route_state(
    path: Path,
    decision: RouteDecision,
    *,
    repo_root: Path | None = None,
    route_mode: str = LEGACY_ENTRY_ROUTE_MODE,
    session_mode: str = LEGACY_SESSION_MODE,
    snapshot_mode: str = DEFAULT_SNAPSHOT_MODE,
    profile: str = "none",
    mutation_lock_timeout_sec: float = 5.0,
) -> None:
    phase_brief = decision.module.current_phase.as_posix() if decision.module is not None else None
    payload: dict[str, Any] = {
        "updated_at": utc_now().isoformat().replace("+00:00", "Z"),
        "route_mode": route_mode,
        "session_mode": session_mode,
        "snapshot_mode": snapshot_mode,
        "profile": profile,
        "route_signal": ROUTE_SIGNAL,
        "entry_route": decision.route,
        "route": decision.route,
        "phase_brief": phase_brief,
        "reason": decision.reason,
        "package_path": decision.package_path.as_posix() if decision.package_path else None,
        "module_resolution": decision.module_resolution,
        "module_candidates": decision.module_candidates or [],
        "module_ambiguity_report": (
            decision.ambiguity_report_path.as_posix() if decision.ambiguity_report_path else None
        ),
        "continuation_contract_path": (
            decision.continuation_contract_path.as_posix() if decision.continuation_contract_path else None
        ),
        "module": None,
    }
    if decision.module is not None:
        payload["module"] = {
            "slug": decision.module.slug,
            "execution_contract": decision.module.execution_contract.as_posix(),
            "parent_brief": decision.module.parent_brief.as_posix(),
            "current_phase": decision.module.current_phase.as_posix(),
        }
    if repo_root is None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return
    try:
        with repo_mutation_lock(
            repo_root=repo_root,
            owner="codex_governed_entry:route-state",
            timeout_sec=mutation_lock_timeout_sec,
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except RepoMutationLockError as exc:
        raise GovernedEntryError(str(exc)) from exc


def normalize_route_argv(argv: list[str]) -> list[str]:
    if not argv:
        return []
    first = str(argv[0]).strip().lower()
    if first in {"auto", "package", "continue", STACKED_FOLLOWUP_ROUTE}:
        return ["--route", first, *argv[1:]]
    return list(argv)


def uses_positional_route_alias(argv: list[str]) -> bool:
    if not argv:
        return False
    first = str(argv[0]).strip().lower()
    return first in {"auto", "package", "continue", STACKED_FOLLOWUP_ROUTE}


def normalize_entry_route_mode(raw: str, *, positional_alias: bool) -> str:
    value = str(raw or "").strip().lower()
    if value in {"", "legacy", "legacy-wrapper", "compat", "compatibility"}:
        return LEGACY_ENTRY_ROUTE_MODE
    if value in {"explicit", "explicit-dual-mode", "dual-mode"}:
        if positional_alias:
            raise GovernedEntryError(
                "explicit route mode requires `--route <value>`; positional route alias is legacy-only"
            )
        return EXPLICIT_ENTRY_ROUTE_MODE
    raise GovernedEntryError(
        "unknown route mode; expected one of legacy|explicit|legacy-wrapper|explicit-dual-mode"
    )


def normalize_session_mode(raw: str) -> str:
    value = str(raw or "").strip().lower()
    if value in {"", "legacy", "legacy-full", "full"}:
        return LEGACY_SESSION_MODE
    if value in {"tracked", "tracked-session", "tracked_session"}:
        return TRACKED_SESSION_MODE
    raise GovernedEntryError("unknown session mode; expected one of legacy-full|tracked-session")


def normalize_snapshot_mode(raw: str) -> str:
    value = str(raw or "").strip().lower()
    if value in {"", "route-report"}:
        return DEFAULT_SNAPSHOT_MODE
    if value in {"changed-files", "phase-state", "contract-only"}:
        return value
    raise GovernedEntryError(
        "unknown snapshot mode; expected one of route-report|changed-files|phase-state|contract-only"
    )


def run_package_route(
    *,
    repo_root: Path,
    decision: RouteDecision,
    mode: str,
    profile: str,
    inbox: Path,
    artifact_root: Path,
    output_last_message: Path,
) -> int:
    if decision.package_path is None:
        raise GovernedEntryError("package route requires a package path")
    args = [
        decision.package_path.as_posix(),
        "--mode",
        mode,
        "--profile",
        profile,
        "--inbox",
        inbox.as_posix(),
        "--artifact-root",
        artifact_root.as_posix(),
        "--output-last-message",
        output_last_message.as_posix(),
    ]
    exit_code = int(package_main(args))
    if exit_code != 0:
        return exit_code

    active_modules = discover_active_modules(repo_root)
    if len(active_modules) == 1:
        active_module = active_modules[0]
        print("package_route_outcome: active_module_detected")
        print("next_governed_route: continue")
        print(f"execution_contract: {active_module.execution_contract.as_posix()}")
        print(f"parent_brief: {active_module.parent_brief.as_posix()}")
        print(f"current_phase: {active_module.current_phase.as_posix()}")
    elif len(active_modules) > 1:
        print("package_route_outcome: active_module_ambiguity")
        print("next_governed_route: continue")
        print("reason: multiple active modules require --module-slug or --module-priority")
        print("candidates: " + ", ".join(sorted(item.slug for item in active_modules)))
    else:
        print("package_route_outcome: no_active_module_detected")
    return 0


def run_continue_route(
    *,
    decision: RouteDecision,
    entry_route: str,
    continuation_contract_path: Path | None,
    backend: str,
    artifact_root: Path,
    profile: str,
    worker_model: str,
    acceptor_model: str,
    remediation_model: str,
    skip_clean_check: bool,
    mutation_lock_timeout_sec: float,
    codex_bin: str | None,
    max_remediation_cycles: int,
) -> int:
    if decision.module is None:
        raise GovernedEntryError("continue route requires a module context")
    common = [
        "--execution-contract",
        decision.module.execution_contract.as_posix(),
        "--parent-brief",
        decision.module.parent_brief.as_posix(),
        "--entry-route",
        entry_route,
        "--backend",
        backend,
        "--artifact-root",
        artifact_root.as_posix(),
        "--worker-model",
        worker_model,
        "--acceptor-model",
        acceptor_model,
        "--remediation-model",
        remediation_model or worker_model,
        "--mutation-lock-timeout-sec",
        str(max(mutation_lock_timeout_sec, 0.0)),
    ]
    if continuation_contract_path is not None:
        common.extend(["--continuation-contract", continuation_contract_path.as_posix()])
    if profile.strip():
        common.extend(["--profile", profile])
    if codex_bin:
        common.extend(["--codex-bin", codex_bin])
    if skip_clean_check:
        common.append("--skip-clean-check")

    preflight_code = int(orchestrator_main(["preflight", *common]))
    if preflight_code != 0:
        return preflight_code

    run_args = [
        "run-current-phase",
        *common,
        "--max-remediation-cycles",
        str(max(max_remediation_cycles, 0)),
    ]
    return int(orchestrator_main(run_args))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mandatory governed Codex entrypoint.")
    parser.add_argument(
        "--route",
        choices=("auto", "package", "continue", STACKED_FOLLOWUP_ROUTE),
        default="auto",
    )
    parser.add_argument("--route-mode", default="legacy")
    parser.add_argument("--session-mode", default=LEGACY_SESSION_MODE)
    parser.add_argument("--snapshot-mode", default=DEFAULT_SNAPSHOT_MODE)
    parser.add_argument("--package-path", default=None)
    parser.add_argument("--execution-contract", default=None)
    parser.add_argument("--parent-brief", default=None)
    parser.add_argument("--module-slug", default=None)
    parser.add_argument("--module-priority", choices=("phase-order", "slug-lexical"), default=None)
    parser.add_argument("--ambiguity-report-file", default=str(DEFAULT_AMBIGUITY_REPORT))
    parser.add_argument("--followup-contract-file", default=str(DEFAULT_STACKED_FOLLOWUP_CONTRACT))
    parser.add_argument("--predecessor-ref", default=None)
    parser.add_argument("--source-branch", default=None)
    parser.add_argument("--new-base-ref", default="origin/main")
    parser.add_argument("--carry-surface", action="append", default=[])
    parser.add_argument("--temporary-downgrade-surface", action="append", default=[])
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX))
    parser.add_argument("--artifact-root", default="artifacts/codex")
    parser.add_argument("--output-last-message", default="artifacts/codex/from-package-last-message.txt")
    parser.add_argument("--route-state-file", default=str(DEFAULT_ROUTE_STATE))
    parser.add_argument("--mode", default="auto")
    parser.add_argument("--profile", default="")
    parser.add_argument("--backend", choices=("simulate", "codex-cli"), default="codex-cli")
    parser.add_argument("--worker-model", default="gpt-5.3-codex")
    parser.add_argument("--acceptor-model", default="gpt-5.4")
    parser.add_argument("--remediation-model", default="")
    parser.add_argument("--max-remediation-cycles", type=int, default=2)
    parser.add_argument("--codex-bin", default=None)
    parser.add_argument("--skip-clean-check", action="store_true")
    parser.add_argument("--mutation-lock-timeout-sec", type=float, default=5.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(argv or sys.argv[1:])
    positional_alias = uses_positional_route_alias(raw_argv)
    normalized_argv = normalize_route_argv(raw_argv)
    args = build_parser().parse_args(normalized_argv)
    repo_root = resolve_repo_root()
    inbox = resolve_path(repo_root, args.inbox)
    artifact_root = resolve_path(repo_root, args.artifact_root)
    route_state_file = resolve_path(repo_root, args.route_state_file)
    ambiguity_report_file = resolve_path(repo_root, args.ambiguity_report_file)
    followup_contract_file = resolve_path(repo_root, args.followup_contract_file)
    output_last_message = resolve_path(repo_root, args.output_last_message)
    explicit_package = resolve_path(repo_root, args.package_path) if args.package_path else None
    explicit_contract = resolve_path(repo_root, args.execution_contract) if args.execution_contract else None
    explicit_parent = resolve_path(repo_root, args.parent_brief) if args.parent_brief else None
    module_slug = str(args.module_slug or "").strip() or None
    module_priority = str(args.module_priority or "").strip() or None
    profile = args.profile.strip()
    profile_marker = profile or "none"

    try:
        route_mode = normalize_entry_route_mode(args.route_mode, positional_alias=positional_alias)
        session_mode = normalize_session_mode(args.session_mode)
        snapshot_mode = normalize_snapshot_mode(args.snapshot_mode)
    except GovernedEntryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        decision = decide_route(
            repo_root=repo_root,
            requested_route=args.route,
            explicit_package=explicit_package if args.route in {"auto", "package"} else None,
            explicit_contract=(
                explicit_contract if args.route in {"auto", "continue", STACKED_FOLLOWUP_ROUTE} else None
            ),
            explicit_parent=(
                explicit_parent if args.route in {"auto", "continue", STACKED_FOLLOWUP_ROUTE} else None
            ),
            inbox=inbox,
            module_slug=module_slug,
            module_priority=module_priority,
            ambiguity_report_path=ambiguity_report_file,
        )
    except GovernedEntryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.route == "package" and decision.route != "package":
        print("error: forced package route could not be satisfied safely", file=sys.stderr)
        return 2
    if args.route == "continue" and decision.route != "continue":
        print("error: forced continue route could not be satisfied safely", file=sys.stderr)
        return 2
    if args.route == STACKED_FOLLOWUP_ROUTE and decision.route != STACKED_FOLLOWUP_ROUTE:
        print("error: forced stacked-followup route could not be satisfied safely", file=sys.stderr)
        return 2

    if decision.route == STACKED_FOLLOWUP_ROUTE:
        try:
            decision.continuation_contract_path = build_stacked_followup_contract(
                repo_root=repo_root,
                decision=decision,
                contract_path=followup_contract_file,
                predecessor_ref=str(args.predecessor_ref or ""),
                source_branch=str(args.source_branch or ""),
                new_base_ref=str(args.new_base_ref or ""),
                carry_surfaces=list(args.carry_surface or []),
                temporary_downgrade_surfaces=list(args.temporary_downgrade_surface or []),
                mutation_lock_timeout_sec=max(float(args.mutation_lock_timeout_sec), 0.0),
            )
        except GovernedEntryError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

    write_route_state(
        route_state_file,
        decision,
        repo_root=repo_root,
        route_mode=route_mode,
        session_mode=session_mode,
        snapshot_mode=snapshot_mode,
        profile=profile_marker,
        mutation_lock_timeout_sec=max(float(args.mutation_lock_timeout_sec), 0.0),
    )

    print(f"route_mode: {route_mode}")
    print(f"session_mode: {session_mode}")
    print(f"snapshot_mode: {snapshot_mode}")
    print(f"profile: {profile_marker}")
    print(f"entry_route: {decision.route}")
    print(f"governed_route: {decision.route}")
    print(f"reason: {decision.reason}")
    if decision.module_resolution:
        print(f"module_resolution: {decision.module_resolution}")
    if decision.module_candidates:
        print("module_candidates: " + ", ".join(decision.module_candidates))
    if decision.ambiguity_report_path is not None:
        print(f"module_ambiguity_report: {decision.ambiguity_report_path.as_posix()}")
    if decision.package_path is not None:
        print(f"package_path: {decision.package_path.as_posix()}")
    if decision.module is not None:
        print(f"execution_contract: {decision.module.execution_contract.as_posix()}")
        print(f"parent_brief: {decision.module.parent_brief.as_posix()}")
        print(f"current_phase: {decision.module.current_phase.as_posix()}")
    if decision.continuation_contract_path is not None:
        print(f"stacked_followup_contract: {decision.continuation_contract_path.as_posix()}")

    if args.dry_run:
        print("dry run: governed route dispatch skipped")
        return 0

    if decision.route == "package":
        return run_package_route(
            repo_root=repo_root,
            decision=decision,
            mode=args.mode,
            profile=profile,
            inbox=inbox,
            artifact_root=artifact_root / "package-intake",
            output_last_message=output_last_message,
        )

    return run_continue_route(
        decision=decision,
        entry_route=decision.route,
        continuation_contract_path=decision.continuation_contract_path,
        backend=args.backend,
        artifact_root=artifact_root / "orchestration",
        profile=profile,
        worker_model=args.worker_model,
        acceptor_model=args.acceptor_model,
        remediation_model=args.remediation_model,
        skip_clean_check=bool(args.skip_clean_check),
        mutation_lock_timeout_sec=max(float(args.mutation_lock_timeout_sec), 0.0),
        codex_bin=args.codex_bin,
        max_remediation_cycles=args.max_remediation_cycles,
    )


if __name__ == "__main__":
    raise SystemExit(main())
