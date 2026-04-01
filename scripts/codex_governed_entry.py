#!/usr/bin/env python3
"""Single fail-closed entrypoint for package intake and governed phase continuation."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from codex_from_package import choose_latest_package, main as package_main
from codex_phase_orchestrator import main as orchestrator_main


DEFAULT_INBOX = Path("docs/codex/packages/inbox")
DEFAULT_ROUTE_STATE = Path(".runlogs/codex-governed-entry/last-route.json")
LEGACY_ENTRY_ROUTE_MODE = "legacy-wrapper"
EXPLICIT_ENTRY_ROUTE_MODE = "explicit-dual-mode"
LEGACY_SESSION_MODE = "legacy-full"
TRACKED_SESSION_MODE = "tracked_session"
DEFAULT_SNAPSHOT_MODE = "route-report"
ROUTE_SIGNAL = "entry:route-selection"


class GovernedEntryError(RuntimeError):
    """Raised when the governed route cannot be chosen safely."""


@dataclass(frozen=True)
class ModuleRoute:
    slug: str
    parent_brief: Path
    execution_contract: Path
    current_phase: Path


@dataclass(frozen=True)
class RouteDecision:
    route: str
    reason: str
    package_path: Path | None
    module: ModuleRoute | None


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


def discover_active_module(repo_root: Path) -> ModuleRoute | None:
    modules_root = repo_root / "docs/codex/modules"
    if not modules_root.exists():
        return None

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

    if not candidates:
        return None
    if len(candidates) > 1:
        joined = ", ".join(item.slug for item in candidates)
        raise GovernedEntryError(
            "multiple active modules require an explicit route choice: " + joined
        )
    return candidates[0]


def decide_route(
    *,
    repo_root: Path,
    explicit_package: Path | None,
    explicit_contract: Path | None,
    explicit_parent: Path | None,
    inbox: Path,
) -> RouteDecision:
    if explicit_contract or explicit_parent:
        if not (explicit_contract and explicit_parent):
            raise GovernedEntryError("continue route requires both execution contract and parent brief")
        current_phase = resolve_path(repo_root, _single_bullet_value(explicit_parent, "## Next Phase To Execute"))
        if not current_phase.exists():
            raise GovernedEntryError(f"current phase not found: {current_phase.as_posix()}")
        slug = explicit_parent.name[: -len(".parent.md")] if explicit_parent.name.endswith(".parent.md") else explicit_parent.stem
        return RouteDecision(
            route="continue",
            reason="explicit module continuation arguments were provided",
            package_path=None,
            module=ModuleRoute(
                slug=slug,
                parent_brief=explicit_parent.resolve(),
                execution_contract=explicit_contract.resolve(),
                current_phase=current_phase.resolve(),
            ),
        )

    if explicit_package is not None:
        return RouteDecision(
            route="package",
            reason="explicit package path was provided",
            package_path=explicit_package.resolve(),
            module=None,
        )

    active_module = discover_active_module(repo_root)
    if active_module is not None:
        return RouteDecision(
            route="continue",
            reason="an active module with a next phase pointer exists, so continuation wins over package intake",
            package_path=None,
            module=active_module,
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
    route_mode: str = LEGACY_ENTRY_ROUTE_MODE,
    session_mode: str = LEGACY_SESSION_MODE,
    snapshot_mode: str = DEFAULT_SNAPSHOT_MODE,
    profile: str = "none",
) -> None:
    payload: dict[str, Any] = {
        "updated_at": utc_now().isoformat().replace("+00:00", "Z"),
        "route_mode": route_mode,
        "session_mode": session_mode,
        "snapshot_mode": snapshot_mode,
        "profile": profile,
        "route_signal": ROUTE_SIGNAL,
        "entry_route": decision.route,
        "route": decision.route,
        "reason": decision.reason,
        "package_path": decision.package_path.as_posix() if decision.package_path else None,
        "module": None,
    }
    if decision.module is not None:
        payload["module"] = {
            "slug": decision.module.slug,
            "execution_contract": decision.module.execution_contract.as_posix(),
            "parent_brief": decision.module.parent_brief.as_posix(),
            "current_phase": decision.module.current_phase.as_posix(),
        }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def normalize_route_argv(argv: list[str]) -> list[str]:
    if not argv:
        return []
    first = str(argv[0]).strip().lower()
    if first in {"auto", "package", "continue"}:
        return ["--route", first, *argv[1:]]
    return list(argv)


def uses_positional_route_alias(argv: list[str]) -> bool:
    if not argv:
        return False
    first = str(argv[0]).strip().lower()
    return first in {"auto", "package", "continue"}


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

    active_module = discover_active_module(repo_root)
    if active_module is not None:
        print("package_route_outcome: active_module_detected")
        print("next_governed_route: continue")
        print(f"execution_contract: {active_module.execution_contract.as_posix()}")
        print(f"parent_brief: {active_module.parent_brief.as_posix()}")
        print(f"current_phase: {active_module.current_phase.as_posix()}")
    else:
        print("package_route_outcome: no_active_module_detected")
    return 0


def run_continue_route(
    *,
    decision: RouteDecision,
    backend: str,
    artifact_root: Path,
    profile: str,
    worker_model: str,
    acceptor_model: str,
    remediation_model: str,
    skip_clean_check: bool,
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
    ]
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
    parser.add_argument("--route", choices=("auto", "package", "continue"), default="auto")
    parser.add_argument("--route-mode", default="legacy")
    parser.add_argument("--session-mode", default=LEGACY_SESSION_MODE)
    parser.add_argument("--snapshot-mode", default=DEFAULT_SNAPSHOT_MODE)
    parser.add_argument("--package-path", default=None)
    parser.add_argument("--execution-contract", default=None)
    parser.add_argument("--parent-brief", default=None)
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
    output_last_message = resolve_path(repo_root, args.output_last_message)
    explicit_package = resolve_path(repo_root, args.package_path) if args.package_path else None
    explicit_contract = resolve_path(repo_root, args.execution_contract) if args.execution_contract else None
    explicit_parent = resolve_path(repo_root, args.parent_brief) if args.parent_brief else None
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
            explicit_package=explicit_package if args.route in {"auto", "package"} else None,
            explicit_contract=explicit_contract if args.route in {"auto", "continue"} else None,
            explicit_parent=explicit_parent if args.route in {"auto", "continue"} else None,
            inbox=inbox,
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

    write_route_state(
        route_state_file,
        decision,
        route_mode=route_mode,
        session_mode=session_mode,
        snapshot_mode=snapshot_mode,
        profile=profile_marker,
    )

    print(f"route_mode: {route_mode}")
    print(f"session_mode: {session_mode}")
    print(f"snapshot_mode: {snapshot_mode}")
    print(f"profile: {profile_marker}")
    print(f"entry_route: {decision.route}")
    print(f"governed_route: {decision.route}")
    print(f"reason: {decision.reason}")
    if decision.package_path is not None:
        print(f"package_path: {decision.package_path.as_posix()}")
    if decision.module is not None:
        print(f"execution_contract: {decision.module.execution_contract.as_posix()}")
        print(f"parent_brief: {decision.module.parent_brief.as_posix()}")
        print(f"current_phase: {decision.module.current_phase.as_posix()}")

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
        backend=args.backend,
        artifact_root=artifact_root / "orchestration",
        profile=profile,
        worker_model=args.worker_model,
        acceptor_model=args.acceptor_model,
        remediation_model=args.remediation_model,
        skip_clean_check=bool(args.skip_clean_check),
        codex_bin=args.codex_bin,
        max_remediation_cycles=args.max_remediation_cycles,
    )


if __name__ == "__main__":
    raise SystemExit(main())
