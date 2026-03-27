from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

try:
    from .models import (
        parse_phase_acceptance_report,
        parse_phase_plan,
        parse_project_docs_bundle,
        parse_run_state,
        parse_traceability_matrix,
    )
    from .render_docs_from_registry import DOC_FILE_ORDER, build_rendered_docs
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import (
        parse_phase_acceptance_report,
        parse_phase_plan,
        parse_project_docs_bundle,
        parse_run_state,
        parse_traceability_matrix,
    )
    from scripts.harness.render_docs_from_registry import DOC_FILE_ORDER, build_rendered_docs


class RegistryConsistencyError(RuntimeError):
    """Raised when registry and generated docs drift from canonical constraints."""


@dataclass(frozen=True)
class RegistryConsistencyResult:
    run_id: str
    status: str
    checked_files: int


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise RegistryConsistencyError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RegistryConsistencyError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise RegistryConsistencyError(f"payload at `{path}` must be JSON object")
    return payload


def _resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def validate_registry_consistency(
    *,
    registry_root: Path,
    run_id: str,
    docs_root: Path,
    session_handoff_path: Path,
) -> RegistryConsistencyResult:
    docs_bundle = parse_project_docs_bundle(_read_json(registry_root / "intake" / run_id / "project_docs_bundle.json"))
    phase_plan = parse_phase_plan(_read_json(registry_root / "phases" / run_id / "phase_plan.json"))
    run_state = parse_run_state(_read_json(registry_root / "runs" / run_id / "run_state.json"))
    traceability = parse_traceability_matrix(_read_json(registry_root / "traceability" / run_id / "traceability_matrix.json"))

    if any(model.run_id != run_id for model in (docs_bundle, phase_plan, run_state, traceability)):
        raise RegistryConsistencyError("run_id mismatch across canonical registry artifacts")

    phase_ids = {phase.phase_id for phase in phase_plan.phases}
    if run_state.current_phase_id is not None and run_state.current_phase_id not in phase_ids:
        raise RegistryConsistencyError(
            f"run_state current_phase_id `{run_state.current_phase_id}` not present in phase plan"
        )

    accepted_ids = set(run_state.accepted_phase_ids)
    rejected_ids = set(run_state.rejected_phase_ids)
    if not accepted_ids.issubset(phase_ids):
        raise RegistryConsistencyError("run_state accepted_phase_ids contains unknown phase ids")
    if not rejected_ids.issubset(phase_ids):
        raise RegistryConsistencyError("run_state rejected_phase_ids contains unknown phase ids")

    for phase_id in sorted(accepted_ids):
        acceptance_path = registry_root / "acceptance" / run_id / phase_id / "phase_acceptance_report.json"
        acceptance = parse_phase_acceptance_report(_read_json(acceptance_path))
        if acceptance.verdict != "accepted":
            raise RegistryConsistencyError(
                f"phase `{phase_id}` marked as accepted in run_state but acceptance verdict is `{acceptance.verdict}`"
            )

    for mapping in traceability.mappings:
        if mapping.requirement_id and not set(mapping.phase_ids).issubset(phase_ids):
            raise RegistryConsistencyError(
                f"traceability mapping `{mapping.requirement_id}` references unknown phase ids"
            )

    if not session_handoff_path.exists():
        raise RegistryConsistencyError(f"session handoff file not found: {session_handoff_path}")
    handoff_text = session_handoff_path.read_text(encoding="utf-8").strip()
    if "## Active Task Note" not in handoff_text:
        raise RegistryConsistencyError("session handoff pointer is missing `## Active Task Note` section")

    expected_docs = build_rendered_docs(registry_root=registry_root, run_id=run_id)
    checked_files = 0
    for file_name in DOC_FILE_ORDER:
        expected = expected_docs[file_name]
        target = docs_root / file_name
        if not target.exists():
            raise RegistryConsistencyError(f"generated doc is missing: {target}")
        actual = target.read_text(encoding="utf-8")
        if actual != expected:
            raise RegistryConsistencyError(
                f"generated doc drift detected: {target} does not match canonical rendered content"
            )
        checked_files += 1

    return RegistryConsistencyResult(
        run_id=run_id,
        status="ok",
        checked_files=checked_files,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate consistency between canonical registry and generated outputs.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    parser.add_argument("--docs-root", default="docs/generated", help="Generated docs root (default: docs/generated).")
    parser.add_argument(
        "--session-handoff",
        default="docs/session_handoff.md",
        help="Session handoff pointer path (default: docs/session_handoff.md).",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    try:
        result = validate_registry_consistency(
            registry_root=_resolve_path(repo_root, args.registry_root),
            run_id=args.run_id,
            docs_root=_resolve_path(repo_root, args.docs_root),
            session_handoff_path=_resolve_path(repo_root, args.session_handoff),
        )
    except RegistryConsistencyError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "status": result.status,
                "checked_files": result.checked_files,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
