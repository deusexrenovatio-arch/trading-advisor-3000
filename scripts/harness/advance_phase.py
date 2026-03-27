from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from .models import parse_phase_acceptance_report, parse_phase_plan, parse_run_state
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import parse_phase_acceptance_report, parse_phase_plan, parse_run_state


class PhaseAdvanceError(RuntimeError):
    """Raised when phase advancement cannot be evaluated safely."""


@dataclass(frozen=True)
class PhaseAdvanceResult:
    run_id: str
    phase_id: str
    advanced: bool
    completed: bool
    status: str
    next_phase_id: str | None
    run_state_path: Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise PhaseAdvanceError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise PhaseAdvanceError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise PhaseAdvanceError(f"payload at `{path}` must be JSON object")
    return payload


def _resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _next_phase_id(
    *,
    phase_order: list[object],
    accepted_phase_ids: set[str],
) -> str | None:
    for phase in phase_order:
        phase_id = str(phase.phase_id)
        if phase_id in accepted_phase_ids:
            continue
        if all(dep in accepted_phase_ids for dep in phase.dependencies):
            return phase_id
    return None


def _append_event(
    *,
    events_path: Path,
    sequence: int,
    event_type: str,
    run_id: str,
    phase_id: str,
    details: dict[str, object],
) -> None:
    events_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "sequence": sequence,
        "timestamp": _utc_now(),
        "event_type": event_type,
        "run_id": run_id,
        "phase_id": phase_id,
        "details": details,
    }
    with events_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def run_phase_advancement(
    *,
    registry_root: Path,
    run_id: str,
    phase_id: str | None = None,
) -> PhaseAdvanceResult:
    run_state_path = registry_root / "runs" / run_id / "run_state.json"
    events_path = registry_root / "runs" / run_id / "events.jsonl"
    phase_plan = parse_phase_plan(_read_json(registry_root / "phases" / run_id / "phase_plan.json"))
    run_state = parse_run_state(_read_json(run_state_path))

    if phase_plan.run_id != run_id or run_state.run_id != run_id:
        raise PhaseAdvanceError("run_id mismatch between phase plan and run_state")

    selected_phase_id = (phase_id or run_state.current_phase_id or "").strip()
    if not selected_phase_id:
        raise PhaseAdvanceError("current phase is not set in run_state and --phase-id was not provided")

    acceptance_path = registry_root / "acceptance" / run_id / selected_phase_id / "phase_acceptance_report.json"
    acceptance = parse_phase_acceptance_report(_read_json(acceptance_path))
    if acceptance.run_id != run_id or acceptance.phase_id != selected_phase_id:
        raise PhaseAdvanceError("acceptance report scope does not match requested run/phase")

    accepted_phase_ids = set(run_state.accepted_phase_ids)
    rejected_phase_ids = set(run_state.rejected_phase_ids)
    sequence = int(run_state.last_event_sequence) + 1
    advanced = False

    if acceptance.verdict == "accepted":
        accepted_phase_ids.add(selected_phase_id)
        rejected_phase_ids.discard(selected_phase_id)
        next_phase = _next_phase_id(phase_order=list(phase_plan.phases), accepted_phase_ids=accepted_phase_ids)
        if next_phase is None:
            status = "completed"
            completed = True
            current_phase_id = None
            event_type = "phase_accepted_run_completed"
        else:
            status = "in_progress"
            completed = False
            current_phase_id = next_phase
            advanced = next_phase != selected_phase_id
            event_type = "phase_accepted_advanced"
    else:
        rejected_phase_ids.add(selected_phase_id)
        status = "blocked"
        completed = False
        current_phase_id = selected_phase_id
        next_phase = selected_phase_id
        event_type = "phase_rejected_blocked"

    updated_payload = {
        "run_id": run_state.run_id,
        "created_at": run_state.created_at,
        "updated_at": _utc_now(),
        "status": status,
        "current_phase_id": current_phase_id,
        "accepted_phase_ids": sorted(accepted_phase_ids),
        "rejected_phase_ids": sorted(rejected_phase_ids),
        "iteration_count": int(run_state.iteration_count),
        "last_event_sequence": sequence,
    }
    model = parse_run_state(updated_payload)
    _write_json(run_state_path, model.model_dump(mode="json"))
    _append_event(
        events_path=events_path,
        sequence=sequence,
        event_type=event_type,
        run_id=run_id,
        phase_id=selected_phase_id,
        details={
            "acceptance_verdict": acceptance.verdict,
            "advanced": advanced,
            "next_phase_id": next_phase if acceptance.verdict == "accepted" else None,
        },
    )

    return PhaseAdvanceResult(
        run_id=run_id,
        phase_id=selected_phase_id,
        advanced=advanced,
        completed=completed,
        status=model.status,
        next_phase_id=model.current_phase_id,
        run_state_path=run_state_path,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Advance harness run state to the next phase when gates pass.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--phase-id", default=None, help="Phase identifier (defaults to run_state.current_phase_id).")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    try:
        result = run_phase_advancement(
            registry_root=_resolve_path(repo_root, args.registry_root),
            run_id=args.run_id,
            phase_id=args.phase_id,
        )
    except PhaseAdvanceError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "phase_id": result.phase_id,
                "advanced": result.advanced,
                "completed": result.completed,
                "status": result.status,
                "next_phase_id": result.next_phase_id,
                "run_state": result.run_state_path.as_posix(),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
