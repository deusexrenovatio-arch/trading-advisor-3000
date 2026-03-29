from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _bootstrap_direct_file_imports() -> None:
    """Ensure absolute `scripts.harness.*` imports work for file-based execution."""

    if __package__:
        return
    repo_root = Path(__file__).resolve().parents[2]
    repo_root_text = str(repo_root)
    if repo_root_text not in sys.path:
        sys.path.insert(0, repo_root_text)


_bootstrap_direct_file_imports()

try:
    from .advance_phase import run_phase_advancement
    from .build_phase_context import run_phase_context_build
    from .intake_spec_bundle import run_bundle_intake
    from .models import parse_phase_plan, parse_run_state
    from .normalize_requirements import run_requirements_normalization
    from .plan_phases import run_phase_planning
    from .render_docs_from_registry import run_docs_render
    from .run_phase_acceptance import run_phase_acceptance_stage
    from .run_phase_implementation import run_phase_implementation_stage
    from .run_phase_review import run_phase_review_stage
    from .synthesize_project_docs import run_project_docs_synthesis
    from .traceability import rebuild_traceability_matrix
    from .validate_registry_consistency import validate_registry_consistency
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.advance_phase import run_phase_advancement
    from scripts.harness.build_phase_context import run_phase_context_build
    from scripts.harness.intake_spec_bundle import run_bundle_intake
    from scripts.harness.models import parse_phase_plan, parse_run_state
    from scripts.harness.normalize_requirements import run_requirements_normalization
    from scripts.harness.plan_phases import run_phase_planning
    from scripts.harness.render_docs_from_registry import run_docs_render
    from scripts.harness.run_phase_acceptance import run_phase_acceptance_stage
    from scripts.harness.run_phase_implementation import run_phase_implementation_stage
    from scripts.harness.run_phase_review import run_phase_review_stage
    from scripts.harness.synthesize_project_docs import run_project_docs_synthesis
    from scripts.harness.traceability import rebuild_traceability_matrix
    from scripts.harness.validate_registry_consistency import validate_registry_consistency


class HarnessOrchestrationError(RuntimeError):
    """Raised when harness orchestration cannot proceed safely."""


@dataclass(frozen=True)
class RunCurrentPhaseResult:
    run_id: str
    phase_id: str
    attempts: int
    final_verdict: str
    run_state_status: str
    next_phase_id: str | None
    retry_limit_reached: bool


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise HarnessOrchestrationError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise HarnessOrchestrationError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise HarnessOrchestrationError(f"payload at `{path}` must be JSON object")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_run_state(registry_root: Path, run_id: str):
    state = parse_run_state(_read_json(registry_root / "runs" / run_id / "run_state.json"))
    if state.run_id != run_id:
        raise HarnessOrchestrationError(
            f"run_state run_id mismatch: expected `{run_id}`, got `{state.run_id}`"
        )
    return state


def _load_phase_plan(registry_root: Path, run_id: str):
    phase_plan = parse_phase_plan(_read_json(registry_root / "phases" / run_id / "phase_plan.json"))
    if phase_plan.run_id != run_id:
        raise HarnessOrchestrationError(
            f"phase_plan run_id mismatch: expected `{run_id}`, got `{phase_plan.run_id}`"
        )
    return phase_plan


def _next_ready_phase(phase_plan, accepted_phase_ids: set[str]) -> str | None:
    for phase in phase_plan.phases:
        if phase.phase_id in accepted_phase_ids:
            continue
        if all(dep in accepted_phase_ids for dep in phase.dependencies):
            return phase.phase_id
    return None


def _append_event(
    *,
    registry_root: Path,
    run_id: str,
    event_type: str,
    phase_id: str | None,
    details: dict[str, object],
) -> None:
    run_state = _load_run_state(registry_root, run_id)
    sequence = int(run_state.last_event_sequence) + 1
    updated_payload = {
        "run_id": run_state.run_id,
        "created_at": run_state.created_at,
        "updated_at": _utc_now(),
        "status": run_state.status,
        "current_phase_id": run_state.current_phase_id,
        "accepted_phase_ids": list(run_state.accepted_phase_ids),
        "rejected_phase_ids": list(run_state.rejected_phase_ids),
        "iteration_count": int(run_state.iteration_count),
        "last_event_sequence": sequence,
    }
    model = parse_run_state(updated_payload)
    _write_json(registry_root / "runs" / run_id / "run_state.json", model.model_dump(mode="json"))

    events_path = registry_root / "runs" / run_id / "events.jsonl"
    event_payload = {
        "sequence": sequence,
        "timestamp": _utc_now(),
        "event_type": event_type,
        "run_id": run_id,
        "phase_id": phase_id,
        "details": details,
    }
    with events_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event_payload, ensure_ascii=False) + "\n")


def _update_run_state(
    *,
    registry_root: Path,
    run_id: str,
    status: str | None = None,
    current_phase_id: str | None | object = None,
    iteration_count: int | None = None,
) -> None:
    state = _load_run_state(registry_root, run_id)
    payload: dict[str, object] = {
        "run_id": state.run_id,
        "created_at": state.created_at,
        "updated_at": _utc_now(),
        "status": status or state.status,
        "current_phase_id": state.current_phase_id,
        "accepted_phase_ids": list(state.accepted_phase_ids),
        "rejected_phase_ids": list(state.rejected_phase_ids),
        "iteration_count": int(state.iteration_count) if iteration_count is None else int(iteration_count),
        "last_event_sequence": int(state.last_event_sequence),
    }
    if current_phase_id is not None:
        payload["current_phase_id"] = current_phase_id
    model = parse_run_state(payload)
    _write_json(registry_root / "runs" / run_id / "run_state.json", model.model_dump(mode="json"))


def _initialize_run_state(
    *,
    registry_root: Path,
    run_id: str,
) -> Path:
    phase_plan = _load_phase_plan(registry_root, run_id)
    first_phase_id = _next_ready_phase(phase_plan, accepted_phase_ids=set())
    payload = {
        "run_id": run_id,
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "status": "initialized",
        "current_phase_id": first_phase_id,
        "accepted_phase_ids": [],
        "rejected_phase_ids": [],
        "iteration_count": 0,
        "last_event_sequence": 0,
    }
    state_model = parse_run_state(payload)
    state_path = registry_root / "runs" / run_id / "run_state.json"
    _write_json(state_path, state_model.model_dump(mode="json"))

    events_path = registry_root / "runs" / run_id / "events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_text("", encoding="utf-8")
    _append_event(
        registry_root=registry_root,
        run_id=run_id,
        event_type="run_initialized",
        phase_id=first_phase_id,
        details={"status": "initialized"},
    )
    return state_path


def _resolve_phase_id(*, registry_root: Path, run_id: str, phase_id: str | None) -> str:
    state = _load_run_state(registry_root, run_id)
    selected = (phase_id or state.current_phase_id or "").strip()
    if not selected:
        raise HarnessOrchestrationError("run has no current phase to execute")
    if phase_id and state.current_phase_id and phase_id != state.current_phase_id:
        raise HarnessOrchestrationError(
            "cannot execute phase out of order: requested phase does not match run_state.current_phase_id"
        )
    phase_plan = _load_phase_plan(registry_root, run_id)
    allowed = {phase.phase_id: phase for phase in phase_plan.phases}
    if selected not in allowed:
        raise HarnessOrchestrationError(
            f"phase `{selected}` is not present in phase plan for run `{run_id}`"
        )
    accepted_ids = set(state.accepted_phase_ids)
    dependencies = set(allowed[selected].dependencies)
    if not dependencies.issubset(accepted_ids):
        missing = ", ".join(sorted(dependencies - accepted_ids))
        raise HarnessOrchestrationError(
            f"phase `{selected}` has unsatisfied dependencies: {missing or '<none>'}"
        )
    return selected


def run_current_phase(
    *,
    registry_root: Path,
    run_id: str,
    phase_id: str | None,
    retry_ceiling: int,
    backend: str,
    quality_profile: str,
    docs_root: Path,
    auto_render_docs: bool,
    prompt_file: Path,
    codex_bin: str | None,
) -> RunCurrentPhaseResult:
    selected_phase = _resolve_phase_id(registry_root=registry_root, run_id=run_id, phase_id=phase_id)
    retry_ceiling = max(int(retry_ceiling), 0)
    attempts_total = retry_ceiling + 1
    _append_event(
        registry_root=registry_root,
        run_id=run_id,
        event_type="phase_execution_started",
        phase_id=selected_phase,
        details={"retry_ceiling": retry_ceiling},
    )

    final_verdict = "rejected"
    retry_limit_reached = False
    attempts_done = 0

    for _attempt_index in range(1, attempts_total + 1):
        attempts_done += 1
        context_result = run_phase_context_build(
            registry_root=registry_root,
            run_id=run_id,
            phase_id=selected_phase,
        )
        implementation = run_phase_implementation_stage(
            registry_root=registry_root,
            run_id=run_id,
            phase_id=selected_phase,
            backend=backend,
            quality_profile=quality_profile,
            prompt_file=prompt_file,
            repo_root=Path(__file__).resolve().parents[2],
            codex_bin=codex_bin,
        )
        _update_run_state(
            registry_root=registry_root,
            run_id=run_id,
            status="in_progress",
            current_phase_id=selected_phase,
            iteration_count=implementation.iteration,
        )
        _append_event(
            registry_root=registry_root,
            run_id=run_id,
            event_type="implementation_completed",
            phase_id=selected_phase,
            details={
                "phase_context": context_result.output_path.as_posix(),
                "implementation_summary": implementation.output_path.as_posix(),
                "iteration": implementation.iteration,
            },
        )

        rebuild_traceability_matrix(registry_root=registry_root, run_id=run_id)
        review = run_phase_review_stage(
            registry_root=registry_root,
            run_id=run_id,
            phase_id=selected_phase,
        )
        _append_event(
            registry_root=registry_root,
            run_id=run_id,
            event_type="review_completed",
            phase_id=selected_phase,
            details={"phase_review_report": review.output_path.as_posix(), "verdict": review.verdict},
        )

        rebuild_traceability_matrix(registry_root=registry_root, run_id=run_id)
        acceptance = run_phase_acceptance_stage(
            registry_root=registry_root,
            run_id=run_id,
            phase_id=selected_phase,
        )
        rebuild_traceability_matrix(registry_root=registry_root, run_id=run_id)
        _append_event(
            registry_root=registry_root,
            run_id=run_id,
            event_type="acceptance_completed",
            phase_id=selected_phase,
            details={
                "phase_acceptance_report": acceptance.output_path.as_posix(),
                "verdict": acceptance.verdict,
                "phase_rework_request": acceptance.rework_request_path.as_posix()
                if acceptance.rework_request_path
                else None,
            },
        )

        final_verdict = acceptance.verdict
        advance_result = run_phase_advancement(
            registry_root=registry_root,
            run_id=run_id,
            phase_id=selected_phase,
        )
        if acceptance.verdict == "accepted":
            if auto_render_docs:
                run_docs_render(registry_root=registry_root, run_id=run_id, docs_root=docs_root)
            return RunCurrentPhaseResult(
                run_id=run_id,
                phase_id=selected_phase,
                attempts=attempts_done,
                final_verdict=final_verdict,
                run_state_status=advance_result.status,
                next_phase_id=advance_result.next_phase_id,
                retry_limit_reached=False,
            )

        if attempts_done >= attempts_total:
            retry_limit_reached = True
            _update_run_state(
                registry_root=registry_root,
                run_id=run_id,
                status="failed",
                current_phase_id=selected_phase,
            )
            _append_event(
                registry_root=registry_root,
                run_id=run_id,
                event_type="retry_ceiling_exceeded",
                phase_id=selected_phase,
                details={"attempts": attempts_done, "retry_ceiling": retry_ceiling},
            )
            break

        _update_run_state(
            registry_root=registry_root,
            run_id=run_id,
            status="in_progress",
            current_phase_id=selected_phase,
        )
        _append_event(
            registry_root=registry_root,
            run_id=run_id,
            event_type="rework_loop_continues",
            phase_id=selected_phase,
            details={"next_attempt": attempts_done + 1},
        )

    state = _load_run_state(registry_root, run_id)
    if auto_render_docs:
        run_docs_render(registry_root=registry_root, run_id=run_id, docs_root=docs_root)
    return RunCurrentPhaseResult(
        run_id=run_id,
        phase_id=selected_phase,
        attempts=attempts_done,
        final_verdict=final_verdict,
        run_state_status=state.status,
        next_phase_id=state.current_phase_id,
        retry_limit_reached=retry_limit_reached,
    )


def run_to_completion(
    *,
    registry_root: Path,
    run_id: str,
    retry_ceiling: int,
    backend: str,
    quality_profile: str,
    docs_root: Path,
    auto_render_docs: bool,
    prompt_file: Path,
    codex_bin: str | None,
) -> dict[str, object]:
    phase_results: list[dict[str, object]] = []
    while True:
        state = _load_run_state(registry_root, run_id)
        if state.status == "completed":
            break
        if state.status == "failed":
            break
        result = run_current_phase(
            registry_root=registry_root,
            run_id=run_id,
            phase_id=state.current_phase_id,
            retry_ceiling=retry_ceiling,
            backend=backend,
            quality_profile=quality_profile,
            docs_root=docs_root,
            auto_render_docs=auto_render_docs,
            prompt_file=prompt_file,
            codex_bin=codex_bin,
        )
        phase_results.append(
            {
                "phase_id": result.phase_id,
                "attempts": result.attempts,
                "final_verdict": result.final_verdict,
                "run_state_status": result.run_state_status,
                "next_phase_id": result.next_phase_id,
                "retry_limit_reached": result.retry_limit_reached,
            }
        )
        state = _load_run_state(registry_root, run_id)
        if state.status in {"completed", "failed"}:
            break
        if result.final_verdict != "accepted":
            break

    final_state = _load_run_state(registry_root, run_id)
    return {
        "run_id": run_id,
        "final_status": final_state.status,
        "current_phase_id": final_state.current_phase_id,
        "accepted_phase_ids": list(final_state.accepted_phase_ids),
        "rejected_phase_ids": list(final_state.rejected_phase_ids),
        "phase_results": phase_results,
    }


def run_plan(
    *,
    registry_root: Path,
    run_id: str,
    docs_root: Path,
    auto_render_docs: bool,
) -> dict[str, object]:
    normalization = run_requirements_normalization(registry_root=registry_root, run_id=run_id)
    docs_bundle = run_project_docs_synthesis(registry_root=registry_root, run_id=run_id)
    planning = run_phase_planning(registry_root=registry_root, run_id=run_id)
    traceability_path = rebuild_traceability_matrix(registry_root=registry_root, run_id=run_id)
    run_state_path = _initialize_run_state(registry_root=registry_root, run_id=run_id)
    state = _load_run_state(registry_root, run_id)
    phase_context_path = None
    if state.current_phase_id:
        phase_context = run_phase_context_build(
            registry_root=registry_root,
            run_id=run_id,
            phase_id=state.current_phase_id,
        )
        phase_context_path = phase_context.output_path.as_posix()
    if auto_render_docs:
        run_docs_render(registry_root=registry_root, run_id=run_id, docs_root=docs_root)

    return {
        "run_id": run_id,
        "normalized_requirements": normalization.output_path.as_posix(),
        "project_docs_bundle": docs_bundle.output_path.as_posix(),
        "phase_plan": planning.output_path.as_posix(),
        "traceability_matrix": traceability_path.as_posix(),
        "run_state": run_state_path.as_posix(),
        "current_phase_context": phase_context_path,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase-driven local harness orchestrator.")
    parser.add_argument(
        "mode",
        choices=(
            "intake",
            "plan",
            "run-current-phase",
            "run-to-completion",
            "render-docs",
            "validate",
        ),
        help="Harness operation mode.",
    )
    parser.add_argument("--run-id", required=False, help="Harness run identifier.")
    parser.add_argument("--phase-id", required=False, help="Target phase identifier.")
    parser.add_argument("--input-zip", required=False, help="Input .zip for intake mode.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    parser.add_argument("--docs-root", default="docs/generated", help="Generated docs root (default: docs/generated).")
    parser.add_argument("--retry-ceiling", type=int, default=2, help="Maximum rework retries per phase.")
    parser.add_argument("--backend", choices=("simulate", "codex-cli"), default="simulate")
    parser.add_argument(
        "--quality-profile",
        choices=("improve-on-rework", "always-pass", "always-fail"),
        default="improve-on-rework",
        help="Simulation quality profile for implementation stage.",
    )
    parser.add_argument(
        "--prompt-file",
        default="configs/harness/prompts/implementer.prompt.md",
        help="Prompt template for codex-cli implementation backend.",
    )
    parser.add_argument("--codex-bin", default=None, help="Optional codex binary path.")
    parser.add_argument(
        "--session-handoff",
        default="docs/session_handoff.md",
        help="Session handoff pointer path for validate mode.",
    )
    parser.add_argument(
        "--no-render-docs",
        action="store_true",
        help="Skip docs render/update in plan and execution modes.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    registry_root = _resolve_path(repo_root, args.registry_root)
    docs_root = _resolve_path(repo_root, args.docs_root)
    auto_render_docs = not bool(args.no_render_docs)

    try:
        if args.mode == "intake":
            if not args.input_zip:
                raise HarnessOrchestrationError("--input-zip is required for intake mode")
            result = run_bundle_intake(
                input_zip=_resolve_path(repo_root, args.input_zip),
                registry_root=registry_root,
                run_id=args.run_id,
            )
            payload = {
                "run_id": result.run_id,
                "run_root": result.run_root.as_posix(),
                "spec_manifest": result.manifest_path.as_posix(),
                "extracted_text_cache": result.text_cache_path.as_posix(),
                "open_questions": result.open_questions_path.as_posix(),
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if not args.run_id:
            raise HarnessOrchestrationError("--run-id is required for this mode")

        if args.mode == "plan":
            payload = run_plan(
                registry_root=registry_root,
                run_id=args.run_id,
                docs_root=docs_root,
                auto_render_docs=auto_render_docs,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.mode == "run-current-phase":
            result = run_current_phase(
                registry_root=registry_root,
                run_id=args.run_id,
                phase_id=args.phase_id,
                retry_ceiling=args.retry_ceiling,
                backend=args.backend,
                quality_profile=args.quality_profile,
                docs_root=docs_root,
                auto_render_docs=auto_render_docs,
                prompt_file=_resolve_path(repo_root, args.prompt_file),
                codex_bin=args.codex_bin,
            )
            print(
                json.dumps(
                    {
                        "run_id": result.run_id,
                        "phase_id": result.phase_id,
                        "attempts": result.attempts,
                        "final_verdict": result.final_verdict,
                        "run_state_status": result.run_state_status,
                        "next_phase_id": result.next_phase_id,
                        "retry_limit_reached": result.retry_limit_reached,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        if args.mode == "run-to-completion":
            payload = run_to_completion(
                registry_root=registry_root,
                run_id=args.run_id,
                retry_ceiling=args.retry_ceiling,
                backend=args.backend,
                quality_profile=args.quality_profile,
                docs_root=docs_root,
                auto_render_docs=auto_render_docs,
                prompt_file=_resolve_path(repo_root, args.prompt_file),
                codex_bin=args.codex_bin,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.mode == "render-docs":
            result = run_docs_render(registry_root=registry_root, run_id=args.run_id, docs_root=docs_root)
            print(
                json.dumps(
                    {
                        "run_id": result.run_id,
                        "docs_root": result.docs_root.as_posix(),
                        "registry_generated_root": result.registry_generated_root.as_posix(),
                        "render_manifest": result.manifest_path.as_posix(),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        if args.mode == "validate":
            result = validate_registry_consistency(
                registry_root=registry_root,
                run_id=args.run_id,
                docs_root=docs_root,
                session_handoff_path=_resolve_path(repo_root, args.session_handoff),
            )
            print(json.dumps(result.__dict__, ensure_ascii=False, indent=2))
            return

        raise HarnessOrchestrationError(f"unsupported mode: {args.mode}")
    except Exception as exc:
        if isinstance(exc, HarnessOrchestrationError):
            raise SystemExit(str(exc)) from exc
        raise


if __name__ == "__main__":
    main()
