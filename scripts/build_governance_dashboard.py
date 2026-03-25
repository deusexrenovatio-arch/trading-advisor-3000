from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

from agent_process_telemetry import compute_process_rollup, load_task_outcomes
from critical_contours import SOLUTION_CLASSES, extract_solution_intent, normalize_text
from harness_baseline_metrics import build_metrics

PILOT_OBSERVATION_WINDOW_START = date(2026, 3, 25)
PILOT_OBSERVATION_WINDOW_MIN_DAYS = 7
PILOT_OBSERVATION_WINDOW_MAX_DAYS = 14


def _health_label(value: bool) -> str:
    return "green" if value else "yellow"


def _observation_window_status(observation_window_days: int) -> str:
    if observation_window_days < PILOT_OBSERVATION_WINDOW_MIN_DAYS:
        return "collecting"
    if observation_window_days <= PILOT_OBSERVATION_WINDOW_MAX_DAYS:
        return "review_ready"
    return "review_due"


def _section_fields(lines: list[str], heading: str) -> dict[str, str]:
    start = -1
    for index, raw in enumerate(lines):
        if raw.strip() == heading:
            start = index
            break
    if start < 0:
        return {}
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].strip().startswith("## "):
            end = index
            break
    fields: dict[str, str] = {}
    for raw in lines[start + 1 : end]:
        stripped = raw.strip()
        if not stripped.startswith("- "):
            continue
        body = stripped[2:]
        if ":" not in body:
            continue
        key, value = body.split(":", 1)
        normalized_key = key.strip().lower().replace(" ", "_").replace("-", "_")
        fields[normalized_key] = value.strip()
    return fields


def _extract_note_updated_date(lines: list[str]) -> date | None:
    for raw in lines:
        stripped = raw.strip()
        if not stripped.lower().startswith("updated:"):
            continue
        value = stripped.split(":", 1)[1].strip()
        if not value:
            return None
        token = value.split(" ", 1)[0]
        try:
            return date.fromisoformat(token)
        except ValueError:
            return None
    return None


def _note_values_text(lines: list[str]) -> str:
    values: list[str] = []
    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped.startswith("- "):
            body = stripped[2:]
            if ":" in body:
                _key, value = body.split(":", 1)
                values.append(value.strip().lower())
                continue
            values.append(body.lower())
            continue
        values.append(stripped.lower())
    return "\n".join(values)


def _pilot_observation_counters(task_notes_root: Path, *, pilot_start: date) -> dict[str, Any]:
    observation_window_days = max((date.today() - pilot_start).days, 0)
    window_status = _observation_window_status(observation_window_days)
    if not task_notes_root.exists():
        return {
            "status": "yellow",
            "critical_tasks_with_explicit_solution_class": 0,
            "blocked_shortcut_claims": 0,
            "staged_vs_target_declarations": {"staged": 0, "target": 0},
            "observation_window_start": pilot_start.isoformat(),
            "observation_window_days": observation_window_days,
            "observation_window_min_days": PILOT_OBSERVATION_WINDOW_MIN_DAYS,
            "observation_window_max_days": PILOT_OBSERVATION_WINDOW_MAX_DAYS,
            "observation_window_status": window_status,
            "task_notes_scanned": 0,
        }

    explicit_solution_class = 0
    blocked_shortcut_claims = 0
    staged_declarations = 0
    target_declarations = 0
    scanned = 0

    for path in sorted(task_notes_root.rglob("*.md")):
        if not path.is_file():
            continue
        lines = path.read_text(encoding="utf-8").splitlines()
        scanned += 1
        note_date = _extract_note_updated_date(lines)
        if note_date is None or note_date < pilot_start:
            continue

        intent = extract_solution_intent(lines)
        solution_class = normalize_text(intent.get("solution_class", ""))
        critical_contour = normalize_text(intent.get("critical_contour", ""))
        if solution_class not in SOLUTION_CLASSES or critical_contour in {"", "none"}:
            continue
        explicit_solution_class += 1
        if solution_class == "staged":
            staged_declarations += 1
        if solution_class == "target":
            target_declarations += 1

        outcome_fields = _section_fields(lines, "## Task Outcome")
        outcome_status = normalize_text(outcome_fields.get("outcome_status", ""))
        note_text = _note_values_text(lines)
        if outcome_status == "blocked" and "shortcut" in note_text:
            blocked_shortcut_claims += 1

    return {
        "status": _health_label(explicit_solution_class > 0),
        "critical_tasks_with_explicit_solution_class": explicit_solution_class,
        "blocked_shortcut_claims": blocked_shortcut_claims,
        "staged_vs_target_declarations": {
            "staged": staged_declarations,
            "target": target_declarations,
        },
        "observation_window_start": pilot_start.isoformat(),
        "observation_window_days": observation_window_days,
        "observation_window_min_days": PILOT_OBSERVATION_WINDOW_MIN_DAYS,
        "observation_window_max_days": PILOT_OBSERVATION_WINDOW_MAX_DAYS,
        "observation_window_status": window_status,
        "task_notes_scanned": scanned,
    }


def _build_dashboard_payload(
    *,
    plans_path: Path,
    memory_path: Path,
    task_outcomes_path: Path,
    task_notes_root: Path,
    pilot_observation_start: date,
) -> dict[str, Any]:
    baseline = build_metrics(
        plans_path=plans_path,
        memory_path=memory_path,
        task_outcomes_path=task_outcomes_path,
    )
    process_rollup = compute_process_rollup(load_task_outcomes(task_outcomes_path))
    pilot_observation = _pilot_observation_counters(task_notes_root, pilot_start=pilot_observation_start)
    loop = baseline.get("dev_loop_baseline", {})
    plans = baseline.get("plans", {})
    memory = baseline.get("memory", {})

    has_terminal_outcomes = int(loop.get("terminal_outcomes_total", 0)) > 0
    has_plan_registry = int(plans.get("total_items", 0)) > 0
    has_memory_seed = (
        int(memory.get("decisions", 0)) > 0 or int(memory.get("patterns", 0)) > 0
    )
    has_process_signal = int(process_rollup.get("completed_tasks_count", 0)) > 0

    sections = {
        "lifecycle": {
            "status": _health_label(has_terminal_outcomes),
            "terminal_outcomes_total": loop.get("terminal_outcomes_total", 0),
            "correct_first_time_rate": loop.get("correct_first_time_rate", 0.0),
        },
        "state": {
            "status": _health_label(has_plan_registry and has_memory_seed),
            "plans_total": plans.get("total_items", 0),
            "memory_decisions": memory.get("decisions", 0),
            "memory_patterns": memory.get("patterns", 0),
        },
        "quality": {
            "status": _health_label(has_process_signal),
            "process_completed_tasks": process_rollup.get("completed_tasks_count", 0),
            "process_window_tasks": process_rollup.get("window_tasks_count", 0),
            "burn_in_complete": process_rollup.get("burn_in_complete", False),
        },
        "pilot_observation": pilot_observation,
    }

    return {
        "generated_at": date.today().isoformat(),
        "overall_status": _health_label(
            all(section.get("status") == "green" for section in sections.values())
        ),
        "sections": sections,
        "baseline_metrics": baseline,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    sections = payload.get("sections", {})
    lines = [
        "# Governance Dashboard",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- overall_status: {payload.get('overall_status')}",
        "",
        "| Section | Status | Notes |",
        "| --- | --- | --- |",
    ]
    lifecycle = sections.get("lifecycle", {})
    state = sections.get("state", {})
    quality = sections.get("quality", {})
    observation = sections.get("pilot_observation", {})
    staged_vs_target = observation.get("staged_vs_target_declarations", {})
    lines.append(
        "| lifecycle | "
        f"{lifecycle.get('status', 'yellow')} | "
        f"terminal_outcomes={lifecycle.get('terminal_outcomes_total', 0)}, "
        f"correct_first_time_rate={float(lifecycle.get('correct_first_time_rate', 0.0)):.2f} |"
    )
    lines.append(
        "| state | "
        f"{state.get('status', 'yellow')} | "
        f"plans_total={state.get('plans_total', 0)}, "
        f"memory_decisions={state.get('memory_decisions', 0)}, "
        f"memory_patterns={state.get('memory_patterns', 0)} |"
    )
    lines.append(
        "| quality | "
        f"{quality.get('status', 'yellow')} | "
        f"completed_tasks={quality.get('process_completed_tasks', 0)}, "
        f"window_tasks={quality.get('process_window_tasks', 0)}, "
        f"burn_in={quality.get('burn_in_complete', False)} |"
    )
    lines.append(
        "| pilot_observation | "
        f"{observation.get('status', 'yellow')} | "
        f"critical_tasks_with_explicit_solution_class={observation.get('critical_tasks_with_explicit_solution_class', 0)}, "
        f"blocked_shortcut_claims={observation.get('blocked_shortcut_claims', 0)}, "
        f"staged={staged_vs_target.get('staged', 0)}, "
        f"target={staged_vs_target.get('target', 0)}, "
        f"observation_window_start={observation.get('observation_window_start', 'none')}, "
        f"observation_window_days={observation.get('observation_window_days', 0)}, "
        f"observation_window_status={observation.get('observation_window_status', 'collecting')} |"
    )
    lines.append("")
    return "\n".join(lines)


def run(
    *,
    plans_path: Path,
    memory_path: Path,
    task_outcomes_path: Path,
    task_notes_root: Path,
    pilot_observation_start: date,
    output_json: Path,
    output_md: Path,
) -> int:
    payload = _build_dashboard_payload(
        plans_path=plans_path,
        memory_path=memory_path,
        task_outcomes_path=task_outcomes_path,
        task_notes_root=task_notes_root,
        pilot_observation_start=pilot_observation_start,
    )

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    output_md.write_text(_render_markdown(payload), encoding="utf-8")

    print(f"governance dashboard written: {output_json.as_posix()} and {output_md.as_posix()}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Build governance dashboard artifacts.")
    parser.add_argument("--plans", default="plans/PLANS.yaml")
    parser.add_argument("--memory", default="memory/agent_memory.yaml")
    parser.add_argument("--task-outcomes", default="memory/task_outcomes.yaml")
    parser.add_argument("--task-notes-root", default="docs/tasks")
    parser.add_argument("--pilot-observation-start", default=PILOT_OBSERVATION_WINDOW_START.isoformat())
    parser.add_argument("--output-json", default="artifacts/governance-dashboard.json")
    parser.add_argument("--output-md", default="artifacts/governance-dashboard.md")
    args = parser.parse_args()
    try:
        pilot_observation_start = date.fromisoformat(args.pilot_observation_start)
    except ValueError as exc:
        raise SystemExit(f"invalid --pilot-observation-start: {exc}") from exc
    raise SystemExit(
        run(
            plans_path=Path(args.plans),
            memory_path=Path(args.memory),
            task_outcomes_path=Path(args.task_outcomes),
            task_notes_root=Path(args.task_notes_root),
            pilot_observation_start=pilot_observation_start,
            output_json=Path(args.output_json),
            output_md=Path(args.output_md),
        )
    )


if __name__ == "__main__":
    main()
