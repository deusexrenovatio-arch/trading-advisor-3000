from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

from agent_process_telemetry import compute_process_rollup, load_task_outcomes
from harness_baseline_metrics import build_metrics


def _health_label(value: bool) -> str:
    return "green" if value else "yellow"


def _build_dashboard_payload(
    *,
    plans_path: Path,
    memory_path: Path,
    task_outcomes_path: Path,
) -> dict[str, Any]:
    baseline = build_metrics(
        plans_path=plans_path,
        memory_path=memory_path,
        task_outcomes_path=task_outcomes_path,
    )
    process_rollup = compute_process_rollup(load_task_outcomes(task_outcomes_path))
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
    lines.append("")
    return "\n".join(lines)


def run(
    *,
    plans_path: Path,
    memory_path: Path,
    task_outcomes_path: Path,
    output_json: Path,
    output_md: Path,
) -> int:
    payload = _build_dashboard_payload(
        plans_path=plans_path,
        memory_path=memory_path,
        task_outcomes_path=task_outcomes_path,
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
    parser.add_argument("--output-json", default="artifacts/governance-dashboard.json")
    parser.add_argument("--output-md", default="artifacts/governance-dashboard.md")
    args = parser.parse_args()
    raise SystemExit(
        run(
            plans_path=Path(args.plans),
            memory_path=Path(args.memory),
            task_outcomes_path=Path(args.task_outcomes),
            output_json=Path(args.output_json),
            output_md=Path(args.output_md),
        )
    )


if __name__ == "__main__":
    main()
