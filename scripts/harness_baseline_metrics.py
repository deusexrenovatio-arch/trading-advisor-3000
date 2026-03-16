from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from agent_process_telemetry import compute_process_rollup, load_task_outcomes
from measure_dev_loop import compute_baseline


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def build_metrics(
    *,
    plans_path: Path,
    memory_path: Path,
    task_outcomes_path: Path,
) -> dict[str, Any]:
    plans_payload = _load_yaml(plans_path)
    memory_payload = _load_yaml(memory_path)
    outcomes_payload = load_task_outcomes(task_outcomes_path)

    plans_items = _safe_list(plans_payload.get("items"))
    completed_plan_items = [
        row
        for row in plans_items
        if isinstance(row, dict) and str(row.get("status", "")).strip() == "completed"
    ]
    active_plan_items = [
        row
        for row in plans_items
        if isinstance(row, dict) and str(row.get("status", "")).strip() == "active"
    ]

    process_rollup = compute_process_rollup(outcomes_payload)
    loop_baseline = compute_baseline(outcomes_payload)

    decisions = _safe_list(memory_payload.get("decisions"))
    incidents = _safe_list(memory_payload.get("incidents"))
    patterns = _safe_list(memory_payload.get("patterns"))

    return {
        "generated_at": date.today().isoformat(),
        "plans": {
            "total_items": len(plans_items),
            "active_items": len(active_plan_items),
            "completed_items": len(completed_plan_items),
        },
        "memory": {
            "decisions": len(decisions),
            "incidents": len(incidents),
            "patterns": len(patterns),
        },
        "process_rollup": process_rollup,
        "dev_loop_baseline": loop_baseline,
    }


def _render_markdown(metrics: dict[str, Any]) -> str:
    plans = metrics.get("plans", {})
    memory = metrics.get("memory", {})
    rollup = metrics.get("process_rollup", {})
    loop = metrics.get("dev_loop_baseline", {})
    lines = [
        "# Harness Baseline Metrics",
        "",
        f"- generated_at: {metrics.get('generated_at')}",
        "",
        "## Inventory",
        f"- plans_total: {plans.get('total_items', 0)}",
        f"- plans_active: {plans.get('active_items', 0)}",
        f"- plans_completed: {plans.get('completed_items', 0)}",
        f"- memory_decisions: {memory.get('decisions', 0)}",
        f"- memory_incidents: {memory.get('incidents', 0)}",
        f"- memory_patterns: {memory.get('patterns', 0)}",
        "",
        "## Process Rollup",
        f"- completed_tasks_count: {rollup.get('completed_tasks_count', 0)}",
        f"- window_tasks_count: {rollup.get('window_tasks_count', 0)}",
        f"- burn_in_complete: {rollup.get('burn_in_complete', False)}",
        "",
        "## Dev Loop Baseline",
        f"- terminal_outcomes_total: {loop.get('terminal_outcomes_total', 0)}",
        f"- outcomes_with_patch_timing: {loop.get('outcomes_with_patch_timing', 0)}",
        f"- median_time_to_first_patch_sec: {loop.get('median_time_to_first_patch_sec')}",
        f"- correct_first_time_rate: {float(loop.get('correct_first_time_rate', 0.0)):.2f}",
        "",
    ]
    return "\n".join(lines)


def run(
    *,
    plans_path: Path,
    memory_path: Path,
    task_outcomes_path: Path,
    output: Path,
    output_format: str,
) -> int:
    metrics = build_metrics(
        plans_path=plans_path,
        memory_path=memory_path,
        task_outcomes_path=task_outcomes_path,
    )
    if output_format == "markdown":
        rendered = _render_markdown(metrics)
    else:
        rendered = json.dumps(metrics, ensure_ascii=False, indent=2) + "\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(f"harness baseline metrics written: {output.as_posix()}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Build baseline metrics for governance harness.")
    parser.add_argument("--plans", default="plans/PLANS.yaml")
    parser.add_argument("--memory", default="memory/agent_memory.yaml")
    parser.add_argument("--task-outcomes", default="memory/task_outcomes.yaml")
    parser.add_argument("--output", default="artifacts/harness-baseline-metrics.json")
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args()
    raise SystemExit(
        run(
            plans_path=Path(args.plans),
            memory_path=Path(args.memory),
            task_outcomes_path=Path(args.task_outcomes),
            output=Path(args.output),
            output_format=args.format,
        )
    )


if __name__ == "__main__":
    main()
