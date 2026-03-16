from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

from agent_process_telemetry import compute_process_rollup, load_task_outcomes


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"YAML payload must be object: {path}")
    return payload


def run(*, task_outcomes_path: Path, thresholds_path: Path) -> int:
    payload = load_task_outcomes(task_outcomes_path)
    thresholds_payload = _load_yaml(thresholds_path)
    window_size = int(thresholds_payload.get("window_size", 20))
    metrics_thresholds = thresholds_payload.get("thresholds", {})
    if not isinstance(metrics_thresholds, dict):
        metrics_thresholds = {}

    rollup = compute_process_rollup(payload, window_size=window_size)
    if not rollup["burn_in_complete"]:
        print(
            "process regressions validation: OK "
            f"(burn_in=False completed={rollup['completed_tasks_count']} window_size={window_size})"
        )
        return 0

    metrics = rollup["current_metrics"]
    errors: list[str] = []

    def _check_min(name: str) -> None:
        if name not in metrics_thresholds:
            return
        threshold = float(metrics_thresholds[name])
        value = float(metrics.get(name, 0.0))
        if value < threshold:
            errors.append(f"{name} below threshold: {value:.2f} < {threshold:.2f}")

    def _check_max(name: str) -> None:
        if name not in metrics_thresholds:
            return
        threshold = float(metrics_thresholds[name])
        value = float(metrics.get(name, 0.0))
        if value > threshold:
            errors.append(f"{name} above threshold: {value:.2f} > {threshold:.2f}")

    _check_min("correct_first_time_pct")
    _check_min("start_match_pct")
    _check_max("context_expansion_rate")
    _check_max("repeat_error_rate")
    _check_max("environment_blocker_rate")

    if errors:
        print("process regressions validation failed:")
        for err in errors:
            print(f"- {err}")
        print("remediation: see docs/runbooks/governance-remediation.md")
        return 1

    print(
        "process regressions validation: OK "
        f"(burn_in=True window_tasks={rollup['window_tasks_count']} window_size={window_size})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate rolling process metrics against thresholds.")
    parser.add_argument("--task-outcomes-path", default="memory/task_outcomes.yaml")
    parser.add_argument("--thresholds", default="configs/process_regression_thresholds.yaml")
    args = parser.parse_args()
    sys.exit(
        run(
            task_outcomes_path=Path(args.task_outcomes_path),
            thresholds_path=Path(args.thresholds),
        )
    )


if __name__ == "__main__":
    main()
