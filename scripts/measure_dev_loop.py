from __future__ import annotations

import argparse
import json
import math
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any

import yaml


TERMINAL_STATUSES = {"completed", "partial", "blocked"}


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": None, "items": []}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {"version": 1, "updated_at": None, "items": []}
    items = payload.get("items")
    if not isinstance(items, list):
        payload["items"] = []
    payload.setdefault("version", 1)
    payload.setdefault("updated_at", None)
    return payload


def _safe_ratio(numerator: int, denominator: int) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _as_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def compute_baseline(payload: dict[str, Any]) -> dict[str, Any]:
    rows = [row for row in payload.get("items", []) if isinstance(row, dict)]
    terminal_rows = [
        row
        for row in rows
        if str(row.get("outcome_status", "")).strip().lower() in TERMINAL_STATUSES
    ]

    timing_values = [
        value
        for value in (_as_positive_int(row.get("time_to_first_patch_sec")) for row in terminal_rows)
        if value is not None
    ]
    same_path_attempts = [
        value
        for value in (_as_positive_int(row.get("same_path_attempts")) for row in terminal_rows)
        if value is not None
    ]
    correct_first_time_count = sum(
        1
        for row in terminal_rows
        if str(row.get("decision_quality", "")).strip().lower() == "correct_first_time"
    )
    matched_count = sum(
        1
        for row in terminal_rows
        if str(row.get("route_match", "")).strip().lower() == "matched"
    )

    return {
        "generated_at": date.today().isoformat(),
        "task_outcomes_total": len(rows),
        "terminal_outcomes_total": len(terminal_rows),
        "outcomes_with_patch_timing": len(timing_values),
        "outcomes_without_patch_timing": max(len(terminal_rows) - len(timing_values), 0),
        "median_time_to_first_patch_sec": median(timing_values) if timing_values else None,
        "p90_time_to_first_patch_sec": (
            sorted(timing_values)[max(math.ceil(len(timing_values) * 0.9) - 1, 0)]
            if timing_values
            else None
        ),
        "median_same_path_attempts": median(same_path_attempts) if same_path_attempts else None,
        "correct_first_time_rate": _safe_ratio(correct_first_time_count, len(terminal_rows)),
        "start_route_match_rate": _safe_ratio(matched_count, len(terminal_rows)),
    }


def _render_markdown(metrics: dict[str, Any]) -> str:
    lines = [
        "# Dev Loop Baseline",
        "",
        f"- generated_at: {metrics.get('generated_at')}",
        "",
        "| Metric | Value |",
        "| --- | --- |",
    ]
    ordered_keys = (
        "task_outcomes_total",
        "terminal_outcomes_total",
        "outcomes_with_patch_timing",
        "outcomes_without_patch_timing",
        "median_time_to_first_patch_sec",
        "p90_time_to_first_patch_sec",
        "median_same_path_attempts",
        "correct_first_time_rate",
        "start_route_match_rate",
    )
    for key in ordered_keys:
        value = metrics.get(key)
        if isinstance(value, float):
            lines.append(f"| `{key}` | {value:.2f} |")
        else:
            lines.append(f"| `{key}` | {value} |")
    lines.append("")
    return "\n".join(lines)


def run(
    *,
    task_outcomes_path: Path,
    output: Path | None,
    output_format: str,
) -> int:
    payload = _load_yaml(task_outcomes_path)
    metrics = compute_baseline(payload)
    rendered = (
        _render_markdown(metrics)
        if output_format == "markdown"
        else json.dumps(metrics, ensure_ascii=False, indent=2) + "\n"
    )
    if output is None:
        print(rendered.rstrip())
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
        print(f"dev loop baseline written: {output.as_posix()}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure baseline development-loop metrics.")
    parser.add_argument("--task-outcomes-path", default="memory/task_outcomes.yaml")
    parser.add_argument("--output", default=None)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args()
    output = Path(args.output) if args.output else None
    raise SystemExit(
        run(
            task_outcomes_path=Path(args.task_outcomes_path),
            output=output,
            output_format=args.format,
        )
    )


if __name__ == "__main__":
    main()
