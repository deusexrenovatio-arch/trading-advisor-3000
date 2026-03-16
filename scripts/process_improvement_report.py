from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from agent_process_telemetry import compute_process_rollup, load_task_outcomes, render_rollup_markdown


def run(
    *,
    task_outcomes_path: Path,
    output: Path,
    summary_file: Path | None,
    output_format: str,
    window_size: int,
) -> int:
    payload = load_task_outcomes(task_outcomes_path)
    rollup = compute_process_rollup(payload, window_size=window_size)
    if output_format == "json":
        rendered = json.dumps(rollup, ensure_ascii=False, indent=2) + "\n"
    else:
        rendered = render_rollup_markdown(rollup)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(f"process improvement report written: {output.as_posix()}")

    if summary_file is not None:
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        with summary_file.open("a", encoding="utf-8") as handle:
            if output_format == "json":
                handle.write("Process improvement report JSON written to artifact.\n")
            else:
                handle.write(rendered + "\n")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Build process-improvement report from task outcomes.")
    parser.add_argument("--task-outcomes-path", default="memory/task_outcomes.yaml")
    parser.add_argument("--output", default="artifacts/process-improvement-report.md")
    parser.add_argument("--summary-file", default=None)
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    parser.add_argument("--window-size", type=int, default=20)
    args = parser.parse_args()
    summary_path = Path(args.summary_file) if args.summary_file else None
    sys.exit(
        run(
            task_outcomes_path=Path(args.task_outcomes_path),
            output=Path(args.output),
            summary_file=summary_path,
            output_format=args.format,
            window_size=args.window_size,
        )
    )


if __name__ == "__main__":
    main()
