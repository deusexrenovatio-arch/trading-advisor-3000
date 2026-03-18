from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

from gate_common import collect_changed_files


DEFAULT_REPORT_PATH = Path("artifacts/phase8-operational-proving.json")
REQUIRED_DASHBOARD_ARTIFACTS = (
    Path("artifacts/dev-loop-baseline.md"),
    Path("artifacts/harness-baseline-metrics.json"),
    Path("artifacts/process-improvement-report.md"),
    Path("artifacts/autonomy-kpi-report.md"),
    Path("artifacts/governance-dashboard.json"),
    Path("artifacts/governance-dashboard.md"),
)


@dataclass(frozen=True)
class Phase8Step:
    lane: str
    step_id: str
    command: tuple[str, ...]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _render_command(command: Sequence[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(list(command))
    return shlex.join(command)


def _tail_text(text: str, *, max_lines: int = 40, max_chars: int = 4_000) -> str:
    lines = text.splitlines()
    tail = "\n".join(lines[-max_lines:])
    if len(tail) > max_chars:
        return tail[-max_chars:]
    return tail


def _gate_scope_args(
    *,
    changed_files: list[str],
    explicit_scope_mode: bool,
    base_ref: str | None,
    head_ref: str | None,
    from_git: bool,
    git_ref: str,
) -> list[str]:
    if explicit_scope_mode:
        return ["--changed-files", *changed_files]
    if base_ref and head_ref:
        return ["--base-ref", base_ref, "--head-ref", head_ref]
    if from_git:
        return ["--from-git", "--git-ref", git_ref]
    return []


def build_phase8_plan(
    *,
    mapping: str,
    scope_args: list[str],
    enforce_session_check: bool,
    include_nightly_lane: bool,
    include_dashboard_refresh: bool,
    python_executable: str = sys.executable,
) -> list[Phase8Step]:
    loop_command = [
        python_executable,
        "scripts/run_loop_gate.py",
        "--mapping",
        mapping,
    ]
    pr_command = [
        python_executable,
        "scripts/run_pr_gate.py",
        "--mapping",
        mapping,
    ]
    if not enforce_session_check:
        loop_command.append("--skip-session-check")
        pr_command.append("--skip-session-check")
    loop_command.extend(scope_args)
    pr_command.extend(scope_args)

    plan = [
        Phase8Step(
            lane="loop-lane",
            step_id="loop-gate",
            command=tuple(loop_command),
        ),
        Phase8Step(
            lane="pr-lane",
            step_id="pr-gate",
            command=tuple(pr_command),
        ),
    ]

    if include_nightly_lane:
        nightly_command = [
            python_executable,
            "scripts/run_nightly_gate.py",
            "--mapping",
            mapping,
            *scope_args,
        ]
        plan.append(
            Phase8Step(
                lane="nightly-lane",
                step_id="nightly-gate",
                command=tuple(nightly_command),
            )
        )

    if include_dashboard_refresh:
        plan.extend(
            [
                Phase8Step(
                    lane="dashboard-refresh",
                    step_id="measure-dev-loop",
                    command=(
                        python_executable,
                        "scripts/measure_dev_loop.py",
                        "--format",
                        "markdown",
                        "--output",
                        "artifacts/dev-loop-baseline.md",
                    ),
                ),
                Phase8Step(
                    lane="dashboard-refresh",
                    step_id="harness-baseline-metrics",
                    command=(
                        python_executable,
                        "scripts/harness_baseline_metrics.py",
                        "--output",
                        "artifacts/harness-baseline-metrics.json",
                    ),
                ),
                Phase8Step(
                    lane="dashboard-refresh",
                    step_id="process-improvement-report",
                    command=(
                        python_executable,
                        "scripts/process_improvement_report.py",
                        "--output",
                        "artifacts/process-improvement-report.md",
                    ),
                ),
                Phase8Step(
                    lane="dashboard-refresh",
                    step_id="autonomy-kpi-report",
                    command=(
                        python_executable,
                        "scripts/autonomy_kpi_report.py",
                        "--output",
                        "artifacts/autonomy-kpi-report.md",
                    ),
                ),
                Phase8Step(
                    lane="dashboard-refresh",
                    step_id="build-governance-dashboard",
                    command=(
                        python_executable,
                        "scripts/build_governance_dashboard.py",
                        "--output-json",
                        "artifacts/governance-dashboard.json",
                        "--output-md",
                        "artifacts/governance-dashboard.md",
                    ),
                ),
            ]
        )
    return plan


def collect_dashboard_artifact_status(
    *,
    artifact_paths: Sequence[Path] = REQUIRED_DASHBOARD_ARTIFACTS,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in artifact_paths:
        exists = path.exists()
        size_bytes = path.stat().st_size if exists else 0
        rows.append(
            {
                "path": path.as_posix(),
                "exists": exists,
                "size_bytes": size_bytes,
            }
        )
    return rows


def run_phase8_plan(
    *,
    plan: Sequence[Phase8Step],
    report_path: Path,
    dry_run: bool,
    runner: Callable[[Sequence[str]], subprocess.CompletedProcess[str]] | None = None,
    dashboard_artifact_paths: Sequence[Path] = REQUIRED_DASHBOARD_ARTIFACTS,
    require_dashboard_artifacts: bool,
) -> tuple[int, dict[str, object]]:
    run_command = (
        runner
        if runner is not None
        else lambda command: subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
        )
    )
    records: list[dict[str, object]] = []
    overall_status = "dry_run" if dry_run else "ok"
    failure: dict[str, object] | None = None

    for step in plan:
        command_rendered = _render_command(step.command)
        started_at = _utc_now()
        started_clock = time.perf_counter()
        print(f">>> [{step.lane}] {command_rendered}", flush=True)
        if dry_run:
            records.append(
                {
                    "lane": step.lane,
                    "step_id": step.step_id,
                    "command": command_rendered,
                    "status": "planned",
                    "return_code": None,
                    "started_at": started_at,
                    "finished_at": _utc_now(),
                    "duration_sec": 0.0,
                    "stdout_tail": "",
                    "stderr_tail": "",
                }
            )
            continue

        completed = run_command(step.command)
        finished_at = _utc_now()
        duration_sec = round(time.perf_counter() - started_clock, 3)
        status = "ok" if completed.returncode == 0 else "failed"
        step_record = {
            "lane": step.lane,
            "step_id": step.step_id,
            "command": command_rendered,
            "status": status,
            "return_code": int(completed.returncode),
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_sec": duration_sec,
            "stdout_tail": _tail_text(completed.stdout),
            "stderr_tail": _tail_text(completed.stderr),
        }
        records.append(step_record)
        if completed.returncode != 0:
            overall_status = "failed"
            failure = {
                "lane": step.lane,
                "step_id": step.step_id,
                "return_code": int(completed.returncode),
            }
            break

    dashboard_artifacts = collect_dashboard_artifact_status(artifact_paths=dashboard_artifact_paths)
    missing_dashboard_artifacts = [
        str(row["path"])
        for row in dashboard_artifacts
        if not bool(row["exists"]) or int(row["size_bytes"]) <= 0
    ]
    if (
        not dry_run
        and overall_status == "ok"
        and require_dashboard_artifacts
        and missing_dashboard_artifacts
    ):
        overall_status = "failed"
        failure = {
            "lane": "dashboard-refresh",
            "step_id": "artifact-validation",
            "return_code": 2,
            "missing_artifacts": missing_dashboard_artifacts,
        }

    report = {
        "version": 1,
        "generated_at": _utc_now(),
        "status": overall_status,
        "steps": records,
        "failure": failure,
        "dashboard_artifacts": dashboard_artifacts,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return (0 if overall_status in {"ok", "dry_run"} else 1), report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 8 operational proving lanes and write a consolidated evidence report."
    )
    parser.add_argument("--mapping", default="configs/change_surface_mapping.yaml")
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--output", default=DEFAULT_REPORT_PATH.as_posix())
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--enforce-session-check", action="store_true")
    parser.add_argument("--skip-nightly-lane", action="store_true")
    parser.add_argument("--skip-dashboard-refresh", action="store_true")
    args = parser.parse_args()

    explicit_scope_mode = bool(args.changed_files) or args.stdin
    use_from_git = bool(args.from_git) or (
        not args.base_ref and not args.head_ref and not explicit_scope_mode
    )
    changed_files = collect_changed_files(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        git_ref=args.git_ref,
        from_git=use_from_git,
        changed_files=list(args.changed_files),
        from_stdin=args.stdin,
    )
    scope_args = _gate_scope_args(
        changed_files=changed_files,
        explicit_scope_mode=explicit_scope_mode,
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        from_git=use_from_git,
        git_ref=args.git_ref,
    )
    include_nightly_lane = not args.skip_nightly_lane
    include_dashboard_refresh = not args.skip_dashboard_refresh
    plan = build_phase8_plan(
        mapping=args.mapping,
        scope_args=scope_args,
        enforce_session_check=args.enforce_session_check,
        include_nightly_lane=include_nightly_lane,
        include_dashboard_refresh=include_dashboard_refresh,
    )
    exit_code, report = run_phase8_plan(
        plan=plan,
        report_path=Path(args.output),
        dry_run=args.dry_run,
        require_dashboard_artifacts=include_dashboard_refresh,
    )
    print(
        "phase8 operational proving: "
        f"{report['status']} "
        f"(steps={len(report['steps'])}, report={Path(args.output).as_posix()})"
    )
    if report.get("failure"):
        print(f"phase8 failure details: {json.dumps(report['failure'], ensure_ascii=False)}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
