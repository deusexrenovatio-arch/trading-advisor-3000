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
from typing import Any, Callable, Sequence

from gate_common import collect_changed_files


DEFAULT_REPORT_PATH = Path("artifacts/shell-delivery-operational-proving.json")
REQUIRED_DASHBOARD_ARTIFACTS = (
    Path("artifacts/dev-loop-baseline.md"),
    Path("artifacts/harness-baseline-metrics.json"),
    Path("artifacts/process-improvement-report.md"),
    Path("artifacts/autonomy-kpi-report.md"),
    Path("artifacts/governance-dashboard.json"),
    Path("artifacts/governance-dashboard.md"),
)


@dataclass(frozen=True)
class ShellDeliveryProvingStep:
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


def build_shell_delivery_operational_proving_plan(
    *,
    mapping: str,
    scope_args: list[str],
    enforce_session_check: bool,
    include_nightly_lane: bool,
    include_dashboard_refresh: bool,
    gate_snapshot_mode: str = "changed-files",
    gate_profile: str = "none",
    python_executable: str = sys.executable,
) -> list[ShellDeliveryProvingStep]:
    loop_command = [
        python_executable,
        "scripts/run_loop_gate.py",
        "--mapping",
        mapping,
        "--snapshot-mode",
        gate_snapshot_mode,
        "--profile",
        gate_profile,
        "--enforce-explicit-markers",
    ]
    pr_command = [
        python_executable,
        "scripts/run_pr_gate.py",
        "--mapping",
        mapping,
        "--snapshot-mode",
        gate_snapshot_mode,
        "--profile",
        gate_profile,
        "--enforce-explicit-markers",
    ]
    if not enforce_session_check:
        loop_command.append("--skip-session-check")
        pr_command.append("--skip-session-check")
    loop_command.extend(scope_args)
    pr_command.extend(scope_args)

    plan = [
        ShellDeliveryProvingStep(
            lane="loop-lane",
            step_id="loop-gate",
            command=tuple(loop_command),
        ),
        ShellDeliveryProvingStep(
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
            "--snapshot-mode",
            gate_snapshot_mode,
            "--profile",
            gate_profile,
            *scope_args,
        ]
        plan.append(
            ShellDeliveryProvingStep(
                lane="nightly-lane",
                step_id="nightly-gate",
                command=tuple(nightly_command),
            )
        )

    if include_dashboard_refresh:
        plan.extend(
            [
                ShellDeliveryProvingStep(
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
                ShellDeliveryProvingStep(
                    lane="dashboard-refresh",
                    step_id="harness-baseline-metrics",
                    command=(
                        python_executable,
                        "scripts/harness_baseline_metrics.py",
                        "--output",
                        "artifacts/harness-baseline-metrics.json",
                    ),
                ),
                ShellDeliveryProvingStep(
                    lane="dashboard-refresh",
                    step_id="process-improvement-report",
                    command=(
                        python_executable,
                        "scripts/process_improvement_report.py",
                        "--output",
                        "artifacts/process-improvement-report.md",
                    ),
                ),
                ShellDeliveryProvingStep(
                    lane="dashboard-refresh",
                    step_id="autonomy-kpi-report",
                    command=(
                        python_executable,
                        "scripts/autonomy_kpi_report.py",
                        "--output",
                        "artifacts/autonomy-kpi-report.md",
                    ),
                ),
                ShellDeliveryProvingStep(
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
    baseline_by_path: dict[str, dict[str, Any]] | None = None,
    run_started_ns: int | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in artifact_paths:
        key = path.as_posix()
        baseline = (baseline_by_path or {}).get(key)
        exists = path.exists()
        size_bytes = path.stat().st_size if exists else 0
        mtime_ns = int(path.stat().st_mtime_ns) if exists else 0
        fresh_in_run = False
        if exists and size_bytes > 0:
            if baseline is None:
                fresh_in_run = run_started_ns is None or mtime_ns >= run_started_ns
            else:
                baseline_mtime_ns = int(baseline.get("mtime_ns", 0))
                baseline_size = int(baseline.get("size_bytes", 0))
                fresh_in_run = mtime_ns > baseline_mtime_ns or size_bytes != baseline_size
        rows.append(
            {
                "path": key,
                "exists": exists,
                "size_bytes": size_bytes,
                "mtime_ns": mtime_ns,
                "fresh_in_run": fresh_in_run,
            }
        )
    return rows


def run_shell_delivery_operational_proving_plan(
    *,
    plan: Sequence[ShellDeliveryProvingStep],
    report_path: Path | None,
    dry_run: bool,
    runner: Callable[[Sequence[str]], subprocess.CompletedProcess[str]] | None = None,
    dashboard_artifact_paths: Sequence[Path] = REQUIRED_DASHBOARD_ARTIFACTS,
    require_dashboard_artifacts: bool,
    write_report: bool = True,
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
    run_started_ns = time.time_ns()
    dashboard_baseline: dict[str, dict[str, Any]] = {}
    for path in dashboard_artifact_paths:
        key = path.as_posix()
        exists = path.exists()
        stat = path.stat() if exists else None
        dashboard_baseline[key] = {
            "exists": exists,
            "size_bytes": int(stat.st_size) if stat is not None else 0,
            "mtime_ns": int(stat.st_mtime_ns) if stat is not None else 0,
        }

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

    dashboard_artifacts = collect_dashboard_artifact_status(
        artifact_paths=dashboard_artifact_paths,
        baseline_by_path=dashboard_baseline,
        run_started_ns=run_started_ns,
    )
    missing_dashboard_artifacts = [
        str(row["path"])
        for row in dashboard_artifacts
        if not bool(row["exists"]) or int(row["size_bytes"]) <= 0
    ]
    stale_dashboard_artifacts = [
        str(row["path"])
        for row in dashboard_artifacts
        if bool(row["exists"]) and int(row["size_bytes"]) > 0 and not bool(row["fresh_in_run"])
    ]
    if (
        not dry_run
        and overall_status == "ok"
        and require_dashboard_artifacts
        and (missing_dashboard_artifacts or stale_dashboard_artifacts)
    ):
        overall_status = "failed"
        failure = {
            "lane": "dashboard-refresh",
            "step_id": "artifact-validation",
            "return_code": 2,
            "missing_artifacts": missing_dashboard_artifacts,
            "stale_artifacts": stale_dashboard_artifacts,
        }

    report = {
        "version": 1,
        "generated_at": _utc_now(),
        "status": overall_status,
        "steps": records,
        "failure": failure,
        "dashboard_artifacts": dashboard_artifacts,
    }
    if write_report and report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return (0 if overall_status in {"ok", "dry_run"} else 1), report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run shell-delivery operational proving lanes and write a consolidated evidence report."
    )
    parser.add_argument("--mapping", default="configs/change_surface_mapping.yaml")
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--output", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write-dry-run-report", action="store_true")
    parser.add_argument("--enforce-session-check", action="store_true")
    parser.add_argument("--skip-nightly-lane", action="store_true")
    parser.add_argument("--skip-dashboard-refresh", action="store_true")
    args = parser.parse_args()
    if args.dry_run and args.output and not args.write_dry_run_report:
        print(
            "shell delivery operational proving: dry-run report output is disabled by default. "
            "Use --write-dry-run-report to opt in."
        )
        return 2

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
    write_report = bool((not args.dry_run) or args.write_dry_run_report)
    output_path: Path | None = None
    if write_report:
        output_path = Path(args.output) if args.output else DEFAULT_REPORT_PATH
    plan = build_shell_delivery_operational_proving_plan(
        mapping=args.mapping,
        scope_args=scope_args,
        enforce_session_check=args.enforce_session_check,
        include_nightly_lane=include_nightly_lane,
        include_dashboard_refresh=include_dashboard_refresh,
    )
    exit_code, report = run_shell_delivery_operational_proving_plan(
        plan=plan,
        report_path=output_path,
        dry_run=args.dry_run,
        require_dashboard_artifacts=include_dashboard_refresh,
        write_report=write_report,
    )
    report_target = output_path.as_posix() if output_path is not None else "not-written"
    print(
        "shell delivery operational proving: "
        f"{report['status']} "
        f"(steps={len(report['steps'])}, report={report_target})"
    )
    if report.get("failure"):
        print(f"shell delivery operational proving failure details: {json.dumps(report['failure'], ensure_ascii=False)}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
