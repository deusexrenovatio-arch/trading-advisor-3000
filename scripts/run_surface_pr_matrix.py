from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from compute_change_surface import compute_surface
from gate_common import collect_changed_files, run_commands


@dataclass(frozen=True)
class PrSurfaceMatrixPlan:
    contour: str
    gate_profile: str
    dependency_profiles: tuple[str, ...]
    install_extras: tuple[str, ...]
    checks: tuple[str, ...]


RUNTIME_CHECKS = (
    "python -m pytest tests/product-plane/unit/test_phase6_fastapi_smoke.py -q",
    "python -m pytest tests/product-plane/unit/test_phase6_runtime_durable_bootstrap.py -q",
    "python -m pytest tests/product-plane/unit/test_real_execution_http_transport.py -q",
    "python -m pytest tests/process/test_run_f1d_sidecar_immutable_evidence.py -q",
)

DATA_CHECKS = (
    "python -m pytest tests/product-plane/unit/test_phase2a_spark_proof_runner.py -q",
    "python -m pytest tests/product-plane/unit/test_phase2a_spark_runtime_dirs.py -q",
    "python -m pytest tests/product-plane/integration/test_phase2a_data_plane.py -q",
    "python -m pytest tests/product-plane/integration/test_phase2a_dagster_execution.py -q",
    "python -m pytest tests/product-plane/integration/test_phase2a_spark_execution.py -q",
    "python -m pytest tests/product-plane/integration/test_phase2b_research_plane.py -q",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_python_command(raw: str) -> str:
    python_exec = sys.executable
    if " " in python_exec:
        python_exec = f"\"{python_exec}\""
    return raw.replace("python", python_exec, 1)


def _dedupe(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        marker = value.strip()
        if not marker or marker in seen:
            continue
        seen.add(marker)
        ordered.append(marker)
    return tuple(ordered)


def select_contour(surface_result: dict[str, Any]) -> str:
    surfaces = {str(item) for item in surface_result.get("surfaces", [])}
    has_runtime = "app-runtime" in surfaces
    has_data = "app-data" in surfaces
    has_app_generic = "app" in surfaces
    if has_runtime and has_data:
        return "mixed-integration"
    if has_data:
        return "data-proof"
    if has_runtime:
        return "runtime-publication"
    if has_app_generic:
        return "mixed-integration"
    return "governance-only"


def build_pr_surface_matrix_plan(surface_result: dict[str, Any]) -> PrSurfaceMatrixPlan:
    contour = select_contour(surface_result)
    if contour == "runtime-publication":
        return PrSurfaceMatrixPlan(
            contour=contour,
            gate_profile="runtime-api",
            dependency_profiles=("base", "runtime-api", "dev-test"),
            install_extras=("runtime-api", "dev-test"),
            checks=tuple(_resolve_python_command(command) for command in RUNTIME_CHECKS),
        )
    if contour == "data-proof":
        return PrSurfaceMatrixPlan(
            contour=contour,
            gate_profile="data-proof",
            dependency_profiles=("base", "runtime-api", "data-proof", "proof-docker", "dev-test"),
            install_extras=("runtime-api", "data-proof", "proof-docker", "dev-test"),
            checks=tuple(_resolve_python_command(command) for command in DATA_CHECKS),
        )
    if contour == "mixed-integration":
        checks = _dedupe(
            [_resolve_python_command(command) for command in (*RUNTIME_CHECKS, *DATA_CHECKS)]
        )
        return PrSurfaceMatrixPlan(
            contour=contour,
            gate_profile="integration",
            dependency_profiles=("base", "runtime-api", "data-proof", "proof-docker", "dev-test"),
            install_extras=("runtime-api", "data-proof", "proof-docker", "dev-test"),
            checks=checks,
        )
    return PrSurfaceMatrixPlan(
        contour=contour,
        gate_profile="governance",
        dependency_profiles=("base", "dev-test"),
        install_extras=("dev-test",),
        checks=tuple(),
    )


def _write_summary(*, summary_file: str, payload: dict[str, Any]) -> None:
    if not summary_file:
        return
    summary_path = Path(summary_file)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "## pr-surface-matrix",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Contour: `{payload['contour']}`",
        f"- Gate profile: `{payload['gate_profile']}`",
        f"- Dependency profiles: `{', '.join(payload['dependency_profiles'])}`",
        f"- Install extras: `{payload['install_extras_csv'] or 'none'}`",
        f"- Primary surface: `{payload['primary_surface']}`",
        f"- Surfaces: `{', '.join(payload['surfaces'])}`",
        f"- Changed files: `{len(payload['changed_files'])}`",
        "",
        "### Checks",
    ]
    checks = payload.get("checks") or []
    if checks:
        for command in checks:
            lines.append(f"- `{command}`")
    else:
        lines.append("- none")
    lines.append("")
    with summary_path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def _emit_github_output(*, output_path: str, payload: dict[str, Any]) -> None:
    if not output_path:
        return
    output_lines = [
        f"ci_contour={payload['contour']}",
        f"gate_profile={payload['gate_profile']}",
        f"dependency_profiles={','.join(payload['dependency_profiles'])}",
        f"install_extras={payload['install_extras_csv']}",
        f"checks_count={len(payload['checks'])}",
    ]
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(output_lines) + "\n")


def _payload_from_plan(
    *,
    plan: PrSurfaceMatrixPlan,
    surface_result: dict[str, Any],
    changed_files: list[str],
) -> dict[str, Any]:
    install_extras_csv = ",".join(plan.install_extras)
    return {
        "version": 1,
        "generated_at": _utc_now(),
        "contour": plan.contour,
        "gate_profile": plan.gate_profile,
        "dependency_profiles": list(plan.dependency_profiles),
        "install_extras": list(plan.install_extras),
        "install_extras_csv": install_extras_csv,
        "primary_surface": surface_result["primary_surface"],
        "surfaces": list(surface_result["surfaces"]),
        "changed_files": list(changed_files),
        "checks": list(plan.checks),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run profile-aware PR matrix checks for app/runtime/data contours.")
    parser.add_argument("--mapping", default="configs/change_surface_mapping.yaml")
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--summary-file", default="")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--emit-github-output", default="")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    changed_files = collect_changed_files(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        git_ref=args.git_ref,
        from_git=args.from_git,
        changed_files=list(args.changed_files),
        from_stdin=args.stdin,
    )
    surface_result = compute_surface(changed_files, mapping_path=Path(args.mapping))
    plan = build_pr_surface_matrix_plan(surface_result)
    payload = _payload_from_plan(plan=plan, surface_result=surface_result, changed_files=changed_files)

    if args.output_json:
        output_json_path = Path(args.output_json)
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        output_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_summary(summary_file=args.summary_file, payload=payload)
    _emit_github_output(output_path=args.emit_github_output, payload=payload)

    print(
        "surface pr matrix: "
        f"contour={plan.contour} "
        f"gate_profile={plan.gate_profile} "
        f"dependency_profiles={','.join(plan.dependency_profiles)} "
        f"checks={len(plan.checks)}"
    )
    if args.plan_only:
        return 0

    if not plan.checks:
        print("surface pr matrix: no additional app contour checks required")
        return 0

    code, failed_command = run_commands(list(plan.checks))
    if code != 0:
        print(
            "surface pr matrix: FAILED "
            f"(contour={plan.contour} command={failed_command})"
        )
        return code
    print(f"surface pr matrix: OK (contour={plan.contour})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
