#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class LegacyToken:
    token_id: str
    token: str
    description: str


@dataclass(frozen=True)
class Classification:
    path_zone: str
    group: str
    risk: str
    wave: str
    cluster_id: str
    wave_owner: str


@dataclass(frozen=True)
class ReferenceRecord:
    file: str
    line: int
    token_id: str
    token: str
    scope: str
    path_zone: str
    group: str
    risk: str
    wave: str
    cluster_id: str
    wave_owner: str
    line_excerpt: str


LEGACY_TOKENS: tuple[LegacyToken, ...] = (
    LegacyToken(
        token_id="docs_architecture_app",
        token="docs/architecture/app/",
        description="legacy docs namespace candidate",
    ),
    LegacyToken(
        token_id="tests_app_path",
        token="tests/app/",
        description="legacy product tests namespace candidate",
    ),
    LegacyToken(
        token_id="src_product_app",
        token="src/trading_advisor_3000/app/",
        description="legacy product runtime namespace candidate",
    ),
    LegacyToken(
        token_id="python_import_app",
        token="trading_advisor_3000.app",
        description="legacy python import namespace candidate",
    ),
)

EXCLUDED_PREFIXES: tuple[str, ...] = (
    "artifacts/",
    "artifacts/codex/package-intake/",
    "codex_ai_delivery_shell_package/",
    "docs/tasks/archive/",
    "memory/",
    "plans/",
)

PACKAGING_FILES: set[str] = {
    "pyproject.toml",
    "setup.py",
    "setup.cfg",
    "requirements.txt",
    "requirements-dev.txt",
    "poetry.lock",
}

DOC_OPERATION_PREFIXES: tuple[str, ...] = (
    "docs/runbooks/",
    "docs/checklists/",
    "docs/workflows/",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_text_if_safe(path: Path) -> str | None:
    raw = path.read_bytes()
    if b"\x00" in raw:
        return None
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return None


def _git_ls_files(repo_root: Path) -> list[str]:
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"git ls-files failed: {(completed.stderr or b'').decode('utf-8', errors='ignore').strip()}"
        )
    rows = [item for item in completed.stdout.split(b"\x00") if item]
    return [row.decode("utf-8", errors="ignore").replace("\\", "/") for row in rows]


def classify_scope(rel_path: str) -> str:
    for prefix in EXCLUDED_PREFIXES:
        if rel_path.startswith(prefix):
            return "excluded-historical"
    return "active"


def _path_zone(rel_path: str) -> str:
    if rel_path == "CODEOWNERS" or rel_path.startswith(".github/workflows/"):
        return "ci-codeowners"
    if rel_path.startswith("scripts/"):
        return "scripts"
    if rel_path.startswith("tests/"):
        return "tests"
    if rel_path.startswith("src/trading_advisor_3000/"):
        return "runtime"
    if rel_path.startswith("docs/"):
        return "docs"
    if rel_path in PACKAGING_FILES or rel_path.startswith("deployment/"):
        return "packaging"
    return "other"


def classify_reference(rel_path: str, token: LegacyToken) -> Classification:
    path_zone = _path_zone(rel_path)

    if path_zone == "ci-codeowners":
        return Classification(
            path_zone=path_zone,
            group="ci-and-codeowners",
            risk="high",
            wave="governance-selector-cutover",
            cluster_id="governance-routing-selectors",
            wave_owner="process+platform",
        )

    if path_zone == "scripts":
        high_risk_markers = ("validate_", "run_", "gate", "selector", "codeowners")
        is_high_risk = any(marker in rel_path for marker in high_risk_markers)
        return Classification(
            path_zone=path_zone,
            group="scripts-validators",
            risk="high" if is_high_risk else "medium",
            wave="compatibility-bridge",
            cluster_id=(
                "validator-and-gate-paths"
                if is_high_risk
                else "script-path-references"
            ),
            wave_owner="platform",
        )

    if path_zone == "tests":
        return Classification(
            path_zone=path_zone,
            group="test-paths-fixtures",
            risk="medium",
            wave="runtime-test-cutover",
            cluster_id="test-namespace-dependencies",
            wave_owner="app-core+platform",
        )

    if path_zone == "runtime":
        return Classification(
            path_zone=path_zone,
            group="imports-code",
            risk="high",
            wave="runtime-test-cutover",
            cluster_id="runtime-namespace-dependencies",
            wave_owner="app-core+platform",
        )

    if path_zone == "packaging":
        return Classification(
            path_zone=path_zone,
            group="packaging-runbook",
            risk="medium",
            wave="compatibility-bridge",
            cluster_id="packaging-and-release-references",
            wave_owner="platform",
        )

    if path_zone == "docs":
        if rel_path.startswith(DOC_OPERATION_PREFIXES):
            return Classification(
                path_zone=path_zone,
                group="packaging-runbook",
                risk="medium",
                wave="compatibility-bridge",
                cluster_id="runbook-operational-references",
                wave_owner="architecture+platform",
            )
        if token.token_id == "docs_architecture_app":
            return Classification(
                path_zone=path_zone,
                group="docs-links",
                risk="low",
                wave="docs-subtree-rename",
                cluster_id="docs-architecture-linkage",
                wave_owner="architecture",
            )
        return Classification(
            path_zone=path_zone,
            group="docs-links",
            risk="low",
            wave="docs-subtree-rename",
            cluster_id="documentation-linkage",
            wave_owner="architecture",
        )

    if token.token_id in {"src_product_app", "python_import_app"}:
        return Classification(
            path_zone=path_zone,
            group="imports-code",
            risk="high",
            wave="runtime-test-cutover",
            cluster_id="runtime-namespace-dependencies",
            wave_owner="app-core+platform",
        )
    if token.token_id == "tests_app_path":
        return Classification(
            path_zone=path_zone,
            group="test-paths-fixtures",
            risk="medium",
            wave="runtime-test-cutover",
            cluster_id="test-namespace-dependencies",
            wave_owner="app-core+platform",
        )
    return Classification(
        path_zone=path_zone,
        group="docs-links",
        risk="low",
        wave="docs-subtree-rename",
        cluster_id="documentation-linkage",
        wave_owner="architecture",
    )


def collect_references(repo_root: Path) -> list[ReferenceRecord]:
    references: list[ReferenceRecord] = []
    tracked_paths = _git_ls_files(repo_root)

    for rel_path in tracked_paths:
        absolute = (repo_root / rel_path).resolve()
        if not absolute.exists() or not absolute.is_file():
            continue
        text = _read_text_if_safe(absolute)
        if text is None:
            continue
        scope = classify_scope(rel_path)
        for line_number, line in enumerate(text.splitlines(), start=1):
            for token in LEGACY_TOKENS:
                if token.token not in line:
                    continue
                classified = classify_reference(rel_path, token)
                references.append(
                    ReferenceRecord(
                        file=rel_path,
                        line=line_number,
                        token_id=token.token_id,
                        token=token.token,
                        scope=scope,
                        path_zone=classified.path_zone,
                        group=classified.group,
                        risk=classified.risk,
                        wave=classified.wave,
                        cluster_id=classified.cluster_id,
                        wave_owner=classified.wave_owner,
                        line_excerpt=line.strip()[:260],
                    )
                )
    references.sort(key=lambda item: (item.file, item.line, item.token_id))
    return references


def summarize_records(records: list[ReferenceRecord]) -> dict[str, object]:
    by_scope = Counter(item.scope for item in records)
    by_group = Counter(item.group for item in records if item.scope == "active")
    by_risk = Counter(item.risk for item in records if item.scope == "active")
    by_wave = Counter(item.wave for item in records if item.scope == "active")

    cluster_map: dict[str, dict[str, object]] = {}
    for item in records:
        cluster = cluster_map.get(item.cluster_id)
        if cluster is None:
            cluster = {
                "cluster_id": item.cluster_id,
                "group": item.group,
                "risk": item.risk,
                "wave": item.wave,
                "wave_owner": item.wave_owner,
                "total_references": 0,
                "active_references": 0,
                "excluded_references": 0,
                "files": set(),
            }
            cluster_map[item.cluster_id] = cluster
        cluster["total_references"] = int(cluster["total_references"]) + 1
        cluster["files"].add(item.file)
        if item.scope == "active":
            cluster["active_references"] = int(cluster["active_references"]) + 1
        else:
            cluster["excluded_references"] = int(cluster["excluded_references"]) + 1

    clusters: list[dict[str, object]] = []
    for raw in cluster_map.values():
        files_sorted = sorted(str(path) for path in raw["files"])
        clusters.append(
            {
                "cluster_id": raw["cluster_id"],
                "group": raw["group"],
                "risk": raw["risk"],
                "wave": raw["wave"],
                "wave_owner": raw["wave_owner"],
                "total_references": raw["total_references"],
                "active_references": raw["active_references"],
                "excluded_references": raw["excluded_references"],
                "distinct_files": len(files_sorted),
                "sample_files": files_sorted[:8],
            }
        )
    clusters.sort(
        key=lambda item: (
            -int(item["active_references"]),
            -int(item["total_references"]),
            str(item["cluster_id"]),
        )
    )

    return {
        "counts": {
            "total_references": len(records),
            "active_references": int(by_scope.get("active", 0)),
            "excluded_references": int(by_scope.get("excluded-historical", 0)),
        },
        "counts_by_group": dict(sorted(by_group.items())),
        "counts_by_risk": dict(sorted(by_risk.items())),
        "counts_by_wave": dict(sorted(by_wave.items())),
        "clusters": clusters,
        "excluded_prefixes": list(EXCLUDED_PREFIXES),
    }


def render_markdown(
    *,
    summary: dict[str, object],
    records: list[ReferenceRecord],
    output_json_path: Path,
) -> str:
    counts = summary["counts"]
    lines: list[str] = [
        "# Dual-Surface Rename Reference Inventory",
        "",
        f"Generated: {utc_now_iso()}",
        "",
        "## Scope Notes",
        "- Source: tracked files from `git ls-files`.",
        "- Legacy tokens: `docs/architecture/app/`, `tests/app/`, `src/trading_advisor_3000/app/`, `trading_advisor_3000.app`.",
        "- Excluded historical prefixes are counted separately and not assigned as active migration workload.",
        "",
        "## Totals",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Total matches | {counts['total_references']} |",
        f"| Active matches | {counts['active_references']} |",
        f"| Excluded historical matches | {counts['excluded_references']} |",
        "",
        "## Active Matches By Risk",
        "",
        "| Risk | Count |",
        "| --- | ---: |",
    ]

    for risk, value in summary["counts_by_risk"].items():
        lines.append(f"| {risk} | {value} |")

    lines.extend(
        [
            "",
            "## Active Matches By Wave",
            "",
            "| Wave | Count |",
            "| --- | ---: |",
        ]
    )
    for wave, value in summary["counts_by_wave"].items():
        lines.append(f"| {wave} | {value} |")

    lines.extend(
        [
            "",
            "## Wave Clusters",
            "",
            "| Cluster | Group | Risk | Wave | Owner | Active refs | Files |",
            "| --- | --- | --- | --- | --- | ---: | ---: |",
        ]
    )
    for cluster in summary["clusters"]:
        lines.append(
            "| "
            + f"{cluster['cluster_id']} | {cluster['group']} | {cluster['risk']} | "
            + f"{cluster['wave']} | {cluster['wave_owner']} | "
            + f"{cluster['active_references']} | {cluster['distinct_files']} |"
        )

    lines.extend(
        [
            "",
            "## Active Reference Sample (Top 80)",
            "",
            "| File | Line | Token | Group | Risk | Wave |",
            "| --- | ---: | --- | --- | --- | --- |",
        ]
    )
    active_records = [item for item in records if item.scope == "active"]
    for item in active_records[:80]:
        lines.append(
            "| "
            + f"`{item.file}` | {item.line} | `{item.token}` | {item.group} | "
            + f"{item.risk} | {item.wave} |"
        )

    lines.extend(
        [
            "",
            "## Machine-Readable Artifact",
            f"- JSON: `{output_json_path.as_posix()}`",
            "",
        ]
    )
    return "\n".join(lines)


def run(*, repo_root: Path, output_json: Path, output_md: Path) -> int:
    records = collect_references(repo_root)
    summary = summarize_records(records)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at": utc_now_iso(),
        "repo_root": repo_root.resolve().as_posix(),
        "legacy_tokens": [asdict(item) for item in LEGACY_TOKENS],
        "summary": summary,
        "references": [asdict(item) for item in records],
    }
    output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    output_md.write_text(
        render_markdown(summary=summary, records=records, output_json_path=output_json),
        encoding="utf-8",
    )

    counts = summary["counts"]
    print(
        "dual-surface inventory: "
        f"total={counts['total_references']} active={counts['active_references']} "
        f"excluded={counts['excluded_references']}"
    )
    print(f"json: {output_json.as_posix()}")
    print(f"markdown: {output_md.as_posix()}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build reference inventory for the dual-surface safe rename migration."
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument(
        "--output-json",
        default="artifacts/rename-migration/reference-inventory/legacy-reference-inventory.json",
    )
    parser.add_argument(
        "--output-md",
        default="artifacts/rename-migration/reference-inventory/legacy-reference-inventory.md",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    output_json = (repo_root / args.output_json).resolve()
    output_md = (repo_root / args.output_md).resolve()
    raise SystemExit(run(repo_root=repo_root, output_json=output_json, output_md=output_md))


if __name__ == "__main__":
    main()
