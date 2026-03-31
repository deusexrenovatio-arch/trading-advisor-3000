from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from phase_tz_compiler import CompiledPhasePlan, compile_phase_plan

REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"
EXECUTION_CONTRACT_HEADING = "## Release Target Contract"
MANDATORY_REAL_CONTOURS_HEADING = "## Mandatory Real Contours"
RELEASE_SURFACE_MATRIX_HEADING = "## Release Surface Matrix"
PHASE_IMPACT_HEADING = "## Release Gate Impact"
PHASE_OWNERSHIP_HEADING = "## Release Surface Ownership"
PHASE_LIMITS_HEADING = "## What This Phase Does Not Prove"
EXECUTION_CONTRACT_PREFIXES = (
    "- target decision:",
    "- target environment:",
    "- forbidden proof substitutes:",
    "- release-ready proof class:",
)
PHASE_IMPACT_PREFIXES = (
    "- surface transition:",
    "- minimum proof class:",
    "- accepted state label:",
)
PHASE_OWNERSHIP_PREFIXES = (
    "- owned surfaces:",
    "- delivered proof class:",
    "- required real bindings:",
    "- target downgrade is forbidden:",
)
ALLOWED_PROOF_CLASSES = {"doc", "schema", "unit", "integration", "staging-real", "live-real"}
ALLOWED_ACCEPTED_STATE_LABELS = {"prep_closed", "real_contour_closed", "release_decision"}
REAL_PROOF_CLASSES = {"staging-real", "live-real"}


def _normalize_changed_files(paths: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        normalized = str(raw).replace("\\", "/").strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _collect_changed_files(
    *,
    base_sha: str | None,
    head_sha: str | None,
    changed_files_override: list[str] | None,
) -> list[str]:
    if changed_files_override is not None:
        return _normalize_changed_files(changed_files_override)
    if not base_sha or not head_sha:
        return []
    completed = subprocess.run(
        ["git", "diff", "--name-only", f"{base_sha}..{head_sha}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return []
    rows = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    return _normalize_changed_files(rows)


def _find_heading_line(lines: list[str], heading: str) -> int:
    for idx, raw in enumerate(lines):
        if raw.strip() == heading:
            return idx
    return -1


def _section_lines(lines: list[str], heading: str) -> list[str]:
    start = _find_heading_line(lines, heading)
    if start < 0:
        return []
    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            end = idx
            break
    return lines[start + 1 : end]


def _normalize(lines: list[str]) -> list[str]:
    return [line.strip().lower() for line in lines if line.strip()]


def _missing_prefixes(section_lines: list[str], required_prefixes: tuple[str, ...]) -> list[str]:
    normalized = _normalize(section_lines)
    missing: list[str] = []
    for prefix in required_prefixes:
        if not any(line.startswith(prefix) for line in normalized):
            missing.append(prefix)
    return missing


def _extract_prefixed_value(section_lines: list[str], prefix: str) -> str | None:
    normalized_prefix = prefix.strip().lower()
    for raw in section_lines:
        normalized = raw.strip().lower()
        if normalized.startswith(normalized_prefix):
            return raw.strip()[len(prefix) :].strip()
    return None


def _bullet_values(section_lines: list[str]) -> list[str]:
    values: list[str] = []
    for raw in section_lines:
        stripped = raw.strip()
        if stripped.startswith("- "):
            values.append(stripped[2:].strip())
    return values


def _slug_from_execution_contract(path: Path) -> str | None:
    suffix = ".execution-contract.md"
    if not path.name.endswith(suffix):
        return None
    return path.name[: -len(suffix)]


def _slug_from_phase_brief(path: Path) -> str | None:
    marker = ".phase-"
    if marker not in path.name:
        return None
    return path.name.split(marker, 1)[0]


def _phase_name_short(path: Path) -> str | None:
    lines = path.read_text(encoding="utf-8").splitlines()
    phase_section = _section_lines(lines, "## Phase")
    phase_name = _extract_prefixed_value(phase_section, "- name:")
    if not phase_name:
        return None
    return phase_name.split(" -", 1)[0].strip()


def _phase_artifacts_for_contract(path: Path) -> list[Path]:
    slug = _slug_from_execution_contract(path)
    if not slug:
        return []
    modules_root = path.resolve().parents[1] / "modules"
    return sorted(modules_root.glob(f"{slug}.phase-*.md"))


def _execution_contract_for_phase(path: Path) -> Path | None:
    slug = _slug_from_phase_brief(path)
    if not slug:
        return None
    contracts_root = path.resolve().parents[1] / "contracts"
    candidate = contracts_root / f"{slug}.execution-contract.md"
    if candidate.exists():
        return candidate
    return None


def _repo_root_for_planning_path(path: Path) -> Path:
    return path.resolve().parents[3]


def _resolve_declared_path(base_path: Path, raw: str | None) -> Path | None:
    if not raw:
        return None
    candidate = Path(raw.strip())
    if candidate.is_absolute():
        return candidate.resolve()
    return (_repo_root_for_planning_path(base_path) / candidate).resolve()


def _relevant_planning_files(repo_root: Path, changed_files: list[str], validate_all: bool) -> list[Path]:
    candidates: list[Path] = []
    if validate_all:
        candidates.extend(sorted((repo_root / "docs" / "codex" / "contracts").glob("*.execution-contract.md")))
        candidates.extend(sorted((repo_root / "docs" / "codex" / "modules").glob("*.phase-*.md")))
        return [path.resolve() for path in candidates]

    for rel_path in changed_files:
        if not (
            rel_path.startswith("docs/codex/contracts/")
            or rel_path.startswith("docs/codex/modules/")
        ):
            continue
        if not (rel_path.endswith(".execution-contract.md") or ".phase-" in rel_path):
            continue
        path = (repo_root / rel_path).resolve()
        if path.exists() and path not in candidates:
            candidates.append(path)
    return candidates


def _validate_execution_contract(path: Path, errors: list[str]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    section = _section_lines(lines, EXECUTION_CONTRACT_HEADING)
    contours_section = _section_lines(lines, MANDATORY_REAL_CONTOURS_HEADING)
    matrix_section = _section_lines(lines, RELEASE_SURFACE_MATRIX_HEADING)
    if not section:
        errors.append(f"{path.as_posix()}: missing section `{EXECUTION_CONTRACT_HEADING}`")
        return
    for prefix in _missing_prefixes(section, EXECUTION_CONTRACT_PREFIXES):
        errors.append(f"{path.as_posix()}: missing execution-contract item `{prefix}`")

    target_decision = (_extract_prefixed_value(section, "- target decision:") or "").strip().lower()
    proof_class = (_extract_prefixed_value(section, "- release-ready proof class:") or "").strip().lower()
    if target_decision == "allow_release_readiness" and proof_class != "live-real":
        errors.append(
            f"{path.as_posix()}: `ALLOW_RELEASE_READINESS` target must use `live-real` release-ready proof class"
        )
    if not contours_section:
        errors.append(f"{path.as_posix()}: missing section `{MANDATORY_REAL_CONTOURS_HEADING}`")
    if not matrix_section:
        errors.append(f"{path.as_posix()}: missing section `{RELEASE_SURFACE_MATRIX_HEADING}`")


def _parse_contour_map(path: Path, errors: list[str]) -> dict[str, str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    contours_section = _section_lines(lines, MANDATORY_REAL_CONTOURS_HEADING)
    contours: dict[str, str] = {}
    for raw in _bullet_values(contours_section):
        if ":" not in raw:
            errors.append(f"{path.as_posix()}: invalid mandatory real contour row `{raw}`")
            continue
        key, value = raw.split(":", 1)
        contour_id = key.strip().lower()
        contour_desc = value.strip()
        if not contour_id or not contour_desc:
            errors.append(f"{path.as_posix()}: invalid mandatory real contour row `{raw}`")
            continue
        contours[contour_id] = contour_desc
    if not contours:
        errors.append(f"{path.as_posix()}: `{MANDATORY_REAL_CONTOURS_HEADING}` must contain at least one bullet")
    return contours


def _parse_surface_matrix(path: Path, errors: list[str]) -> dict[str, dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    matrix_section = _section_lines(lines, RELEASE_SURFACE_MATRIX_HEADING)
    surfaces: dict[str, dict[str, str]] = {}
    for raw in _bullet_values(matrix_section):
        parts = [item.strip() for item in raw.split("|")]
        row: dict[str, str] = {}
        for part in parts:
            if ":" not in part:
                continue
            key, value = part.split(":", 1)
            row[key.strip().lower()] = value.strip()
        surface_id = row.get("surface", "").strip().lower()
        if not surface_id:
            errors.append(f"{path.as_posix()}: invalid release surface row `{raw}`")
            continue
        if surface_id in surfaces:
            errors.append(f"{path.as_posix()}: duplicate release surface `{surface_id}`")
            continue
        surfaces[surface_id] = row
    if not surfaces:
        errors.append(f"{path.as_posix()}: `{RELEASE_SURFACE_MATRIX_HEADING}` must contain at least one bullet")
    return surfaces


def _validate_phase_brief(path: Path, errors: list[str]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    impact_section = _section_lines(lines, PHASE_IMPACT_HEADING)
    ownership_section = _section_lines(lines, PHASE_OWNERSHIP_HEADING)
    limits_section = _section_lines(lines, PHASE_LIMITS_HEADING)

    if not impact_section:
        errors.append(f"{path.as_posix()}: missing section `{PHASE_IMPACT_HEADING}`")
        return
    if not ownership_section:
        errors.append(f"{path.as_posix()}: missing section `{PHASE_OWNERSHIP_HEADING}`")
        return
    if not limits_section:
        errors.append(f"{path.as_posix()}: missing section `{PHASE_LIMITS_HEADING}`")
        return

    for prefix in _missing_prefixes(impact_section, PHASE_IMPACT_PREFIXES):
        errors.append(f"{path.as_posix()}: missing phase-planning item `{prefix}`")
    for prefix in _missing_prefixes(ownership_section, PHASE_OWNERSHIP_PREFIXES):
        errors.append(f"{path.as_posix()}: missing phase-ownership item `{prefix}`")

    minimum_proof_class = (_extract_prefixed_value(impact_section, "- minimum proof class:") or "").strip().lower()
    accepted_state_label = (_extract_prefixed_value(impact_section, "- accepted state label:") or "").strip().lower()

    if minimum_proof_class and minimum_proof_class not in ALLOWED_PROOF_CLASSES:
        errors.append(
            f"{path.as_posix()}: invalid minimum proof class `{minimum_proof_class}` "
            f"(allowed: {', '.join(sorted(ALLOWED_PROOF_CLASSES))})"
        )
    if accepted_state_label and accepted_state_label not in ALLOWED_ACCEPTED_STATE_LABELS:
        errors.append(
            f"{path.as_posix()}: invalid accepted state label `{accepted_state_label}` "
            f"(allowed: {', '.join(sorted(ALLOWED_ACCEPTED_STATE_LABELS))})"
        )
    if accepted_state_label == "release_decision" and minimum_proof_class and minimum_proof_class != "live-real":
        errors.append(f"{path.as_posix()}: `release_decision` phases must require `live-real` proof")

    normalized_limits = _normalize(limits_section)
    bullet_lines = [line for line in normalized_limits if line.startswith("- ")]
    if not bullet_lines:
        errors.append(f"{path.as_posix()}: `{PHASE_LIMITS_HEADING}` must contain at least one bullet")
    if accepted_state_label != "release_decision" and not any("release readiness" in line for line in bullet_lines):
        errors.append(
            f"{path.as_posix()}: non-release phases must say explicitly in `{PHASE_LIMITS_HEADING}` "
            "that they do not prove release readiness"
        )


def _parse_phase_ownership(path: Path, errors: list[str]) -> dict[str, object]:
    lines = path.read_text(encoding="utf-8").splitlines()
    impact_section = _section_lines(lines, PHASE_IMPACT_HEADING)
    ownership_section = _section_lines(lines, PHASE_OWNERSHIP_HEADING)
    minimum_proof_class = (_extract_prefixed_value(impact_section, "- minimum proof class:") or "").strip().lower()
    accepted_state_label = (_extract_prefixed_value(impact_section, "- accepted state label:") or "").strip().lower()
    owned_surfaces_raw = (_extract_prefixed_value(ownership_section, "- owned surfaces:") or "").strip()
    delivered_proof_class = (_extract_prefixed_value(ownership_section, "- delivered proof class:") or "").strip().lower()
    real_bindings = (_extract_prefixed_value(ownership_section, "- required real bindings:") or "").strip().lower()
    downgrade_forbidden = (_extract_prefixed_value(ownership_section, "- target downgrade is forbidden:") or "").strip().lower()
    owned_surfaces = [
        item.strip().lower()
        for item in owned_surfaces_raw.split(",")
        if item.strip()
    ]
    return {
        "phase_id": (_phase_name_short(path) or "").strip().lower(),
        "path": path,
        "minimum_proof_class": minimum_proof_class,
        "accepted_state_label": accepted_state_label,
        "owned_surfaces": owned_surfaces,
        "delivered_proof_class": delivered_proof_class,
        "required_real_bindings": real_bindings,
        "target_downgrade_forbidden": downgrade_forbidden,
    }


def _normalize_text(value: str) -> str:
    cleaned = (
        value.lower()
        .replace("`", "")
        .replace("'", "")
        .replace('"', "")
        .replace("(", " ")
        .replace(")", " ")
        .replace(",", " ")
        .replace(":", " ")
        .replace(";", " ")
        .replace("-", " ")
    )
    return " ".join(cleaned.split())


def _matching_entry_present(expected: str, actual_items: list[str]) -> bool:
    expected_normalized = _normalize_text(expected)
    if not expected_normalized:
        return True
    for item in actual_items:
        actual_normalized = _normalize_text(item)
        if not actual_normalized:
            continue
        if expected_normalized in actual_normalized or actual_normalized in expected_normalized:
            return True
    return False


def _primary_source_document(execution_contract: Path) -> Path | None:
    lines = execution_contract.read_text(encoding="utf-8").splitlines()
    section = _section_lines(lines, "## Primary Source Decision")
    raw_path = _extract_prefixed_value(section, "- selected primary document:")
    return _resolve_declared_path(execution_contract, raw_path)


def _phase_section_entries(path: Path, heading: str) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return _bullet_values(_section_lines(lines, heading))


def _phase_objective_entry(path: Path) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    objective_section = _section_lines(lines, "## Objective")
    values = _bullet_values(objective_section)
    return values[0] if values else ""


def _cross_validate_compiled_source_plan(
    execution_contract: Path,
    compiled_plan: CompiledPhasePlan,
    errors: list[str],
) -> None:
    if not compiled_plan.phases:
        return
    phase_files = _phase_artifacts_for_contract(execution_contract)
    if len(phase_files) != len(compiled_plan.phases):
        errors.append(
            f"{execution_contract.as_posix()}: source TZ declares {len(compiled_plan.phases)} phases "
            f"but execution contract materializes {len(phase_files)} phase briefs"
        )
        return

    for phase_path, compiled_phase in zip(phase_files, compiled_plan.phases):
        phase_id = (_phase_name_short(phase_path) or "").strip()
        if phase_id.lower() != compiled_phase.phase_id.lower():
            errors.append(
                f"{phase_path.as_posix()}: phase id `{phase_id}` does not preserve source phase id "
                f"`{compiled_phase.phase_id}`"
            )
        objective = _phase_objective_entry(phase_path)
        if compiled_phase.objective and not _matching_entry_present(compiled_phase.objective, [objective]):
            errors.append(
                f"{phase_path.as_posix()}: phase objective does not preserve source objective "
                f"`{compiled_phase.objective}`"
            )

        acceptance_entries = _phase_section_entries(phase_path, "## Acceptance Gate")
        for expected in compiled_phase.acceptance_gate:
            if not _matching_entry_present(expected, acceptance_entries):
                errors.append(
                    f"{phase_path.as_posix()}: acceptance gate dropped or rewrote source clause `{expected}`"
                )

        disprover_entries = _phase_section_entries(phase_path, "## Disprover")
        for expected in compiled_phase.disprover:
            if not _matching_entry_present(expected, disprover_entries):
                errors.append(
                    f"{phase_path.as_posix()}: disprover dropped or rewrote source clause `{expected}`"
                )


def _proof_rank(value: str) -> int:
    order = ["doc", "schema", "unit", "integration", "staging-real", "live-real"]
    try:
        return order.index(value)
    except ValueError:
        return -1


def _cross_validate_release_plan(execution_contract: Path, errors: list[str]) -> None:
    contours = _parse_contour_map(execution_contract, errors)
    matrix = _parse_surface_matrix(execution_contract, errors)
    phase_files = _phase_artifacts_for_contract(execution_contract)
    if not phase_files:
        errors.append(f"{execution_contract.as_posix()}: no phase briefs found for execution contract")
        return
    phase_rows = [_parse_phase_ownership(path, errors) for path in phase_files]
    phase_by_id = {
        str(row["phase_id"]): row
        for row in phase_rows
        if str(row["phase_id"])
    }

    owner_seen: dict[str, str] = {}
    for surface_id, row in matrix.items():
        owner_phase = str(row.get("owner phase", "")).strip().lower()
        required_proof_class = str(row.get("required proof class", "")).strip().lower()
        if owner_phase in owner_seen.values():
            pass
        if owner_phase not in phase_by_id:
            errors.append(
                f"{execution_contract.as_posix()}: surface `{surface_id}` points to unknown owner phase `{owner_phase}`"
            )
            continue
        phase_row = phase_by_id[owner_phase]
        if surface_id not in phase_row["owned_surfaces"]:
            errors.append(
                f"{phase_row['path'].as_posix()}: phase does not declare owned surface `{surface_id}` from the matrix"
            )
        if required_proof_class not in ALLOWED_PROOF_CLASSES:
            errors.append(
                f"{execution_contract.as_posix()}: surface `{surface_id}` uses invalid required proof class `{required_proof_class}`"
            )
        if _proof_rank(str(phase_row["delivered_proof_class"])) < _proof_rank(required_proof_class):
            errors.append(
                f"{phase_row['path'].as_posix()}: delivered proof class `{phase_row['delivered_proof_class']}` "
                f"is weaker than matrix requirement `{required_proof_class}` for `{surface_id}`"
            )
        if required_proof_class in REAL_PROOF_CLASSES and phase_row["accepted_state_label"] == "prep_closed":
            errors.append(
                f"{phase_row['path'].as_posix()}: real contour `{surface_id}` cannot be owned by a `prep_closed` phase"
            )
        if required_proof_class in REAL_PROOF_CLASSES and phase_row["required_real_bindings"] in {"", "none"}:
            errors.append(
                f"{phase_row['path'].as_posix()}: real contour `{surface_id}` must declare explicit real bindings"
            )
        owner_seen[surface_id] = owner_phase

    for contour_id in contours:
        if contour_id not in matrix:
            errors.append(
                f"{execution_contract.as_posix()}: mandatory real contour `{contour_id}` is missing from the release surface matrix"
            )
            continue
        row = matrix[contour_id]
        required_proof_class = str(row.get("required proof class", "")).strip().lower()
        owner_phase = str(row.get("owner phase", "")).strip().lower()
        phase_row = phase_by_id.get(owner_phase)
        if required_proof_class not in REAL_PROOF_CLASSES:
            errors.append(
                f"{execution_contract.as_posix()}: mandatory real contour `{contour_id}` must require `staging-real` or `live-real` proof"
            )
        if phase_row and phase_row["accepted_state_label"] == "prep_closed":
            errors.append(
                f"{phase_row['path'].as_posix()}: mandatory real contour `{contour_id}` cannot terminate as `prep_closed`"
            )

    for row in phase_rows:
        for surface_id in row["owned_surfaces"]:
            if surface_id not in matrix:
                errors.append(
                    f"{row['path'].as_posix()}: phase claims unknown owned surface `{surface_id}`"
                )

    primary_source = _primary_source_document(execution_contract)
    if primary_source is None:
        return
    if not primary_source.exists():
        errors.append(
            f"{execution_contract.as_posix()}: selected primary document does not exist: {primary_source.as_posix()}"
        )
        return
    compiled_plan = compile_phase_plan(primary_source)
    _cross_validate_compiled_source_plan(execution_contract, compiled_plan, errors)


def run(
    repo_root: Path,
    *,
    base_sha: str | None = None,
    head_sha: str | None = None,
    changed_files_override: list[str] | None = None,
    validate_all: bool = False,
) -> int:
    changed_files = _collect_changed_files(
        base_sha=base_sha,
        head_sha=head_sha,
        changed_files_override=changed_files_override,
    )
    planning_files = _relevant_planning_files(repo_root, changed_files, validate_all)

    if not planning_files:
        print("phase planning contract validation: OK (no planning artifacts changed)")
        return 0

    errors: list[str] = []
    execution_contracts: list[Path] = []
    for path in planning_files:
        if path.name.endswith(".execution-contract.md"):
            _validate_execution_contract(path, errors)
            execution_contracts.append(path)
        elif ".phase-" in path.name:
            _validate_phase_brief(path, errors)
            execution_contract = _execution_contract_for_phase(path)
            if execution_contract and execution_contract not in execution_contracts:
                execution_contracts.append(execution_contract)

    for execution_contract in execution_contracts:
        _cross_validate_release_plan(execution_contract, errors)

    if errors:
        print("phase planning contract validation failed:")
        for item in errors:
            print(f"- {item}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    print(
        "phase planning contract validation: OK "
        f"(planning_files={len(planning_files)})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate planning-gate contract for execution contracts and phase briefs."
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--base-sha", default=None)
    parser.add_argument("--head-sha", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    changed_files_override: list[str] | None = None
    if args.base_sha and args.head_sha:
        changed_files_override = None
    elif args.stdin or args.changed_files:
        stdin_items = [line.strip() for line in sys.stdin.read().splitlines() if line.strip()] if args.stdin else []
        changed_files_override = [*list(args.changed_files), *stdin_items]

    sys.exit(
        run(
            Path(args.repo_root).resolve(),
            base_sha=args.base_sha,
            head_sha=args.head_sha,
            changed_files_override=changed_files_override,
            validate_all=bool(args.all),
        )
    )


if __name__ == "__main__":
    main()
