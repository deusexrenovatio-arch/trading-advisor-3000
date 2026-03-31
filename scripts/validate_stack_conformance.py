from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import yaml


REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"
ALLOWED_CLAIMS = {"implemented", "partial", "planned", "not accepted", "removed"}
REQUIREMENT_NAME_RE = re.compile(r"^\s*([A-Za-z0-9_.-]+)")


def _normalize_claim(value: Any) -> str:
    return str(value or "").strip().lower()


def _is_replaced_by_claim(claim: str) -> bool:
    if not claim.startswith("replaced_by:"):
        return False
    replacement_target = claim.split(":", 1)[1].strip()
    return bool(replacement_target)


def _is_supported_technology_claim(claim: str) -> bool:
    return claim in ALLOWED_CLAIMS or _is_replaced_by_claim(claim)


def _resolve(repo_root: Path, path_text: str) -> Path:
    candidate = Path(path_text)
    if candidate.is_absolute():
        return candidate
    return (repo_root / candidate).resolve()


def _load_yaml(path: Path, errors: list[str]) -> dict[str, Any]:
    if not path.exists():
        errors.append(f"missing registry file: {path.as_posix()}")
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # pragma: no cover - defensive path
        errors.append(f"failed to parse registry YAML `{path.as_posix()}`: {exc}")
        return {}
    if not isinstance(payload, dict):
        errors.append(f"registry must be a YAML object: {path.as_posix()}")
        return {}
    return payload


def _read_text(repo_root: Path, path_text: str, errors: list[str], *, context: str) -> str:
    path = _resolve(repo_root, path_text)
    if not path.exists():
        errors.append(f"missing {context}: {path_text}")
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except Exception as exc:  # pragma: no cover - defensive path
        errors.append(f"failed to read {context} `{path_text}`: {exc}")
        return ""


def _string_list(value: Any, *, context: str, errors: list[str]) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{context} must be a list")
        return []
    out: list[str] = []
    for item in value:
        rendered = str(item).strip()
        if rendered:
            out.append(rendered)
    return out


def _has_glob_match(repo_root: Path, patterns: list[str]) -> bool:
    for pattern in patterns:
        if any(repo_root.glob(pattern)):
            return True
    return False


def _expand_glob_paths(
    repo_root: Path,
    patterns: list[str],
    *,
    context: str,
    errors: list[str],
) -> list[str]:
    expanded: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        matches = sorted(path for path in repo_root.glob(pattern) if path.is_file())
        if not matches:
            errors.append(f"{context} pattern matched no files: {pattern}")
            continue
        for path in matches:
            rendered = path.resolve().relative_to(repo_root).as_posix()
            if rendered in seen:
                continue
            seen.add(rendered)
            expanded.append(rendered)
    return expanded


def _parse_dependencies(repo_root: Path, pyproject_path: str, errors: list[str]) -> set[str]:
    path = _resolve(repo_root, pyproject_path)
    if not path.exists():
        errors.append(f"missing pyproject for dependency evidence: {pyproject_path}")
        return set()
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive path
        errors.append(f"failed to parse pyproject `{pyproject_path}`: {exc}")
        return set()

    project = payload.get("project") or {}
    if not isinstance(project, dict):
        errors.append("pyproject `[project]` must be an object")
        return set()

    raw_deps: list[str] = []
    dependencies = project.get("dependencies") or []
    if isinstance(dependencies, list):
        raw_deps.extend(str(item) for item in dependencies)
    else:
        errors.append("pyproject `[project].dependencies` must be a list")

    optional = project.get("optional-dependencies") or {}
    if isinstance(optional, dict):
        for group_value in optional.values():
            if isinstance(group_value, list):
                raw_deps.extend(str(item) for item in group_value)
    else:
        errors.append("pyproject `[project].optional-dependencies` must be an object")

    names: set[str] = set()
    for item in raw_deps:
        head = item.split(";", 1)[0].strip()
        match = REQUIREMENT_NAME_RE.match(head)
        if match:
            names.add(match.group(1).lower())
    return names


def _parse_status_table(status_markdown: str) -> dict[str, str]:
    rows: dict[str, str] = {}
    for raw in status_markdown.splitlines():
        line = raw.strip()
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 2:
            continue
        if all(not cell or set(cell) <= {"-", " "} for cell in cells):
            continue
        if cells[0].lower() == "surface" and cells[1].lower() == "status":
            continue
        surface = cells[0]
        status = cells[1].lower()
        if surface:
            rows[surface] = status
    return rows


def _line_mentions_alias(line_lower: str, alias_lower: str) -> bool:
    if not alias_lower:
        return False
    if alias_lower.isalnum():
        return re.search(rf"\b{re.escape(alias_lower)}\b", line_lower) is not None
    return alias_lower in line_lower


def _validate_runtime_proof(
    *,
    subject: str,
    claim: str,
    proof_payload: Any,
    repo_root: Path,
    dependencies: set[str],
    errors: list[str],
) -> None:
    proof = proof_payload if isinstance(proof_payload, dict) else {}
    files_all = _string_list(proof.get("files_all"), context=f"{subject}.runtime_proof.files_all", errors=errors)
    files_any = _string_list(proof.get("files_any"), context=f"{subject}.runtime_proof.files_any", errors=errors)
    entrypoints_any = _string_list(
        proof.get("entrypoints_any"),
        context=f"{subject}.runtime_proof.entrypoints_any",
        errors=errors,
    )
    tests_any = _string_list(proof.get("tests_any"), context=f"{subject}.runtime_proof.tests_any", errors=errors)
    dependencies_any = _string_list(
        proof.get("dependencies_any"),
        context=f"{subject}.runtime_proof.dependencies_any",
        errors=errors,
    )

    has_proof_contract = any((files_all, files_any, entrypoints_any, tests_any, dependencies_any))
    if claim == "implemented" and not has_proof_contract:
        errors.append(f"{subject} is marked `implemented` but runtime_proof is empty")
        return
    if not has_proof_contract:
        return

    missing_all = [path for path in files_all if not _resolve(repo_root, path).exists()]
    if missing_all:
        errors.append(f"{subject} runtime proof missing required files: {', '.join(missing_all)}")

    if files_any and not any(_resolve(repo_root, path).exists() for path in files_any):
        errors.append(f"{subject} runtime proof missing files_any match: {', '.join(files_any)}")

    if entrypoints_any and not any(_resolve(repo_root, path).exists() for path in entrypoints_any):
        errors.append(f"{subject} runtime proof missing entrypoint evidence: {', '.join(entrypoints_any)}")

    if tests_any and not _has_glob_match(repo_root, tests_any):
        errors.append(f"{subject} runtime proof missing test evidence: {', '.join(tests_any)}")

    if dependencies_any:
        expected = {item.lower() for item in dependencies_any}
        if not expected.intersection(dependencies):
            errors.append(
                f"{subject} runtime proof missing dependency evidence: "
                f"expected any of {', '.join(sorted(expected))}"
            )


def _validate_surface_claims(
    *,
    registry: dict[str, Any],
    status_rows: dict[str, str],
    repo_root: Path,
    dependencies: set[str],
    errors: list[str],
) -> list[str]:
    non_implemented_labels: list[str] = []
    entries = registry.get("surface_claims") or []
    if not isinstance(entries, list):
        errors.append("surface_claims must be a list")
        return non_implemented_labels

    for index, payload in enumerate(entries, start=1):
        if not isinstance(payload, dict):
            errors.append(f"surface_claims[{index}] must be an object")
            continue
        label = str(payload.get("status_label") or "").strip()
        claim = _normalize_claim(payload.get("claim"))
        surface_id = str(payload.get("id") or f"surface-{index}").strip()
        subject = f"surface `{surface_id}`"

        if not label:
            errors.append(f"{subject} is missing status_label")
            continue
        if claim not in ALLOWED_CLAIMS:
            errors.append(f"{subject} has unsupported claim `{claim}`")
            continue

        status_claim = status_rows.get(label)
        if status_claim is None:
            errors.append(f"{subject} references unknown STATUS row `{label}`")
        elif status_claim != claim:
            errors.append(
                f"{subject} claim drift for `{label}`: registry=`{claim}` "
                f"status_doc=`{status_claim}`"
            )

        if claim != "implemented":
            non_implemented_labels.append(label)

        if claim == "implemented" or payload.get("runtime_proof") is not None:
            _validate_runtime_proof(
                subject=subject,
                claim=claim,
                proof_payload=payload.get("runtime_proof"),
                repo_root=repo_root,
                dependencies=dependencies,
                errors=errors,
            )

    return non_implemented_labels


def _validate_closure_claim_guard(
    *,
    registry: dict[str, Any],
    repo_root: Path,
    non_implemented_labels: list[str],
    errors: list[str],
) -> None:
    guard = registry.get("closure_claim_guard") or {}
    if not isinstance(guard, dict):
        errors.append("closure_claim_guard must be an object")
        return
    documents = _string_list(guard.get("documents"), context="closure_claim_guard.documents", errors=errors)
    document_globs = _string_list(
        guard.get("document_globs"),
        context="closure_claim_guard.document_globs",
        errors=errors,
    )
    forbidden_terms = _string_list(
        guard.get("forbidden_terms"),
        context="closure_claim_guard.forbidden_terms",
        errors=errors,
    )
    if not non_implemented_labels or not forbidden_terms:
        return
    if not documents and not document_globs:
        errors.append("closure_claim_guard must define documents or document_globs")
        return

    expanded_documents = _expand_glob_paths(
        repo_root,
        document_globs,
        context="closure_claim_guard.document_globs",
        errors=errors,
    )
    coverage_documents: list[str] = []
    for document in [*documents, *expanded_documents]:
        if document not in coverage_documents:
            coverage_documents.append(document)
    if not coverage_documents:
        errors.append("closure_claim_guard resolved no documents for closure-claim scanning")
        return

    remaining = ", ".join(non_implemented_labels)
    for document in coverage_documents:
        text = _read_text(repo_root, document, errors, context="closure-guard document")
        if not text:
            continue
        lines = text.splitlines()
        for term in forbidden_terms:
            token = term.lower()
            for line_no, raw in enumerate(lines, start=1):
                if token in raw.lower():
                    errors.append(
                        f"forbidden closure term `{term}` in {document}:{line_no} "
                        f"while non-implemented surfaces remain: {remaining}"
                    )


def _validate_removed_claim_guard(
    *,
    registry: dict[str, Any],
    repo_root: Path,
    errors: list[str],
) -> None:
    guard = registry.get("removed_claim_guard") or {}
    if not guard:
        return
    if not isinstance(guard, dict):
        errors.append("removed_claim_guard must be an object")
        return

    documents = _string_list(guard.get("documents"), context="removed_claim_guard.documents", errors=errors)
    document_globs = _string_list(
        guard.get("document_globs"),
        context="removed_claim_guard.document_globs",
        errors=errors,
    )
    phrase_markers = _string_list(
        guard.get("phrase_markers_any"),
        context="removed_claim_guard.phrase_markers_any",
        errors=errors,
    )
    contradiction_markers = _string_list(
        guard.get("contradiction_markers_any"),
        context="removed_claim_guard.contradiction_markers_any",
        errors=errors,
    )
    skip_markers = _string_list(
        guard.get("skip_markers_any"),
        context="removed_claim_guard.skip_markers_any",
        errors=errors,
    )
    if not phrase_markers and not contradiction_markers:
        return
    if not documents and not document_globs:
        errors.append("removed_claim_guard must define documents or document_globs")
        return

    expanded_documents = _expand_glob_paths(
        repo_root,
        document_globs,
        context="removed_claim_guard.document_globs",
        errors=errors,
    )
    claim_documents: list[str] = []
    for document in [*documents, *expanded_documents]:
        if document not in claim_documents:
            claim_documents.append(document)
    if not claim_documents:
        errors.append("removed_claim_guard resolved no documents for ADR-removal scanning")
        return

    raw_technologies = registry.get("technology_claims") or []
    if not isinstance(raw_technologies, list):
        errors.append("technology_claims must be a list")
        return

    technologies: list[dict[str, Any]] = []
    for index, payload in enumerate(raw_technologies, start=1):
        if not isinstance(payload, dict):
            errors.append(f"technology_claims[{index}] must be an object")
            continue
        claim = _normalize_claim(payload.get("claim"))
        if not _is_supported_technology_claim(claim):
            continue
        tech_id = str(payload.get("id") or f"technology-{index}").strip()
        display_name = str(payload.get("display_name") or tech_id).strip()
        aliases = {
            tech_id.lower(),
            display_name.lower(),
        }
        aliases = {alias for alias in aliases if alias}
        if not aliases:
            continue
        technologies.append(
            {
                "subject": f"technology `{display_name}`",
                "claim": claim,
                "aliases": sorted(aliases),
            }
        )

    if not technologies:
        return

    phrase_tokens = [item.lower() for item in phrase_markers]
    contradiction_tokens = [item.lower() for item in contradiction_markers]
    skip_tokens = [item.lower() for item in skip_markers]
    for document in claim_documents:
        text = _read_text(repo_root, document, errors, context="removed-claim document")
        if not text:
            continue
        for line_no, raw in enumerate(text.splitlines(), start=1):
            lowered = raw.lower()
            if skip_tokens and any(token in lowered for token in skip_tokens):
                continue

            has_removal_wording = phrase_tokens and any(token in lowered for token in phrase_tokens)
            has_terminal_contradiction = (
                contradiction_tokens and any(token in lowered for token in contradiction_tokens)
            )
            if not has_removal_wording and not has_terminal_contradiction:
                continue

            matched = [
                technology
                for technology in technologies
                if any(_line_mentions_alias(lowered, alias) for alias in technology["aliases"])
            ]
            if has_removal_wording and not matched:
                errors.append(
                    f"ADR-removal wording appears in {document}:{line_no} "
                    "without a recognized technology marker"
                )
                continue

            for technology in matched:
                claim = technology["claim"]
                has_terminal_claim = claim == "removed" or _is_replaced_by_claim(claim)
                if has_removal_wording and not has_terminal_claim:
                    errors.append(
                        f"{technology['subject']} has claim `{claim}` but ADR-removal wording "
                        f"appears in {document}:{line_no}"
                    )
                if has_terminal_contradiction and has_terminal_claim:
                    errors.append(
                        f"{technology['subject']} has terminal claim `{claim}` but non-terminal wording "
                        f"appears in {document}:{line_no}"
                    )


def _validate_removed_technology_active_guard(
    *,
    registry: dict[str, Any],
    repo_root: Path,
    errors: list[str],
) -> None:
    guard = registry.get("removed_technology_active_guard") or {}
    if not guard:
        return
    if not isinstance(guard, dict):
        errors.append("removed_technology_active_guard must be an object")
        return

    documents = _string_list(
        guard.get("documents"),
        context="removed_technology_active_guard.documents",
        errors=errors,
    )
    document_globs = _string_list(
        guard.get("document_globs"),
        context="removed_technology_active_guard.document_globs",
        errors=errors,
    )
    neutral_markers = _string_list(
        guard.get("neutral_markers_any"),
        context="removed_technology_active_guard.neutral_markers_any",
        errors=errors,
    )
    if not documents and not document_globs:
        errors.append("removed_technology_active_guard must define documents or document_globs")
        return

    expanded_documents = _expand_glob_paths(
        repo_root,
        document_globs,
        context="removed_technology_active_guard.document_globs",
        errors=errors,
    )
    guard_documents: list[str] = []
    for document in [*documents, *expanded_documents]:
        if document not in guard_documents:
            guard_documents.append(document)
    if not guard_documents:
        errors.append("removed_technology_active_guard resolved no documents for scanning")
        return

    entries = registry.get("technology_claims") or []
    if not isinstance(entries, list):
        errors.append("technology_claims must be a list")
        return

    removed_technologies: list[dict[str, Any]] = []
    for index, payload in enumerate(entries, start=1):
        if not isinstance(payload, dict):
            continue
        claim = _normalize_claim(payload.get("claim"))
        if claim != "removed" and not _is_replaced_by_claim(claim):
            continue
        tech_id = str(payload.get("id") or f"technology-{index}").strip()
        display_name = str(payload.get("display_name") or tech_id).strip()
        extra_aliases = _string_list(
            payload.get("active_aliases_any"),
            context=f"technology `{display_name}`.active_aliases_any",
            errors=errors,
        )
        aliases = {
            tech_id.lower(),
            display_name.lower(),
            *(alias.lower() for alias in extra_aliases),
        }
        aliases = {alias for alias in aliases if alias}
        if not aliases:
            continue
        removed_technologies.append(
            {
                "subject": f"technology `{display_name}`",
                "claim": claim,
                "aliases": sorted(aliases),
            }
        )
    if not removed_technologies:
        return

    neutral_tokens = [item.lower() for item in neutral_markers]
    for document in guard_documents:
        text = _read_text(repo_root, document, errors, context="removed-technology-active-guard document")
        if not text:
            continue
        for line_no, raw in enumerate(text.splitlines(), start=1):
            lowered = raw.lower()
            if neutral_tokens and any(token in lowered for token in neutral_tokens):
                continue
            for technology in removed_technologies:
                if not any(_line_mentions_alias(lowered, alias) for alias in technology["aliases"]):
                    continue
                errors.append(
                    f"{technology['subject']} has claim `{technology['claim']}` but appears without terminal marker "
                    f"in {document}:{line_no}"
                )


def _validate_removed_replacement(
    *,
    subject: str,
    claim: str,
    replacement_payload: Any,
    repo_root: Path,
    errors: list[str],
) -> None:
    if claim != "removed" and not _is_replaced_by_claim(claim):
        return
    replacement = replacement_payload if isinstance(replacement_payload, dict) else {}
    adr_required = bool(replacement.get("adr_required_when_removed", False))
    if not adr_required:
        return

    adr_paths = _string_list(replacement.get("adr_paths_any"), context=f"{subject}.replacement.adr_paths_any", errors=errors)
    if not adr_paths:
        errors.append(f"{subject} has claim `{claim}` but replacement ADR paths are missing")
        return

    existing_paths = [path for path in adr_paths if _resolve(repo_root, path).exists()]
    if not existing_paths:
        errors.append(f"{subject} has claim `{claim}` but no replacement ADR file exists: {', '.join(adr_paths)}")
        return

    markers = _string_list(
        replacement.get("adr_markers_any"),
        context=f"{subject}.replacement.adr_markers_any",
        errors=errors,
    )
    if not markers:
        return

    found_marker = False
    for path_text in existing_paths:
        text = _read_text(repo_root, path_text, errors, context="replacement ADR")
        if not text:
            continue
        lowered = text.lower()
        if any(marker.lower() in lowered for marker in markers):
            found_marker = True
            break
    if not found_marker:
        errors.append(
            f"{subject} has claim `{claim}` but replacement ADR markers are missing: "
            f"{', '.join(markers)}"
        )


def _validate_terminal_technology_guard(
    *,
    registry: dict[str, Any],
    errors: list[str],
) -> None:
    guard = registry.get("terminal_technology_guard") or {}
    if not guard:
        return
    if not isinstance(guard, dict):
        errors.append("terminal_technology_guard must be an object")
        return

    technology_ids = _string_list(
        guard.get("technology_ids"),
        context="terminal_technology_guard.technology_ids",
        errors=errors,
    )
    if not technology_ids:
        errors.append("terminal_technology_guard must define technology_ids")
        return

    terminal_claims = _string_list(
        guard.get("terminal_claims"),
        context="terminal_technology_guard.terminal_claims",
        errors=errors,
    )
    allowed_terminal_claims = {item.lower() for item in terminal_claims}
    if not allowed_terminal_claims:
        allowed_terminal_claims = {"implemented", "removed"}

    entries = registry.get("technology_claims") or []
    if not isinstance(entries, list):
        errors.append("technology_claims must be a list")
        return

    claims_by_id: dict[str, dict[str, str]] = {}
    for index, payload in enumerate(entries, start=1):
        if not isinstance(payload, dict):
            continue
        tech_id = str(payload.get("id") or f"technology-{index}").strip().lower()
        if not tech_id:
            continue
        display_name = str(payload.get("display_name") or payload.get("id") or f"technology-{index}").strip()
        claims_by_id[tech_id] = {
            "claim": _normalize_claim(payload.get("claim")),
            "subject": f"technology `{display_name}`",
        }

    for tech_id in technology_ids:
        normalized_id = tech_id.strip().lower()
        if not normalized_id:
            continue
        entry = claims_by_id.get(normalized_id)
        if entry is None:
            errors.append(f"terminal_technology_guard references unknown technology id `{tech_id}`")
            continue
        claim = entry["claim"]
        if claim in allowed_terminal_claims or _is_replaced_by_claim(claim):
            continue
        errors.append(
            f"{entry['subject']} has non-terminal claim `{claim}` under terminal_technology_guard"
        )


def _validate_technology_claims(
    *,
    registry: dict[str, Any],
    spec_text: str,
    repo_root: Path,
    dependencies: set[str],
    errors: list[str],
) -> None:
    entries = registry.get("technology_claims") or []
    if not isinstance(entries, list):
        errors.append("technology_claims must be a list")
        return

    lowered_spec = spec_text.lower()
    for index, payload in enumerate(entries, start=1):
        if not isinstance(payload, dict):
            errors.append(f"technology_claims[{index}] must be an object")
            continue

        display_name = str(payload.get("display_name") or payload.get("id") or f"technology-{index}").strip()
        subject = f"technology `{display_name}`"
        claim = _normalize_claim(payload.get("claim"))
        if not _is_supported_technology_claim(claim):
            errors.append(f"{subject} has unsupported claim `{claim}`")
            continue

        markers = _string_list(payload.get("spec_markers_any"), context=f"{subject}.spec_markers_any", errors=errors)
        has_spec_marker = any(marker.lower() in lowered_spec for marker in markers) if markers else False
        require_spec_presence = bool(payload.get("require_spec_presence", True))

        if claim == "removed" or _is_replaced_by_claim(claim):
            if has_spec_marker:
                errors.append(
                    f"{subject} has claim `{claim}` but is still declared as chosen in the stack spec"
                )
            _validate_removed_replacement(
                subject=subject,
                claim=claim,
                replacement_payload=payload.get("replacement"),
                repo_root=repo_root,
                errors=errors,
            )
        else:
            if require_spec_presence and markers and not has_spec_marker:
                errors.append(f"{subject} is not found in stack spec chosen markers")

        if claim == "implemented" or payload.get("runtime_proof") is not None and claim in {"implemented", "partial"}:
            _validate_runtime_proof(
                subject=subject,
                claim=claim,
                proof_payload=payload.get("runtime_proof"),
                repo_root=repo_root,
                dependencies=dependencies,
                errors=errors,
            )


def run(repo_root: Path, registry_path: Path) -> int:
    errors: list[str] = []
    registry = _load_yaml(registry_path, errors)
    if not registry:
        print("stack conformance validation failed:")
        for item in errors:
            print(f"- {item}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    truth_sources = registry.get("truth_sources") or {}
    if not isinstance(truth_sources, dict):
        errors.append("truth_sources must be an object")
        truth_sources = {}

    status_doc = str(truth_sources.get("status_doc") or "").strip()
    spec_doc = str(truth_sources.get("spec_doc") or "").strip()
    pyproject_doc = str(truth_sources.get("pyproject") or "").strip()
    if not status_doc:
        errors.append("truth_sources.status_doc is required")
    if not spec_doc:
        errors.append("truth_sources.spec_doc is required")
    if not pyproject_doc:
        errors.append("truth_sources.pyproject is required")

    status_text = _read_text(repo_root, status_doc, errors, context="status truth source") if status_doc else ""
    spec_text = _read_text(repo_root, spec_doc, errors, context="stack spec truth source") if spec_doc else ""
    dependencies = _parse_dependencies(repo_root, pyproject_doc, errors) if pyproject_doc else set()

    status_rows = _parse_status_table(status_text) if status_text else {}
    non_implemented_labels = _validate_surface_claims(
        registry=registry,
        status_rows=status_rows,
        repo_root=repo_root,
        dependencies=dependencies,
        errors=errors,
    )
    _validate_closure_claim_guard(
        registry=registry,
        repo_root=repo_root,
        non_implemented_labels=non_implemented_labels,
        errors=errors,
    )
    _validate_technology_claims(
        registry=registry,
        spec_text=spec_text,
        repo_root=repo_root,
        dependencies=dependencies,
        errors=errors,
    )
    _validate_terminal_technology_guard(
        registry=registry,
        errors=errors,
    )
    _validate_removed_claim_guard(
        registry=registry,
        repo_root=repo_root,
        errors=errors,
    )
    _validate_removed_technology_active_guard(
        registry=registry,
        repo_root=repo_root,
        errors=errors,
    )

    if errors:
        print("stack conformance validation failed:")
        for item in errors:
            print(f"- {item}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    surface_count = len(registry.get("surface_claims") or [])
    technology_count = len(registry.get("technology_claims") or [])
    print(
        "stack conformance validation: OK "
        f"(surfaces={surface_count} technologies={technology_count} "
        f"non_implemented={len(non_implemented_labels)})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate stack-conformance registry against docs and runtime evidence.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--registry", default="registry/stack_conformance.yaml")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    registry_path = _resolve(repo_root, args.registry)
    sys.exit(run(repo_root, registry_path))


if __name__ == "__main__":
    main()
