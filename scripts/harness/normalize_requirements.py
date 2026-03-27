from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from .models import (
        NormalizedRequirementModel,
        NormalizedRequirementsModel,
        parse_normalized_requirements,
    )
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import (
        NormalizedRequirementModel,
        NormalizedRequirementsModel,
        parse_normalized_requirements,
    )


PHASE_HINT_RE = re.compile(r"\b(?:wp|phase)[-_ ]?(\d{1,2}[a-z]?)\b", re.IGNORECASE)
DEPENDS_ON_RE = re.compile(r"\bdepends on[: ]+([A-Za-z0-9_.-]+)\b", re.IGNORECASE)
RUS_DEPENDS_ON_RE = re.compile(r"\bзависит от[: ]+([A-Za-z0-9_.-]+)\b", re.IGNORECASE)


class RequirementsNormalizationError(RuntimeError):
    """Raised when requirements normalization cannot produce canonical output."""


@dataclass(frozen=True)
class RequirementsNormalizationResult:
    run_id: str
    output_path: Path
    requirement_count: int
    source_count: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise RequirementsNormalizationError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RequirementsNormalizationError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise RequirementsNormalizationError(f"payload at `{path}` must be JSON object")
    return payload


def _normalize_statement(text: str) -> str:
    compact = re.sub(r"\s+", " ", text.strip())
    compact = compact.lstrip("-*").strip()
    compact = re.sub(r"^\d+\.\s*", "", compact)
    return compact


def _statement_key(statement: str) -> str:
    return re.sub(r"\s+", " ", statement.lower()).strip()


def _extract_candidates(text: str) -> list[tuple[int, str]]:
    lines = text.splitlines()
    candidates: list[tuple[int, str]] = []
    fallback_lines: list[tuple[int, str]] = []
    keyword_hints = (
        "must",
        "shall",
        "should",
        "need to",
        "required",
        "необходимо",
        "должен",
        "должна",
        "требуется",
        "ограничение",
        "acceptance",
        "open question",
        "integration",
        "security",
    )

    for index, raw_line in enumerate(lines, start=1):
        line = _normalize_statement(raw_line)
        if not line:
            continue
        if line.startswith("#"):
            continue
        if line.startswith("```"):
            continue
        if len(line) < 10:
            continue
        fallback_lines.append((index, line))
        lowered = line.lower()
        if any(keyword in lowered for keyword in keyword_hints):
            candidates.append((index, line))

    if candidates:
        return candidates
    return fallback_lines[:20]


def _infer_category(statement: str) -> str:
    lowered = statement.lower()
    if "open question" in lowered or "?" in statement or "неясно" in lowered or "уточнить" in lowered:
        return "open_question"
    if "assumption" in lowered or "предполага" in lowered:
        return "assumption"
    if "acceptance" in lowered or "критери" in lowered or "приемк" in lowered:
        return "acceptance"
    if "integration" in lowered or "api" in lowered or "интеграц" in lowered:
        return "integration"
    if "security" in lowered or "секьюр" in lowered or "безопас" in lowered:
        return "security"
    if "data " in lowered or "schema" in lowered or "dataset" in lowered or "таблиц" in lowered:
        return "data"
    if "must not" in lowered or "forbid" in lowered or "огранич" in lowered or "constraint" in lowered:
        return "constraint"
    if "latency" in lowered or "performance" in lowered or "slo" in lowered or "non-functional" in lowered:
        return "non_functional"
    return "functional"


def _infer_priority(statement: str, category: str) -> str:
    lowered = statement.lower()
    if category == "open_question":
        return "low"
    if any(token in lowered for token in ("must", "shall", "обязательно", "необходимо", "должен", "должна")):
        return "critical"
    if any(token in lowered for token in ("required", "важно", "критично", "critical")):
        return "high"
    if "should" in lowered or "желательно" in lowered:
        return "medium"
    return "low"


def _infer_phase_hint(statement: str) -> str | None:
    match = PHASE_HINT_RE.search(statement)
    if not match:
        return None
    return f"WP-{match.group(1).upper()}"


def _infer_ambiguity(statement: str) -> bool:
    lowered = statement.lower()
    return any(token in lowered for token in ("?", "tbd", "todo", "to be defined", "неясно", "уточнить"))


def _infer_depends_on(statement: str) -> list[str]:
    depends: list[str] = []
    for regex in (DEPENDS_ON_RE, RUS_DEPENDS_ON_RE):
        for match in regex.findall(statement):
            depends.append(match)
    return sorted(set(depends))


def _build_requirement_payload(
    *,
    requirement_id: str,
    statement: str,
    source_refs: list[str],
) -> dict[str, object]:
    category = _infer_category(statement)
    ambiguity = _infer_ambiguity(statement)
    acceptance_hint = statement if category == "acceptance" else None
    return {
        "requirement_id": requirement_id,
        "title": statement[:96],
        "statement": statement,
        "category": category,
        "priority": _infer_priority(statement, category),
        "phase_hint": _infer_phase_hint(statement),
        "source_refs": sorted(set(source_refs)),
        "ambiguity_flag": ambiguity,
        "acceptance_hint": acceptance_hint,
        "depends_on": _infer_depends_on(statement),
    }


def _load_text_cache(run_root: Path) -> tuple[str, list[dict[str, object]]]:
    cache_path = run_root / "extracted_text_cache.json"
    payload = _read_json(cache_path)
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise RequirementsNormalizationError(f"`run_id` must be non-empty string in {cache_path}")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise RequirementsNormalizationError(f"`entries` must be list in {cache_path}")
    if not entries:
        raise RequirementsNormalizationError("text cache is empty; cannot normalize requirements")
    return run_id, entries


def run_requirements_normalization(
    *,
    registry_root: Path,
    run_id: str,
) -> RequirementsNormalizationResult:
    run_root = registry_root / "intake" / run_id
    if not run_root.exists():
        raise RequirementsNormalizationError(f"run root not found: {run_root}")

    manifest_payload = _read_json(run_root / "spec_manifest.json")
    manifest_run_id = manifest_payload.get("run_id")
    if manifest_run_id != run_id:
        raise RequirementsNormalizationError(
            f"manifest run_id mismatch: expected `{run_id}`, got `{manifest_run_id}`"
        )

    cache_run_id, cache_entries = _load_text_cache(run_root)
    if cache_run_id != run_id:
        raise RequirementsNormalizationError(
            f"text cache run_id mismatch: expected `{run_id}`, got `{cache_run_id}`"
        )

    dedup_index: dict[str, int] = {}
    requirements: list[dict[str, object]] = []

    for entry in cache_entries:
        if not isinstance(entry, dict):
            raise RequirementsNormalizationError("text cache entry must be object")
        artifact_id = entry.get("artifact_id")
        text_relpath = entry.get("text_path")
        if not isinstance(artifact_id, str) or not artifact_id:
            raise RequirementsNormalizationError("text cache entry missing `artifact_id`")
        if not isinstance(text_relpath, str) or not text_relpath:
            raise RequirementsNormalizationError("text cache entry missing `text_path`")

        text_path = run_root / text_relpath
        if not text_path.exists():
            raise RequirementsNormalizationError(f"text cache file not found: {text_path}")

        text = text_path.read_text(encoding="utf-8")
        for line_no, statement_raw in _extract_candidates(text):
            statement = _normalize_statement(statement_raw)
            if not statement:
                continue
            source_ref = f"{artifact_id}:L{line_no}"
            dedup_key = _statement_key(statement)

            if dedup_key in dedup_index:
                req_item = requirements[dedup_index[dedup_key]]
                existing_refs = set(req_item["source_refs"])
                existing_refs.add(source_ref)
                req_item["source_refs"] = sorted(existing_refs)
                continue

            requirement_id = f"REQ-{len(requirements) + 1:04d}"
            payload = _build_requirement_payload(
                requirement_id=requirement_id,
                statement=statement,
                source_refs=[source_ref],
            )
            # Early strict check per record.
            NormalizedRequirementModel.model_validate(payload)
            dedup_index[dedup_key] = len(requirements)
            requirements.append(payload)

    if not requirements:
        raise RequirementsNormalizationError("no requirement candidates extracted from text cache")

    normalized_payload = {
        "run_id": run_id,
        "generated_at": _utc_now(),
        "requirements": requirements,
    }
    normalized_model: NormalizedRequirementsModel = parse_normalized_requirements(normalized_payload)
    output_path = run_root / "normalized_requirements.json"
    output_path.write_text(
        json.dumps(normalized_model.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return RequirementsNormalizationResult(
        run_id=run_id,
        output_path=output_path,
        requirement_count=len(normalized_model.requirements),
        source_count=len(cache_entries),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize extracted requirements into canonical harness artifact.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        result = run_requirements_normalization(
            registry_root=Path(args.registry_root).resolve(),
            run_id=args.run_id,
        )
    except RequirementsNormalizationError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "normalized_requirements": result.output_path.as_posix(),
                "requirement_count": result.requirement_count,
                "source_count": result.source_count,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
