from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


HARNESS_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema"
REQUIRED_SCHEMA_FILES = (
    "phase_acceptance_report.schema.json",
    "phase_context.schema.json",
    "phase_plan.schema.json",
    "phase_review_report.schema.json",
    "phase_rework_request.schema.json",
    "project_docs_bundle.schema.json",
    "run_state.schema.json",
    "normalized_requirements.schema.json",
    "spec_manifest.schema.json",
    "traceability_matrix.schema.json",
)


class SchemaValidationError(ValueError):
    """Raised when harness JSON schema loading or shape checks fail."""


@dataclass(frozen=True)
class LoadedSchema:
    file_name: str
    schema_id: str
    payload: dict[str, object]


def resolve_schema_dir(repo_root: Path | None = None) -> Path:
    root = repo_root or Path(__file__).resolve().parents[2]
    return root / "configs" / "harness" / "schemas"


def load_schema(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise SchemaValidationError(f"invalid JSON in schema `{path.name}`: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise SchemaValidationError(f"schema `{path.name}` must be a JSON object")
    return payload


def _validate_required_shape(file_name: str, payload: dict[str, object]) -> None:
    required_keys = {"$schema", "$id", "title", "type", "properties", "required", "additionalProperties"}
    missing = sorted(required_keys - set(payload))
    if missing:
        raise SchemaValidationError(f"schema `{file_name}` missing keys: {', '.join(missing)}")

    if payload["$schema"] != HARNESS_SCHEMA_DRAFT:
        raise SchemaValidationError(
            f"schema `{file_name}` must declare draft `{HARNESS_SCHEMA_DRAFT}` in `$schema`"
        )

    if payload["type"] != "object":
        raise SchemaValidationError(f"schema `{file_name}` top-level type must be `object`")

    properties = payload.get("properties")
    if not isinstance(properties, dict) or not properties:
        raise SchemaValidationError(f"schema `{file_name}` must provide non-empty object `properties`")

    required = payload.get("required")
    if not isinstance(required, list):
        raise SchemaValidationError(f"schema `{file_name}` must provide `required` as list")
    for item in required:
        if not isinstance(item, str) or not item:
            raise SchemaValidationError(f"schema `{file_name}` has invalid entry in `required`")

    missing_props = sorted(req for req in required if req not in properties)
    if missing_props:
        raise SchemaValidationError(
            f"schema `{file_name}` declares missing required properties: {', '.join(missing_props)}"
        )

    if not isinstance(payload.get("additionalProperties"), bool):
        raise SchemaValidationError(f"schema `{file_name}` must set boolean `additionalProperties`")


def _validate_with_jsonschema_if_available(file_name: str, payload: dict[str, object]) -> None:
    try:
        from jsonschema.validators import validator_for
    except ImportError:
        return

    try:
        validator = validator_for(payload)
        validator.check_schema(payload)
    except Exception as exc:  # pragma: no cover - depends on optional dependency
        raise SchemaValidationError(f"jsonschema validation failed for `{file_name}`: {exc}") from exc


def load_schema_catalog(schema_dir: Path | None = None) -> dict[str, LoadedSchema]:
    directory = schema_dir or resolve_schema_dir()
    if not directory.exists() or not directory.is_dir():
        raise SchemaValidationError(f"schema directory not found: {directory}")

    discovered = {path.name: path for path in directory.glob("*.schema.json")}
    missing_required = sorted(set(REQUIRED_SCHEMA_FILES) - set(discovered))
    if missing_required:
        raise SchemaValidationError(f"missing required schema files: {', '.join(missing_required)}")

    catalog: dict[str, LoadedSchema] = {}
    for file_name in sorted(discovered):
        path = discovered[file_name]
        payload = load_schema(path)
        _validate_required_shape(file_name, payload)
        _validate_with_jsonschema_if_available(file_name, payload)
        schema_id = payload.get("$id")
        if not isinstance(schema_id, str) or not schema_id:
            raise SchemaValidationError(f"schema `{file_name}` must define non-empty `$id`")
        catalog[file_name] = LoadedSchema(file_name=file_name, schema_id=schema_id, payload=payload)

    return catalog


__all__ = [
    "HARNESS_SCHEMA_DRAFT",
    "LoadedSchema",
    "REQUIRED_SCHEMA_FILES",
    "SchemaValidationError",
    "load_schema",
    "load_schema_catalog",
    "resolve_schema_dir",
]
