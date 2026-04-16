from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SchemaValidationError(ValueError):
    """Raised when a payload drifts from a declared JSON-schema snapshot."""


def load_schema(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SchemaValidationError(f"schema payload must be an object: {path.as_posix()}")
    return payload


def validate_schema(schema: dict[str, Any], value: object, *, path: str = "$") -> None:
    if "enum" in schema:
        enum_values = schema["enum"]
        if value not in enum_values:
            raise SchemaValidationError(f"{path}: {value!r} is not in enum {enum_values!r}")
    if "const" in schema:
        expected_const = schema["const"]
        if value != expected_const:
            raise SchemaValidationError(f"{path}: {value!r} != const {expected_const!r}")

    schema_type = schema.get("type")
    resolved_type: str | None = None
    if isinstance(schema_type, list):
        for candidate in schema_type:
            if not isinstance(candidate, str):
                raise SchemaValidationError(f"{path}: union type member must be a string")
            if _is_type(candidate, value):
                resolved_type = candidate
                break
        if resolved_type is None:
            raise SchemaValidationError(f"{path}: value does not match any type in {schema_type!r}")
    elif isinstance(schema_type, str):
        if not _is_type(schema_type, value):
            raise SchemaValidationError(f"{path}: expected type `{schema_type}`, got {type(value).__name__}")
        resolved_type = schema_type
    elif schema_type is not None:
        raise SchemaValidationError(f"{path}: unsupported type declaration {schema_type!r}")

    if resolved_type == "string":
        min_length = schema.get("minLength")
        if min_length is not None:
            if not isinstance(min_length, int):
                raise SchemaValidationError(f"{path}: minLength must be integer")
            if len(str(value)) < min_length:
                raise SchemaValidationError(f"{path}: string shorter than minLength={min_length}")
        return

    if resolved_type in {"integer", "number"}:
        minimum = schema.get("minimum")
        if minimum is not None and float(value) < float(minimum):
            raise SchemaValidationError(f"{path}: {value} < minimum {minimum}")
        exclusive_minimum = schema.get("exclusiveMinimum")
        if exclusive_minimum is not None and float(value) <= float(exclusive_minimum):
            raise SchemaValidationError(f"{path}: {value} <= exclusiveMinimum {exclusive_minimum}")
        maximum = schema.get("maximum")
        if maximum is not None and float(value) > float(maximum):
            raise SchemaValidationError(f"{path}: {value} > maximum {maximum}")
        return

    if resolved_type == "array":
        if not isinstance(value, list):
            raise SchemaValidationError(f"{path}: expected list")
        min_items = schema.get("minItems")
        if min_items is not None and len(value) < int(min_items):
            raise SchemaValidationError(f"{path}: expected at least {min_items} items")
        items_schema = schema.get("items")
        if items_schema is not None:
            if not isinstance(items_schema, dict):
                raise SchemaValidationError(f"{path}: items must be a schema object")
            for index, item in enumerate(value):
                validate_schema(items_schema, item, path=f"{path}[{index}]")
        return

    if resolved_type == "object":
        if not isinstance(value, dict):
            raise SchemaValidationError(f"{path}: expected object")
        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            raise SchemaValidationError(f"{path}: properties must be an object")
        required = schema.get("required", [])
        if not isinstance(required, list):
            raise SchemaValidationError(f"{path}: required must be a list")
        for key in required:
            if key not in value:
                raise SchemaValidationError(f"{path}: missing required field `{key}`")

        additional_properties = schema.get("additionalProperties", True)
        for key, item in value.items():
            child_path = f"{path}.{key}"
            if key in properties:
                child_schema = properties[key]
                if not isinstance(child_schema, dict):
                    raise SchemaValidationError(f"{child_path}: child schema must be an object")
                validate_schema(child_schema, item, path=child_path)
                continue
            if additional_properties is False:
                raise SchemaValidationError(f"{path}: unexpected field `{key}`")
            if isinstance(additional_properties, dict):
                validate_schema(additional_properties, item, path=child_path)
        return


def _is_type(expected: str, value: object) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "null":
        return value is None
    raise SchemaValidationError(f"unsupported schema type `{expected}`")
