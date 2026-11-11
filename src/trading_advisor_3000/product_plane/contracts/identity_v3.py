"""Opt-in identity contracts. No runtime, storage, conversion or ID generation.

market_identity.v3.json owns shapes, field bindings, policies and key projections.
The finite evaluator rejects unsupported assertions before choosing any branch.
Legacy DTOs and their validator are intentionally untouched.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .schema_validation import SchemaValidationError, validate_schema

SCHEMAS = Path(__file__).with_name("schemas")
AUTHORITY = "market_identity.v3.json"
CONTRACTS = {
    AUTHORITY,
    "canonical_bar.v3.json",
    "continuous_front_bars.v3.json",
    "instrument_namespace.v1.json",
    "temporal_mapping.v1.json",
    "snapshot_identity_selector.v1.json",
    "snapshot_identity_binding.v1.json",
    "snapshot_read_request.v1.json",
    "snapshot_read_projection.v1.json",
    "native_observation_snapshot.v1.json",
    "native_observation_request.v1.json",
    "native_observation_read.v1.json",
    "native_observation_intent.v1.json",
    "native_observation_completion.v1.json",
    "native_observation_units.v1.json",
    "native_observation_overlay.v1.json",
    "native_observation_compute.v1.json",
    "native_research_l0_context.v1.json",
    "native_research_l0_intent.v1.json",
    "native_research_l0_completion.v1.json",
    "native_research_compute.v1.json",
    "native_research_feature_context.v1.json",
    "native_research_feature_intent.v1.json",
    "native_research_feature_completion.v1.json",
    "native_research_core_policy.v1.json",
    "native_research_core_context.v1.json",
    "native_research_core_intent.v1.json",
    "native_research_core_completion.v1.json",
    "native_research_read_request.v1.json",
    "native_research_read.v1.json",
    "native_research_shadow.v1.json",
    "native_research_source_key_request.v1.json",
    "native_research_source_key_read.v1.json",
    "native_cf_policy.v1.json",
    "native_cf_context.v1.json",
    "native_cf_intent.v1.json",
    "native_cf_completion.v1.json",
    "native_cf_compute.v1.json",
    "native_cf_read_request.v1.json",
    "native_cf_read.v1.json",
}
KEYWORDS = {
    "$schema",
    "$id",
    "$defs",
    "$ref",
    "title",
    "description",
    "type",
    "const",
    "enum",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "minItems",
    "maxItems",
    "minLength",
    "pattern",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "uniqueItems",
    "format",
    "oneOf",
    "allOf",
    "if",
    "then",
    "x-equal",
    "x-equal-spec",
    "x-order",
    "x-intervals",
    "x-within",
    "x-member",
    "x-unique-by",
    "x-relations",
    "x-namespace",
    "x-nested-bounds",
    "x-key",
    "x-set-paths",
    "x-require-known-for-key",
    "x-row-key",
    "x-research-key",
    "x-resolution-match",
    "x-resolution-time",
    "x-tuple-member",
    "x-arrow",
}


def _fail(message: str) -> None:
    raise SchemaValidationError(message)


def _get(value: Any, path: str) -> Any:
    for part in path.split("."):
        if value is None:
            return None
        if not isinstance(value, dict) or part not in value:
            _fail(f"missing path: {path}")
        value = value[part]
    return value


def _instant(value: str) -> datetime:
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z", value
    ):
        _fail(f"expected explicit UTC instant: {value!r}")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SchemaValidationError(f"invalid UTC instant: {value!r}") from exc


def _source_key_time(value: str) -> datetime:
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", value
    ):
        _fail(f"expected source-local key time: {value!r}")
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise SchemaValidationError(f"invalid source-local key time: {value!r}") from exc


def _source_date(value: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        _fail(f"expected source date: {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SchemaValidationError(f"invalid source date: {value!r}") from exc


def _inside(point: str, start: str, end: str | None) -> bool:
    return _instant(start) <= _instant(point) and (end is None or _instant(point) < _instant(end))


def _known(value: Any) -> None:
    if isinstance(value, dict):
        if value.get("state") == "unknown":
            _fail("unknown structural specification cannot prove equivalence")
        for child in value.values():
            _known(child)
    elif isinstance(value, list):
        for child in value:
            _known(child)


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple((key, _freeze(child)) for key, child in sorted(value.items()))
    if isinstance(value, list):
        return tuple(_freeze(child) for child in value)
    return value


def _json_equal(left: Any, right: Any) -> bool:
    # JSON booleans are not numbers; Python's True == 1 is not schema equality.
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _json_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _json_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    return left == right


def _selected(value: Any, path: str) -> list[Any]:
    parts = path.split(".")
    if "*" not in parts:
        found = _get(value, path)
        return [] if found is None else [found]
    prefix, suffix = path.split(".*.", 1)
    return [_get(item, suffix) for item in _get(value, prefix)]


class _Evaluator:
    def __init__(self, namespace: dict[str, Any]) -> None:
        self.namespace = namespace
        self.documents: dict[str, Any] = {}
        # Audit all bundled schema branches, including branches not used by this payload.
        for name in sorted(CONTRACTS):
            self._document(name)

    def _document(self, name: str) -> dict[str, Any]:
        if name not in CONTRACTS:
            _fail(f"schema reference outside identity contract: {name}")
        if name not in self.documents:
            schema = json.loads((SCHEMAS / name).read_text(encoding="utf-8"))
            self.documents[name] = schema
            self._audit(schema)
        return self.documents[name]

    def _audit(self, schema: dict[str, Any]) -> None:
        if not isinstance(schema, dict):
            _fail("schema must be an object")
        if unsupported := set(schema) - KEYWORDS:
            _fail(f"unsupported schema assertions: {sorted(unsupported)}")
        if "x-arrow" in schema:
            rules = schema["x-arrow"]
            if not isinstance(rules, dict) or set(rules) != {
                "required_columns",
                "forbidden_columns",
                "constants",
                "metadata_key",
            }:
                _fail("unsupported Arrow contract rules")
            for name in ("required_columns", "forbidden_columns"):
                if (
                    not isinstance(rules[name], list)
                    or not all(isinstance(x, str) and x for x in rules[name])
                    or len(set(rules[name])) != len(rules[name])
                ):
                    _fail("invalid Arrow column rules")
            if not isinstance(rules["metadata_key"], str) or not rules["metadata_key"]:
                _fail("invalid Arrow metadata key")
            if not isinstance(rules["constants"], list) or not all(
                isinstance(pair, list)
                and len(pair) == 2
                and all(isinstance(x, str) and x for x in pair)
                for pair in rules["constants"]
            ):
                _fail("invalid Arrow constant rules")
            columns = [pair[0] for pair in rules["constants"]]
            if len(set(columns)) != len(columns) or set(columns) != set(rules["required_columns"]):
                _fail("Arrow constants must bind every mandatory metadata column exactly once")
        if "format" in schema and schema["format"] not in {
            "utc-instant",
            "source-key-time",
            "source-date",
        }:
            _fail("unsupported schema format")
        if "additionalProperties" in schema and not isinstance(
            schema["additionalProperties"], bool
        ):
            _fail("unsupported additionalProperties assertion form")
        for keyword in ("minLength", "minItems", "maxItems"):
            if keyword in schema and (type(schema[keyword]) is not int or schema[keyword] < 0):
                _fail(f"invalid schema {keyword}")
        for keyword in ("minimum", "maximum", "exclusiveMinimum"):
            if keyword in schema and (
                type(schema[keyword]) not in (int, float) or not math.isfinite(schema[keyword])
            ):
                _fail(f"invalid schema {keyword}")
        if "uniqueItems" in schema and type(schema["uniqueItems"]) is not bool:
            _fail("invalid schema uniqueItems")
        if "pattern" in schema:
            try:
                re.compile(schema["pattern"])
            except (TypeError, re.error) as exc:
                raise SchemaValidationError("invalid schema pattern") from exc
        if "type" in schema:
            types = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
            allowed = {"string", "number", "integer", "boolean", "null", "object", "array"}
            if not types or any(not isinstance(x, str) or x not in allowed for x in types):
                _fail("invalid schema type")
        if "required" in schema and (
            not isinstance(schema["required"], list)
            or any(not isinstance(x, str) for x in schema["required"])
        ):
            _fail("invalid schema required")
        for keyword in ("enum", "oneOf", "allOf"):
            if keyword in schema and (not isinstance(schema[keyword], list) or not schema[keyword]):
                _fail(f"invalid schema {keyword}")
        for name in ("properties", "$defs"):
            for child in schema.get(name, {}).values():
                self._audit(child)
        for name in ("oneOf", "allOf"):
            for child in schema.get(name, []):
                self._audit(child)
        for name in ("if", "then", "items"):
            if name in schema:
                self._audit(schema[name])
        if "$ref" in schema:
            self._resolve(schema["$ref"])

    def _resolve(self, reference: str) -> dict[str, Any]:
        name, _, fragment = reference.partition("#")
        node = self._document(name or AUTHORITY)
        if fragment:
            if not fragment.startswith("/$defs/"):
                _fail("only local definition references are supported")
            for part in fragment[1:].split("/"):
                node = node[part]
        return node

    def check(self, schema: dict[str, Any], value: Any) -> None:
        if "$ref" in schema:
            self.check(self._resolve(schema["$ref"]), value)
        if "const" in schema and not _json_equal(schema["const"], value):
            _fail("value differs from schema const")
        if "enum" in schema and not any(_json_equal(x, value) for x in schema["enum"]):
            _fail("value outside schema enum")
        # The established validator owns primitive/container checks; avoid recursive
        # use there because it cannot execute refs or composition assertions.
        primitive = {
            k: v
            for k, v in schema.items()
            if k
            in {
                "type",
                "const",
                "enum",
                "required",
                "minItems",
                "minimum",
                "maximum",
                "exclusiveMinimum",
                "minLength",
            }
        }
        if "type" not in primitive:
            # JSON Schema constraints apply by instance type even when the schema
            # omits type (notably conditional branches). The legacy validator
            # needs an explicit dispatch type to execute these same assertions.
            if isinstance(value, bool):
                primitive["type"] = "boolean"
            elif isinstance(value, (int, float)):
                primitive["type"] = "number"
            elif isinstance(value, str):
                primitive["type"] = "string"
            elif isinstance(value, list):
                primitive["type"] = "array"
            elif isinstance(value, dict):
                primitive["type"] = "object"
            elif value is None:
                primitive["type"] = "null"
            else:
                _fail("value is not JSON data")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if not math.isfinite(value):
                _fail("non-finite JSON number")
        validate_schema(primitive, value)
        if isinstance(value, dict):
            if set(schema.get("required", [])) - set(value):
                _fail("missing required fields")
            properties = schema.get("properties", {})
            if schema.get("additionalProperties") is False and set(value) - set(properties):
                _fail(f"unexpected fields: {sorted(set(value) - set(properties))}")
            for key in set(value) & set(properties):
                self.check(properties[key], value[key])
        if isinstance(value, list):
            if len(value) < schema.get("minItems", 0):
                _fail("too few array items")
            if "maxItems" in schema and len(value) > schema["maxItems"]:
                _fail("too many array items")
            for child in value:
                if "items" in schema:
                    self.check(schema["items"], child)
            if schema.get("uniqueItems") and len({_freeze(x) for x in value}) != len(value):
                _fail("duplicate set entries")
            if "x-unique-by" in schema:
                keys = [_get(x, schema["x-unique-by"]) for x in value]
                if len(set(keys)) != len(keys):
                    _fail("duplicate namespace/version candidates")
        if isinstance(value, str):
            if "pattern" in schema and not re.search(schema["pattern"], value):
                _fail("string does not match schema pattern")
            if "format" in schema:
                {
                    "source-key-time": _source_key_time,
                    "source-date": _source_date,
                    "utc-instant": _instant,
                }[schema["format"]](value)
        for child in schema.get("allOf", []):
            self.check(child, value)
        if "oneOf" in schema:
            if sum(self._matches(child, value) for child in schema["oneOf"]) != 1:
                _fail("expected exactly one schema branch")
        if "if" in schema and self._matches(schema["if"], value):
            self.check(schema["then"], value)
        self._constraints(schema, value)

    def _matches(self, schema: dict[str, Any], value: Any) -> bool:
        try:
            self.check(schema, value)
        except SchemaValidationError:
            return False
        return True

    def _constraints(self, schema: dict[str, Any], value: Any) -> None:
        for left, right in schema.get("x-equal", []) + schema.get("x-equal-spec", []):
            if _get(value, left) != _get(value, right):
                _fail(f"unequal fields: {left}, {right}")
        for left, right in schema.get("x-order", []):
            if _get(value, left) > _get(value, right):
                _fail(f"invalid order: {left}, {right}")
        for start, end in schema.get("x-intervals", []):
            a, b = _get(value, start), _get(value, end)
            parser = {"source-key-time": _source_key_time, "source-date": _source_date}.get(
                schema.get("properties", {}).get(start, {}).get("format"), _instant
            )
            if b is not None and parser(a) >= parser(b):
                _fail(f"empty/reversed interval: {start}, {end}")
        for point, start, end in schema.get("x-within", []):
            if not _inside(_get(value, point), _get(value, start), _get(value, end)):
                _fail("observation outside applicability interval")
        for item, items in schema.get("x-member", []):
            if _get(value, item) not in _get(value, items):
                _fail("value outside eligible set")
        for rule in schema.get("x-tuple-member", []):
            expected = tuple(_get(value, path) for path in rule["local"])
            eligible = {
                tuple(_get(row, path) for path in rule["fields"])
                for row in _get(value, rule["set"])
            }
            if expected not in eligible:
                _fail("tuple outside eligibility")
        if "x-namespace" in schema:
            if _get(value, schema["x-namespace"]) != self.namespace["namespace"]:
                _fail("namespace mismatch")
        if "x-nested-bounds" in schema:
            rule = schema["x-nested-bounds"]
            start, end = _get(value, rule["start"]), _get(value, rule["end"])
            point = _get(value, rule["point"])
            if point is not None and not _inside(point, start, end):
                _fail("active observation outside bar")
            for component in _get(value, rule["intervals"]):
                if not _inside(component["valid_from"], start, end) or _instant(
                    component["valid_to"]
                ) > _instant(end):
                    _fail("component contribution outside bar")
        for rule in schema.get("x-relations", []):
            self._relation(rule, value)

    def _relation(self, rule: dict[str, Any], value: dict[str, Any]) -> None:
        if any(_get(value, key) not in allowed for key, allowed in rule.get("when", {}).items()):
            return
        for identity in _selected(value, rule["select"]):
            rows = [x for x in self.namespace[rule["collection"]] if x[rule["key"]] == identity]
            if len(rows) != 1:
                _fail(f"missing/ambiguous namespace FK: {identity}")
            target = rows[0]
            for field, expected in rule.get("constants", {}).items():
                if _get(target, field) != expected:
                    _fail("wrong FK target kind")
            for local, remote in rule.get("match", []):
                if _get(value, local) != _get(target, remote):
                    _fail("FK descriptor mismatch")
            for local, remote in rule.get("not_equal", []):
                if _get(value, local) == _get(target, remote):
                    _fail("membership must preserve component asset")
            for remote, local in rule.get("member", []):
                if _get(target, remote) not in _get(value, local):
                    _fail("instrument outside CF eligibility")
            if "eligibility" in rule:
                eligibility = rule["eligibility"]
                expected = tuple(_get(target, path) for path in eligibility["target"])
                eligible = {
                    tuple(_get(row, path) for path in eligibility["fields"])
                    for row in _get(value, eligibility["set"])
                }
                if expected not in eligible:
                    _fail("instrument tuple outside CF eligibility")
            for local, remote in rule.get("known_equal", []):
                a, b = _get(value, local), _get(target, remote)
                _known(a)
                _known(b)
                if a != b:
                    _fail("instrument and CF structural specifications differ")
            if "expiry" in rule:
                expiry = rule["expiry"]
                expiry_instant = _get(target, expiry["target"])
                if expiry_instant is None:
                    _fail("unknown UTC expiry cannot establish CF eligibility")
                days = (
                    _instant(expiry_instant).date()
                    - _instant(_get(value, expiry["instant"])).date()
                ).days
                policy = _get(value, expiry["policy"])
                if not policy["min_days_to_expiry"] <= days <= policy["max_days_to_expiry"]:
                    _fail("instrument outside expiry eligibility")


def validate_identity_contract(
    name: str, payload: dict[str, Any], namespace: dict[str, Any]
) -> None:
    """Validate supplied metadata only; no namespace lookup or storage side effects."""
    evaluator = _Evaluator(namespace)
    evaluator.check(evaluator._document("instrument_namespace.v1.json"), namespace)
    evaluator.check(evaluator._document(name), payload)


def definition_key(definition: dict[str, Any], namespace: dict[str, Any]) -> tuple[Any, ...]:
    """Expanded definition tuple, never an opaque ID/hash; unknown CF specs reject."""
    validate_identity_contract(AUTHORITY, definition, namespace)
    schema = _Evaluator(namespace)._resolve("#/$defs/definition")
    for path in schema["x-require-known-for-key"]:
        _known(_get(definition, path))
    normalized = copy.deepcopy(definition)
    for path in schema["x-set-paths"]:
        values = _get(normalized, path)
        if values is not None:
            parent, key = path.rsplit(".", 1)
            _get(normalized, parent)[key] = sorted(values, key=_freeze)
    return tuple(
        _freeze(_get(normalized, path)) for path in schema["x-key"][definition["series_type"]]
    )


def logical_key(payload: dict[str, Any], namespace: dict[str, Any]) -> tuple[Any, ...]:
    """Market row key; Research adds its own dataset/contour and version dimensions."""
    validate_identity_contract(
        "canonical_bar.v3.json"
        if payload["definition"]["series_type"] == "native"
        else "continuous_front_bars.v3.json",
        payload,
        namespace,
    )
    schema = _Evaluator(namespace)._resolve("#/$defs/bar")
    row = definition_key(payload["definition"], namespace) + tuple(
        _instant(payload[path]).astimezone(timezone.utc).isoformat()
        if path == "ts_open"
        else payload[path]
        for path in schema["x-row-key"]
    )
    if "research" in payload:
        research = copy.deepcopy(payload["research"])
        for field in ("set_versions", "profile_versions"):
            research[field].sort(key=lambda x: x["name"])
        return tuple(_freeze(research[path]) for path in schema["x-research-key"]) + (row,)
    return row


def resolve_temporal_mapping(
    records: list[dict[str, Any]],
    *,
    namespace: dict[str, Any],
    source_symbol: str,
    source_context: dict[str, str],
    target_kind: str,
    relation: str,
    valid_at: str,
    known_at: str,
) -> dict[str, Any]:
    """Resolve both time axes, rejecting absent or ambiguous evidence; never latest-wins."""
    evaluator = _Evaluator(namespace)
    evaluator.check(evaluator._document("instrument_namespace.v1.json"), namespace)
    schema = evaluator._resolve("#/$defs/mapping")
    query = {
        "namespace": namespace["namespace"],
        "source_symbol": source_symbol,
        "source_context": source_context,
        "relation": relation,
        "target": {"kind": target_kind},
    }
    _instant(valid_at)
    _instant(known_at)
    selected = []
    for record in records:
        evaluator.check(schema, record)
        if all(_get(record, path) == _get(query, path) for path in schema["x-resolution-match"]):
            axes = schema["x-resolution-time"]
            if all(
                _inside(point, *(_get(record, path) for path in axes[axis]))
                for axis, point in (("valid", valid_at), ("known", known_at))
            ):
                selected.append(record)
    if len(selected) != 1:
        _fail("no applicable mapping" if not selected else "ambiguous temporal mapping")
    return copy.deepcopy(selected[0])


def identity_input_fingerprint(payload: dict[str, Any]) -> str:
    """Fingerprint supplied metadata, not a canonical market identity or source hash."""
    encoded = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def snapshot_arrow_rules() -> dict[str, Any]:
    """Schema-owned executable Arrow carrier rules; never a second DTO authority."""
    evaluator = _Evaluator({})
    return copy.deepcopy(evaluator._resolve("#/$defs/read_envelope")["x-arrow"])
