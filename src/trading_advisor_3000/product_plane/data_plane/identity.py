"""Public Market Data snapshot identity resolution for trusted admitted inputs.

Source-byte verification belongs to the source-backed constructor. This pure
boundary validates supplied metadata and exact pins; it never infers temporal
applicability, reads analytical storage, or silently invokes another resolver.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from typing import Any, Sequence

from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    identity_input_fingerprint,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError


@dataclass(frozen=True)
class SnapshotIdentitySelector:
    """Immutable schema-validated envelope; field meaning stays in Contracts."""

    _json: str

    @classmethod
    def from_payload(
        cls, payload: dict[str, Any], namespace: dict[str, Any]
    ) -> SnapshotIdentitySelector:
        validate_identity_contract("snapshot_identity_selector.v1.json", payload, namespace)
        return cls(json.dumps(payload, sort_keys=True, ensure_ascii=False, allow_nan=False))

    def to_payload(self) -> dict[str, Any]:
        return json.loads(self._json)


def resolve_snapshot_identity(
    selector: SnapshotIdentitySelector,
    *,
    namespace: dict[str, Any],
    bindings: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    """Resolve one trusted admitted binding; zero/multiple matches fail closed.

    A source-backed admission constructor must verify original input bytes and
    associations before supplying bindings. Equal asserted hashes alone are not
    evidence of source verification. Temporal parameters are invalid by schema.
    """
    query = selector.to_payload()
    validate_identity_contract("snapshot_identity_selector.v1.json", query, namespace)
    namespace_hash = identity_input_fingerprint(namespace)
    if query["pins"]["namespace_sha256"] != namespace_hash:
        raise SchemaValidationError("namespace fingerprint mismatch")
    selected = []
    for binding in bindings:
        validate_identity_contract("snapshot_identity_binding.v1.json", binding, namespace)
        if binding["selector"]["pins"]["namespace_sha256"] != namespace_hash:
            raise SchemaValidationError("binding namespace fingerprint mismatch")
        if binding["selector"] == query:
            selected.append(binding)
    if len(selected) != 1:
        raise SchemaValidationError(
            "no applicable snapshot binding" if not selected else "ambiguous snapshot binding"
        )
    return copy.deepcopy(selected[0])
