from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from scripts.harness.models import (
    ArtifactManifestEntry,
    NormalizedRequirementModel,
    RunState,
    SpecManifestModel,
    parse_run_state,
)
from scripts.harness.schema_registry import HARNESS_SCHEMA_DRAFT, REQUIRED_SCHEMA_FILES, load_schema_catalog


ROOT = Path(__file__).resolve().parents[2]


def test_wp01_scaffold_paths_exist() -> None:
    required_dirs = (
        "scripts/harness",
        "configs/harness/schemas",
        "configs/harness/prompts",
        "registry/intake",
        "registry/runs",
        "registry/phases",
        "registry/acceptance",
        "registry/traceability",
        "registry/generated",
        "docs/generated",
    )
    for rel in required_dirs:
        assert (ROOT / rel).is_dir(), f"missing scaffold directory: {rel}"

    required_entrypoints = (
        "intake_spec_bundle.py",
        "normalize_requirements.py",
        "synthesize_project_docs.py",
        "plan_phases.py",
        "build_phase_context.py",
        "run_phase_implementation.py",
        "run_phase_review.py",
        "run_phase_acceptance.py",
        "advance_phase.py",
        "render_docs_from_registry.py",
        "validate_registry_consistency.py",
        "run_harness.py",
    )
    harness_dir = ROOT / "scripts" / "harness"
    for file_name in required_entrypoints:
        assert (harness_dir / file_name).is_file(), f"missing harness entrypoint: {file_name}"


def test_wp01_schema_catalog_loads_and_passes_structural_validation() -> None:
    catalog = load_schema_catalog(ROOT / "configs" / "harness" / "schemas")
    assert set(REQUIRED_SCHEMA_FILES) <= set(catalog)
    for file_name in REQUIRED_SCHEMA_FILES:
        schema_payload = catalog[file_name].payload
        assert schema_payload["$schema"] == HARNESS_SCHEMA_DRAFT
        assert schema_payload["type"] == "object"
        assert schema_payload["additionalProperties"] is False


def test_spec_manifest_model_and_dataclass_round_trip() -> None:
    payload = {
        "run_id": "RUN-001",
        "generated_at": "2026-03-26T08:00:00Z",
        "artifacts": [
            {
                "artifact_id": "artifact-001",
                "path": "workspace/spec.md",
                "sha256": "a" * 64,
                "size_bytes": 512,
                "media_type": "text/markdown",
                "extraction_status": "extracted",
                "source_kind": "file",
            }
        ],
    }
    model = SpecManifestModel.model_validate(payload)
    dataclass_entry = ArtifactManifestEntry.from_model(model.artifacts[0])
    assert dataclass_entry.to_model().model_dump() == model.artifacts[0].model_dump()


def test_run_state_model_and_dataclass_round_trip() -> None:
    payload = {
        "run_id": "RUN-001",
        "created_at": "2026-03-26T08:00:00Z",
        "updated_at": "2026-03-26T08:10:00Z",
        "status": "in_progress",
        "current_phase_id": "WP-01",
        "accepted_phase_ids": [],
        "rejected_phase_ids": [],
        "iteration_count": 0,
        "last_event_sequence": 4,
    }
    model = parse_run_state(payload)
    dataclass_state = RunState.from_model(model)
    assert dataclass_state.to_model().model_dump() == model.model_dump()


def test_normalized_requirement_model_rejects_unsupported_category() -> None:
    payload = {
        "requirement_id": "REQ-001",
        "title": "Unsupported category sample",
        "statement": "Category must be constrained by schema.",
        "category": "other",
        "priority": "high",
        "phase_hint": "WP-01",
        "source_refs": ["spec.md#L10"],
        "ambiguity_flag": False,
        "acceptance_hint": None,
        "depends_on": [],
    }
    with pytest.raises(ValidationError):
        NormalizedRequirementModel.model_validate(payload)
