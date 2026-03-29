from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from scripts.harness.intake_spec_bundle import run_bundle_intake
from scripts.harness.normalize_requirements import (
    RequirementsNormalizationError,
    run_requirements_normalization,
)
from scripts.harness.synthesize_project_docs import (
    ProjectDocsSynthesisError,
    run_project_docs_synthesis,
)


def _build_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, mode="w") as archive:
        for name, text in members.items():
            archive.writestr(name, text.encode("utf-8"))


def test_requirements_pipeline_happy_path_from_sample_bundle(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample_wp03.zip"
    _build_zip(
        zip_path,
        {
            "spec/requirements.md": "\n".join(
                [
                    "# Harness Requirements",
                    "- System must ingest zip bundles and generate a spec manifest.",
                    "- System should build extracted text cache for supported files.",
                    "- Security constraint: do not execute any archive content.",
                    "- Integration: codex exec should run in non-interactive mode.",
                    "- Data: manifest entries must include sha256 and media type.",
                    "- Open question: how to prioritize conflicting requirements?",
                    "- Assumption: input package is provided by internal operator.",
                    "- Acceptance: sample bundle produces spec_manifest.json.",
                ]
            ),
            "spec/duplicate.txt": "System must ingest zip bundles and generate a spec manifest.\n",
        },
    )

    registry_root = tmp_path / "registry"
    intake = run_bundle_intake(
        input_zip=zip_path,
        registry_root=registry_root,
        run_id="RUN-WP03-HAPPY",
    )
    normalized = run_requirements_normalization(
        registry_root=registry_root,
        run_id=intake.run_id,
    )
    docs = run_project_docs_synthesis(
        registry_root=registry_root,
        run_id=intake.run_id,
    )

    assert normalized.output_path.exists()
    normalized_payload = json.loads(normalized.output_path.read_text(encoding="utf-8"))
    assert normalized_payload["run_id"] == intake.run_id
    assert normalized.requirement_count >= 6
    categories = {item["category"] for item in normalized_payload["requirements"]}
    assert "functional" in categories
    assert "security" in categories
    assert "integration" in categories
    assert "open_question" in categories
    assert "acceptance" in categories

    # Duplicated requirement should be deduplicated with multiple source refs.
    functional_items = [
        item
        for item in normalized_payload["requirements"]
        if "generate a spec manifest" in item["statement"].lower()
    ]
    assert len(functional_items) == 1
    assert len(functional_items[0]["source_refs"]) >= 2

    assert docs.output_path.exists()
    docs_payload = json.loads(docs.output_path.read_text(encoding="utf-8"))
    assert docs_payload["run_id"] == intake.run_id
    assert docs_payload["problem_statement"]
    assert docs_payload["data_objects"]
    assert docs_payload["risk_register"]
    assert docs_payload["integration_points"]


def test_requirements_normalization_fail_closed_when_cache_entry_missing(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample_missing_cache.zip"
    _build_zip(
        zip_path,
        {
            "spec.md": "- System must support intake.\n",
        },
    )

    registry_root = tmp_path / "registry"
    intake = run_bundle_intake(
        input_zip=zip_path,
        registry_root=registry_root,
        run_id="RUN-WP03-MISSING-CACHE",
    )

    cache_payload = json.loads((intake.run_root / "extracted_text_cache.json").read_text(encoding="utf-8"))
    first_entry = cache_payload["entries"][0]
    missing_text_file = intake.run_root / first_entry["text_path"]
    missing_text_file.unlink()

    with pytest.raises(RequirementsNormalizationError):
        run_requirements_normalization(registry_root=registry_root, run_id=intake.run_id)

    assert not (intake.run_root / "normalized_requirements.json").exists()


def test_project_docs_synthesis_fail_closed_when_normalized_requirements_missing(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample_docs_failure.zip"
    _build_zip(
        zip_path,
        {
            "spec.md": "- System must support intake.\n",
        },
    )

    registry_root = tmp_path / "registry"
    intake = run_bundle_intake(
        input_zip=zip_path,
        registry_root=registry_root,
        run_id="RUN-WP03-MISSING-NORMALIZED",
    )

    with pytest.raises(ProjectDocsSynthesisError):
        run_project_docs_synthesis(registry_root=registry_root, run_id=intake.run_id)

    assert not (intake.run_root / "project_docs_bundle.json").exists()
