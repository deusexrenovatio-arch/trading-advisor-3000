from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from build_dual_surface_rename_inventory import (  # noqa: E402
    LEGACY_TOKENS,
    ReferenceRecord,
    classify_reference,
    classify_scope,
    summarize_records,
)


def _token(token_id: str):
    return next(item for item in LEGACY_TOKENS if item.token_id == token_id)


def test_classify_docs_link_as_low_risk_docs_subtree_rename() -> None:
    classified = classify_reference(
        "docs/architecture/repository-surfaces.md",
        _token("docs_architecture_app"),
    )
    assert classified.group == "docs-links"
    assert classified.risk == "low"
    assert classified.wave == "docs-subtree-rename"


def test_classify_ci_and_codeowners_as_high_risk_governance_selector_cutover() -> None:
    classified = classify_reference("CODEOWNERS", _token("src_product_app"))
    assert classified.path_zone == "ci-codeowners"
    assert classified.risk == "high"
    assert classified.wave == "governance-selector-cutover"


def test_classify_scope_marks_historical_prefixes_excluded() -> None:
    assert (
        classify_scope("artifacts/codex/package-intake/20260402/example.md")
        == "excluded-historical"
    )
    assert classify_scope("docs/architecture/repository-surfaces.md") == "active"


def test_summarize_records_aggregates_active_and_excluded_counts() -> None:
    rows = [
        ReferenceRecord(
            file="docs/architecture/repository-surfaces.md",
            line=10,
            token_id="docs_architecture_app",
            token="docs/architecture/app/",
            scope="active",
            path_zone="docs",
            group="docs-links",
            risk="low",
            wave="docs-subtree-rename",
            cluster_id="docs-architecture-linkage",
            wave_owner="architecture",
            line_excerpt="docs/architecture/app/",
        ),
        ReferenceRecord(
            file="scripts/validate_docs_links.py",
            line=24,
            token_id="docs_architecture_app",
            token="docs/architecture/app/",
            scope="active",
            path_zone="scripts",
            group="scripts-validators",
            risk="high",
            wave="compatibility-bridge",
            cluster_id="validator-and-script-paths",
            wave_owner="platform",
            line_excerpt="docs/architecture/app/",
        ),
        ReferenceRecord(
            file="artifacts/codex/package-intake/old.md",
            line=7,
            token_id="tests_app_path",
            token="tests/app/",
            scope="excluded-historical",
            path_zone="other",
            group="test-paths-fixtures",
            risk="medium",
            wave="runtime-test-cutover",
            cluster_id="test-namespace-dependencies",
            wave_owner="app-core+platform",
            line_excerpt="tests/app/",
        ),
    ]

    summary = summarize_records(rows)
    assert summary["counts"]["total_references"] == 3
    assert summary["counts"]["active_references"] == 2
    assert summary["counts"]["excluded_references"] == 1
    assert summary["counts_by_risk"]["high"] == 1
    assert summary["counts_by_risk"]["low"] == 1
    assert summary["counts_by_wave"]["compatibility-bridge"] == 1
