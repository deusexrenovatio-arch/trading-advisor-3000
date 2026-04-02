# CI And Proof Runtime Contract (H2)

Updated: 2026-04-01 18:05 UTC

## Purpose

Bind phase `H2` to one explicit runtime contract for:
- dependency profile separation;
- surface-aware PR contour selection;
- shared Docker proof path/runtime normalization.

## Status

- Phase: `H2 - CI and Proof Refactor`
- Contract level: `staging-real`
- Scope: hosted Ubuntu CI plus reproducible local Windows replay semantics.
- Accepted local replay binding for H2: Windows host + Docker proof profile (`proof_profile=docker-linux`).

## Dependency Profiles

Stable profile names:
- `base` (shell/governance baseline);
- `runtime-api`;
- `data-proof`;
- `proof-docker`;
- `dev-test` (test tooling umbrella);
- `dev` (full local umbrella for broad parity runs).

Profile planner:
- `python scripts/run_surface_pr_matrix.py --plan-only ...`

## Surface-Aware PR Contours

Contour mapping:
- `runtime-publication`:
  - dependency profile: `runtime-api + dev-test`;
  - checks: runtime API/smoke + sidecar-focused checks.
- `data-proof`:
  - dependency profile: `runtime-api + data-proof + proof-docker + dev-test`;
  - checks: Delta/Spark/Dagster + research handoff checks.
- `mixed-integration`:
  - dependency profile: runtime + data bundles;
  - checks: explicit integration profile (runtime + data union).
- `governance-only`:
  - dependency profile: `dev-test`;
  - no app contour checks are added by the matrix runner.

Executor:
- `python scripts/run_surface_pr_matrix.py ...`

## Shared Proof Runtime Contract

Docker/host path exchange proof runners must use `scripts/proof_runtime_contract.py` for:
- container runtime-root normalization (`normalize_runtime_root`);
- host-to-container path normalization (`host_to_container_path`);
- container-to-host path normalization (`container_to_host_path`);
- strict workspace-root boundary matching (prefix-only collisions are rejected);
- writable output directory/file fail-closed guards;
- host ownership normalization wrapper for docker-run artifacts.

Current adopter:
- `scripts/run_phase2a_spark_proof.py`

## Negative-Path Contract

Required fail-closed tests:
- unwritable output path;
- missing runtime root;
- cross-platform path normalization;
- workspace-root prefix collision that must not remap host paths.

Covered by:
- `tests/product-plane/unit/test_proof_runtime_contract.py`
- `tests/product-plane/unit/test_phase2a_spark_proof_runner.py`

## Evidence Artifacts

- CI surface plan JSON artifact
- CI surface plan markdown summary artifact
- PR gate markdown summary artifact
- Loop gate markdown summary artifact
