# Module Phase Brief

Updated: 2026-03-25 15:26 UTC

## Parent

- Parent Brief: docs/codex/modules/stack-conformance-remediation.parent.md
- Execution Contract: docs/codex/contracts/stack-conformance-remediation.execution-contract.md

## Phase

- Name: G1 - Machine-Verifiable Stack-Conformance Gate
- Status: completed

## Objective

- Add the registry and validator that make ghost technologies, unsupported closure claims, and doc/runtime drift fail closed.

## In Scope

- `registry/stack_conformance.yaml`
- `scripts/validate_stack_conformance.py`
- targeted tests for the validator
- CI/check wiring required to run the validator on relevant change surfaces

## Out Of Scope

- Delta, Spark, Dagster, runtime, Telegram, or sidecar implementations
- final release re-acceptance
- broad architecture rewrites beyond validator inputs

## Change Surfaces

- CTX-CONTRACTS
- GOV-RUNTIME
- GOV-DOCS

## Constraints

- The validator must cross-check docs/spec claims, dependency/runtime evidence, and ADR replacement status.
- Failing proof must block closure claims instead of degrading to warnings.
- Keep this phase governance-only; do not bundle product implementation here.

## Acceptance Gate

- The validator fails when a surface is marked `implemented` without runtime proof.
- The validator fails when docs claim full closure against non-implemented registry state.
- The validator fails when a removed technology is still declared as chosen in the spec.

## Disprover

- Mark `FastAPI` as implemented without an ASGI entrypoint or dependency and confirm the validator fails.

## Done Evidence

- Registry and validator exist.
- Targeted tests cover deliberate mismatch cases.
- Relevant CI/check flow runs the validator for touched surfaces.

## Rollback Note

- Revert registry/validator wiring together if the implementation blocks valid low-scope work due to incorrect surface matching.
