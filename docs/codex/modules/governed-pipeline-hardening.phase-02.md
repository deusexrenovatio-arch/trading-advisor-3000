# Module Phase Brief

Updated: 2026-04-01 11:20 UTC

## Parent

- Parent Brief: docs/codex/modules/governed-pipeline-hardening.parent.md
- Execution Contract: docs/codex/contracts/governed-pipeline-hardening.execution-contract.md

## Phase

- Name: H1 - Dual-Mode Operation
- Status: blocked

## Objective

- Keep old commands operational while introducing new route and session modes, and expose snapshot/profile metadata in validator reporting.

## In Scope

- Backward-compatible wrappers for existing governed entry behavior.
- New dual-mode route/session semantics made available and documented.
- Validator and gate reporting enriched with explicit snapshot/profile metadata fields.

## Out Of Scope

- Surface-aware CI matrix refactor.
- Portable proof runner contract implementation.
- Stacked follow-up recomposition logic.
- Enforcement-level fail-closed upgrades.

## Change Surfaces

- GOV-RUNTIME
- GOV-DOCS
- PROCESS-STATE

## Constraints

- Compatibility wrappers must not preserve hidden implicit-state behavior.
- Snapshot/profile metadata must be explicit in outputs, not inferred from chat context.
- Ambiguity must fail closed or produce a machine-readable report.

## Acceptance Gate

- Legacy command paths remain usable as compatibility wrappers with consistent behavior.
- New route/session modes are callable without breaking existing lifecycle entrypoints.
- Loop/PR validator outputs include explicit snapshot mode and profile metadata.

## Disprover

- If a legacy command breaks, or snapshot/profile metadata is absent from validator outputs, this phase fails acceptance.

## Release Gate Impact

- Surface Transition: dual-mode transition `legacy-only -> compatibility plus explicit metadata`
- Minimum Proof Class: integration
- Accepted State Label: prep_closed

## Release Surface Ownership

- Owned Surfaces: dual_mode_transition
- Delivered Proof Class: integration
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Real contour closure: this phase does not prove CI/proof portability or stacked follow-up correctness on real merged history.

## Rollback Note

- Revert dual-mode exposure and metadata wiring together if wrappers become inconsistent or hide implicit-state drift.
