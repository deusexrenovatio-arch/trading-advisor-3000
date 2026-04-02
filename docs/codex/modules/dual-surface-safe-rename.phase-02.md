# Module Phase Brief

Updated: 2026-04-02 15:05 UTC

## Parent

- Parent Brief: docs/codex/modules/dual-surface-safe-rename.parent.md
- Execution Contract: docs/codex/contracts/dual-surface-safe-rename.execution-contract.md

## Phase

- Name: P2 - Compatibility Bridge Layer
- Status: completed

## Objective

- Introduce temporary compatibility adapters and anti-regression controls so old and target namespaces can coexist safely during migration.

## In Scope

- Transitional import and path adapters for runtime and test selectors.
- Validation rule that blocks new legacy references in changed files.
- Dual-path CI acceptance mode for the migration window.

## Out Of Scope

- Physical docs subtree rename.
- Physical runtime and tests namespace cutover.
- Final selector and ownership cutover.

## Change Surfaces

- GOV-RUNTIME
- CTX-ORCHESTRATION
- ARCH-DOCS
- PROCESS-STATE

## Constraints

- Bridge layer must be explicit and removable.
- New legacy references must fail closed in changed files.
- Existing workloads must stay green with bridge enabled.

## Acceptance Gate

- Compatibility checks prove old and target paths are both routable where required.
- New legacy references are blocked in changed files.
- Loop and PR gates remain green under dual-path mode.

## Disprover

- Add a fresh legacy path reference in a changed file and verify bridge policy rejects it.

## Done Evidence

- src/trading_advisor_3000/product_plane/__init__.py provides temporary import bridge to legacy product namespace.
- docs/architecture/product-plane/README.md provides compatibility pointer for docs root migration.
- scripts/validate_legacy_namespace_growth.py blocks new legacy namespace tokens in changed files outside explicit migration allowlist.
- configs/change_surface_mapping.yaml runs legacy growth validator inside default loop lane.
- tests/process/test_validate_legacy_namespace_growth.py and tests/process/test_gate_scope_routing.py cover scoped execution and fail-closed behavior.

## Release Gate Impact

- Surface Transition: compatibility bridge controls `missing -> active`
- Minimum Proof Class: integration
- Accepted State Label: prep_closed

## Release Surface Ownership

- Owned Surfaces: compatibility_bridge_controls
- Delivered Proof Class: integration
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove ALLOW_RELEASE_READINESS.
- Physical cutover: this phase does not prove legacy namespaces are removed.

## Rollback Note

- Disable bridge controls and restore pre-bridge selectors in one atomic rollback patch.
