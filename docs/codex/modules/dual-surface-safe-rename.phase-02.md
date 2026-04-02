# Module Phase Brief

Updated: 2026-04-02 14:10 UTC

## Parent

- Parent Brief: docs/codex/modules/dual-surface-safe-rename.parent.md
- Execution Contract: docs/codex/contracts/dual-surface-safe-rename.execution-contract.md

## Phase

- Name: P2 - Compatibility Bridge Layer
- Status: planned

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
- CTX-CONTRACTS
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

- Bridge adapters and policy checks exist with tests.
- CI dual-path mode is active and documented.
- rollback notes exist and were dry-run validated.

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
