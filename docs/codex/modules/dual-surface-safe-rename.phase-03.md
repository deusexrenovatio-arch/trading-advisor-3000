# Module Phase Brief

Updated: 2026-04-02 13:33 UTC

## Parent

- Parent Brief: docs/codex/modules/dual-surface-safe-rename.parent.md
- Execution Contract: docs/codex/contracts/dual-surface-safe-rename.execution-contract.md

## Phase

- Name: P3 - Low-Risk Physical Rename for Docs
- Status: completed

## Objective

- Perform docs-first physical rename of the product-plane architecture subtree and stabilize all navigation links before runtime namespace changes.

## In Scope

- Physical rename docs/architecture/app -> docs/architecture/product-plane.
- Link and index rewrites for active docs.
- Navigation updates in architecture and runbook hubs.

## Out Of Scope

- Runtime import path rename.
- Tests namespace rename.
- CODEOWNERS and CI final selector-only cutover.

## Change Surfaces

- ARCH-DOCS
- GOV-DOCS
- GOV-RUNTIME

## Constraints

- validate_docs_links must pass without unresolved legacy root references in active docs.
- Historical archives remain untouched.
- Navigation clarity must improve from a first-glance contributor perspective.

## Acceptance Gate

- docs/architecture/product-plane exists and is fully linked.
- validate_docs_links passes on AGENTS.md and docs roots.
- No broken architecture index references remain.

## Disprover

- Intentionally leave one stale link to docs/architecture/app and verify docs-link validation catches it.

## Done Evidence

- Physical docs root moved to `docs/architecture/product-plane/` with architecture files preserved and relinked.
- Legacy compatibility stubs landed under `docs/architecture/app/**` with canonical path pointers to the new docs root.
- Navigation hubs updated to canonical docs namespace:
  - `AGENTS.md`
  - `README.md`
  - `docs/README.md`
  - `docs/agent/entrypoint.md`
  - `docs/architecture/README.md`
  - `docs/architecture/repository-surfaces.md`
  - `product-plane/README.md`
  - `src/trading_advisor_3000/AGENTS.md`
- Stack conformance truth-source registry remapped to canonical docs namespace in `registry/stack_conformance.yaml`.
- Validation and gate evidence captured in the phase PR:
  - `python scripts/validate_docs_links.py --roots AGENTS.md docs`
  - `python scripts/validate_phase_planning_contract.py --changed-files ...`
  - `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
  - `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`

## Release Gate Impact

- Surface Transition: docs namespace cutover `planned -> integrated`
- Minimum Proof Class: integration
- Accepted State Label: prep_closed

## Release Surface Ownership

- Owned Surfaces: docs_namespace_cutover
- Delivered Proof Class: integration
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove ALLOW_RELEASE_READINESS.
- Runtime safety: this phase does not prove code and test namespace cutover stability.

## Rollback Note

- Restore docs/architecture/app subtree and reverse docs link rewrites together if navigation or link integrity regresses.
