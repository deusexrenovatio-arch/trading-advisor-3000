# Module Phase Brief

Updated: 2026-04-02 14:10 UTC

## Parent

- Parent Brief: docs/codex/modules/dual-surface-safe-rename.parent.md
- Execution Contract: docs/codex/contracts/dual-surface-safe-rename.execution-contract.md

## Phase

- Name: P5 - Governance and CI Selector Cutover
- Status: planned

## Objective

- Finalize governance routing so CI, validators, and ownership routing operate only on target namespaces.

## In Scope

- CODEOWNERS target namespace patterns.
- CI selector and validator updates to target paths.
- PR/checklist template updates removing legacy namespace hints.
- Guard checks that fail on new legacy-path additions.

## Out Of Scope

- Legacy bridge adapter removal.
- Post-cutover closure report publication.
- Any new product capability work unrelated to migration.

## Change Surfaces

- GOV-POLICY
- GOV-RUNTIME
- PROCESS-STATE

## Constraints

- Governance selectors must match actual target paths.
- No direct-main workflow bypasses.
- Legacy namespace additions must fail by default.

## Acceptance Gate

- CODEOWNERS and CI selectors resolve to target namespaces only.
- Validators reject new legacy references by default.
- PR templates and checklists no longer suggest legacy paths.

## Disprover

- Add one legacy selector to CI or CODEOWNERS and verify governance checks fail.

## Done Evidence

- Selector and ownership diff with deterministic validation output.
- Updated templates and migration guard checks.
- Loop and PR gate reports for selector-only mode.

## Release Gate Impact

- Surface Transition: governance selectors `mixed_legacy_target -> target_only`
- Minimum Proof Class: integration
- Accepted State Label: prep_closed

## Release Surface Ownership

- Owned Surfaces: governance_selector_cutover
- Delivered Proof Class: integration
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove ALLOW_RELEASE_READINESS.
- Final closure: this phase does not prove migration cleanup is complete.

## Rollback Note

- Restore previous selector patterns and temporarily reopen dual-path compatibility mode.
