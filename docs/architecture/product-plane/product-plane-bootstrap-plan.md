# Bootstrap Plan - Shell Alignment and Repo Landing

## Historical Status

This document records the original product-plane repo landing slice from
2026-03-16. It is historical evidence, not a current implementation or readiness
claim.

The old product-plane spec package that drove this bootstrap plan is archived at
`docs/archive/product-plane-spec-v2/2026-05-06/README.md`.

## Goal

Land product-plane structure inside the existing AI delivery shell without
moving trading-domain runtime logic into shell surfaces.

## Change Surface
- `src/trading_advisor_3000/*`
- `tests/product-plane/*`
- `docs/architecture/product-plane/*`
- `docs/checklists/app/*`
- `docs/runbooks/app/*`
- `docs/workflows/app/*`
- `deployment/*`

## Original Scope
1. Add the product docs package that is now archived as spec v2 provenance.
2. Add product overlay `src/trading_advisor_3000/AGENTS.md`.
3. Record target product-plane skeleton directories.
4. Prepare acceptance checklist and evidence commands.

## Original Non-Scope
1. Runtime/data/research/execution module implementation.
2. Changes to shell canonical entrypoints and governance contracts.
3. External-system integrations and secrets.

## Baseline Decision

The original plan used the "PR #1 merged first" decision from the archived
`docs/archive/product-plane-spec-v2/2026-05-06/00_AI_Shell_Alignment.md`
package:

- shell hardening was accepted as the baseline;
- product-plane bootstrap ran on top of that baseline;
- mixed shell/product patch sets were not used for this bootstrap slice.

## Verification Plan
1. `python -m pytest tests/product-plane -q`
2. `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
3. `python scripts/run_pr_gate.py --from-git --git-ref HEAD`

## Current Reading Rule

Use this document only for bootstrap provenance. For current product-plane truth,
read `docs/architecture/product-plane/STATUS.md`,
`docs/architecture/product-plane/CONTRACT_SURFACES.md`, and
`docs/project-map/current-truth-map-2026-05-05.md`.
