# App Architecture Docs

This section stores product-plane architecture artifacts on top of the existing AI shell.

## Phase 0 Package
- `docs/architecture/app/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md`
- `docs/architecture/app/product-plane-spec-v2/00_AI_Shell_Alignment.md`
- `docs/architecture/app/product-plane-spec-v2/02_Repository_Structure.md`
- `docs/architecture/app/product-plane-spec-v2/06_Phases_and_Acceptance_Gates.md`

## Phase Artifacts
- `docs/architecture/app/phase0-plan.md` - shell alignment and repo landing.
- `docs/architecture/app/phase1-contracts-and-scaffolding.md` - contracts freeze and scaffolding.
- `docs/architecture/app/phase2a-data-plane-mvp.md` - data plane MVP implementation and acceptance.
- `docs/architecture/app/phase2b-research-plane-mvp.md` - research plane MVP implementation and acceptance.
- `docs/architecture/app/phase2c-runtime-mvp.md` - runtime MVP implementation and acceptance.
- `docs/architecture/app/phase2d-execution-mvp.md` - execution MVP implementation and acceptance.
- `docs/architecture/app/phase3-shadow-forward-system-integration.md` - shadow-forward and integrated system replay.

## Related Checklists
- `docs/checklists/app/phase0-acceptance-checklist.md`
- `docs/checklists/app/phase1-acceptance-checklist.md`
- `docs/checklists/app/phase2a-acceptance-checklist.md`
- `docs/checklists/app/phase2b-acceptance-checklist.md`
- `docs/checklists/app/phase2c-acceptance-checklist.md`
- `docs/checklists/app/phase2d-acceptance-checklist.md`
- `docs/checklists/app/phase3-acceptance-checklist.md`

## Boundary Rule
Product-plane changes must not break shell contracts and must not move trading business logic into shell-sensitive paths.
