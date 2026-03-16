# App Architecture Docs

Этот раздел хранит product-plane документацию поверх существующего AI shell.

## Phase 0 пакет
- `docs/architecture/app/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md`
- `docs/architecture/app/product-plane-spec-v2/00_AI_Shell_Alignment.md`
- `docs/architecture/app/product-plane-spec-v2/02_Repository_Structure.md`
- `docs/architecture/app/product-plane-spec-v2/06_Phases_and_Acceptance_Gates.md`

## Операционные документы фазы
- `docs/architecture/app/phase0-plan.md` — план выполнения и границы patch set.
- `docs/checklists/app/phase0-acceptance-checklist.md` — приёмка с evidence.

## Важное правило
Product-plane изменения не должны ломать shell contracts и не должны переносить торговую логику в shell paths.



- docs/architecture/app/phase1-contracts-and-scaffolding.md — артефакты и приёмка Phase 1 contracts/scaffolding.

