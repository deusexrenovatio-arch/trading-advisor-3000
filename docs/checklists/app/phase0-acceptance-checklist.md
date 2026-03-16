# Phase 0 Acceptance Checklist

Дата: 2026-03-16

## Deliverables
- [x] docs package добавлен в `docs/architecture/app/product-plane-spec-v2/`
- [x] AGENTS overlay добавлен в `src/trading_advisor_3000/AGENTS.md`
- [x] repo structure decision зафиксирован (`docs/architecture/app/product-plane-spec-v2/02_Repository_Structure.md` + skeleton dirs)
- [x] phase plan добавлен (`docs/architecture/app/phase0-plan.md`)
- [x] acceptance checklist оформлен

## Acceptance Criteria
- [x] Root shell docs referenced correctly
- [x] Phase package committed в product-plane документации
- [x] Нет неконтролируемых правок shell-sensitive paths (кроме системных task session артефактов)
- [x] Loop gate green

## Evidence Commands
- [x] `python -m pytest tests/app -q`
- [x] `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- [x] `python scripts/run_pr_gate.py --from-git --git-ref HEAD`

## Notes
- Phase 0 intentionally excludes runtime business/domain implementation.
- Все изменения ограничены doc/skeleton/test поверх существующего shell.


