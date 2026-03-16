# Phase 0 Plan — Shell Alignment and Repo Landing

## Цель
Зафиксировать интеграцию product-plane в существующий AI delivery shell без внедрения продуктовой торговой логики.

## Change surface
- `src/trading_advisor_3000/*`
- `tests/app/*`
- `docs/architecture/app/*`
- `docs/checklists/app/*`
- `docs/runbooks/app/*`
- `docs/workflows/app/*`
- `deployment/*`

## Что входит в фазу
1. Добавление product docs package (spec v2) в `docs/architecture/app/product-plane-spec-v2/`.
2. Добавление product overlay `src/trading_advisor_3000/AGENTS.md`.
3. Фиксация целевой структуры product-plane каталогов (skeleton only).
4. Подготовка acceptance checklist и evidence команд.

## Что не входит в фазу
1. Реализация runtime/data/research/execution модулей.
2. Изменение shell canonical entrypoints и governance contracts.
3. Интеграции с внешними системами и секретами.

## Решение по baseline PR #1
Применяется вариант "PR #1 merged first" из `docs/architecture/app/product-plane-spec-v2/00_AI_Shell_Alignment.md`:
- shell hardening принимается как baseline;
- product phase выполняется поверх этого baseline;
- смешанные shell/product patch sets в этой фазе не используются.

## План проверки
1. `python -m pytest tests/app -q`
2. `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
3. `python scripts/run_pr_gate.py --from-git --git-ref HEAD`

## Критерии готовности (Phase 0)
- root shell docs корректно referenced в product overlay и phase docs;
- phase package закоммичиваемый и полный;
- shell-sensitive пути не изменяются произвольно;
- loop gate зелёный.

