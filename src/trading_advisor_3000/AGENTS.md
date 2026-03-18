# AGENTS.md — Product Plane Overlay (Trading Advisor 3000)

Этот файл дополняет root `AGENTS.md` и задаёт правила для product-plane.
Root shell policy остаётся источником истины.

## Что читать вначале
1. Root `AGENTS.md`
2. `docs/agent/entrypoint.md`
3. `docs/agent/domains.md`
4. `docs/agent/checks.md`
5. `docs/agent/runtime.md`
6. `docs/DEV_WORKFLOW.md`
7. `docs/architecture/app/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md`
8. `docs/architecture/app/product-plane-spec-v2/00_AI_Shell_Alignment.md`
9. `docs/architecture/app/product-plane-spec-v2/06_Phases_and_Acceptance_Gates.md`

## Scope и границы
- Product code размещается в `src/trading_advisor_3000/*`.
- Product tests размещаются в `tests/app/*`.
- Product docs размещаются в `docs/architecture/app/*`, `docs/runbooks/app/*`, `docs/workflows/app/*`, `docs/checklists/app/*`.
- Deployment artifacts размещаются в `deployment/*`.

## Запреты
1. Не изменять shell runtime/contracts ради продуктовой логики.
2. Не размещать product runtime config в root `configs/*` без отдельного governance-решения.
3. Не обходить loop/pr/nightly gates.
4. Не смешивать shell-sensitive и product patch set без явной необходимости.

## Фазная дисциплина
- Phase 0: docs + структура + приёмка без продуктового runtime-кода.
- Переход к следующей фазе допустим только после закрытия acceptance текущей.
- Для high-risk изменений соблюдать порядок: `contracts/policy -> code -> docs`.

## Базовый тестовый минимум
- `python -m pytest tests/app -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
