# Phase 1 — Contracts and Scaffolding

## Цель
Заморозить базовую contract surface application-plane без внедрения runtime-логики.

## Deliverables
- contracts package в `src/trading_advisor_3000/app/contracts/*`;
- JSON schema snapshots в `src/trading_advisor_3000/app/contracts/schemas/*`;
- migration skeleton в `src/trading_advisor_3000/migrations/*`;
- fixtures в `tests/app/fixtures/contracts/*`;
- contract tests в `tests/app/contracts/test_phase1_contracts.py`.

## Принятые решения
1. Source of truth для контрактов на этой фазе: `src/trading_advisor_3000/app/contracts/*` + schema snapshots.
2. Поле `mode` обязательно в контрактах signal/execution (`shadow|paper|live`).
3. Единый идентификатор стратегии зафиксирован как `strategy_version_id` во всех контрактах и migration skeleton.
4. Versioning по схеме `*.v1.json`; breaking changes только через `v2`.

## Acceptance Commands
- `python -m pytest tests/app/contracts -q`
- `python -m pytest tests/app -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`

## Out of Scope
- business strategy logic;
- wire-интеграции (Telegram/StockSharp/QUIK/Finam);
- runtime orchestration.
