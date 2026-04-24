# Contracts and Scaffolding — Contracts and Scaffolding

## Цель
Заморозить базовую contract surface application-plane без внедрения runtime-логики.

## Deliverables
- contracts package в `src/trading_advisor_3000/product_plane/contracts/*`;
- JSON schema snapshots в `src/trading_advisor_3000/product_plane/contracts/schemas/*`;
- migration skeleton в `src/trading_advisor_3000/migrations/*`;
- fixtures в `tests/product-plane/fixtures/contracts/*`;
- contract tests в `tests/product-plane/contracts/test_contract_scaffolding.py`.

## Принятые решения
1. Source of truth для контрактов на этой фазе: `src/trading_advisor_3000/product_plane/contracts/*` + schema snapshots.
2. Поле `mode` обязательно в контрактах signal/execution (`shadow|paper|live`).
3. Единый идентификатор стратегии зафиксирован как `strategy_version_id` во всех контрактах и migration skeleton.
4. Versioning по схеме `*.v1.json`; breaking changes только через `v2`.

## Acceptance Commands
- `python -m pytest tests/product-plane/contracts -q`
- `python -m pytest tests/product-plane -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`

## Out of Scope
- business strategy logic;
- wire-интеграции (Telegram/StockSharp/QUIK/Finam);
- runtime orchestration.
