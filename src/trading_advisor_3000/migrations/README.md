# Migrations Skeleton (Phase 1)

Эта директория хранит версионируемые миграции для runtime state (PostgreSQL).

## Структура
- `src/trading_advisor_3000/migrations/versions/` — отдельные версии.
- `src/trading_advisor_3000/migrations/0001_initial_contract_tables.sql` — стартовый SQL-скелет.

## Правила
1. Каждая новая миграция идемпотентна по возможности.
2. Ломающие изменения требуют новой версии и плана обратной совместимости.
3. Изменения контрактов и миграций должны сопровождаться тестами в `tests/app/contracts/*`.
