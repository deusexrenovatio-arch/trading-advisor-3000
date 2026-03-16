# Codex package: AI Delivery Shell для Trading Advisor 3000

Статус: v1  
Назначение: пакет документов для переноса из `deusexrenovatio-arch/trading_advisor` не бизнес-логики, а **слоя AI-first разработки** в новый репозиторий с приложением-заглушкой **Trading Advisor 3000**.

## Что внутри

1. `01_TZ_AI_DELIVERY_SHELL.md` — формализованное ТЗ на перенос слоя.
2. `02_SOURCE_AUDIT_AND_PR43_FINDINGS.md` — обзор исходного репозитория и выжимка из PR #43.
3. `03_MIGRATION_SCOPE_AND_FILE_INVENTORY.md` — файл-за-файлом инвентаризация того, что относится к AI delivery shell.
4. `04_SKILLS_TRANSFER_MATRIX.md` — матрица переноса skills.
5. `05_TRANSFER_PLAN_FOR_CODEX.md` — план миграции и последовательность работ для Codex.
6. `06_PHASES_AND_DOD.md` — подробный DoD по фазам.
7. `07_TARGET_ARCHITECTURE_TRADING_ADVISOR_3000.md` — целевая архитектура нового приложения и shell.
8. `08_TESTING_AND_QA_STRATEGY.md` — тестовая стратегия для process layer.
9. `09_CODEX_IMPLEMENTATION_RUNBOOK.md` — operational runbook для Codex.
10. `10_CODEX_START_PROMPT.md` — готовый стартовый prompt для Codex.
11. `appendices/A_DIRECTORY_TEMPLATE.md` — рекомендованная структура репозитория.
12. `appendices/B_COMMAND_MATRIX.md` — карта команд и когда их запускать.
13. `appendices/C_RISKS_ASSUMPTIONS_NON_GOALS.md` — риски, допущения, non-goals.
14. `appendices/D_SOURCE_EVIDENCE.md` — ссылки на наблюдавшиеся источники.

## Главная идея пакета

Новый репозиторий должен унаследовать не трейдинговую специфику, а **управляющий shell для AI-разработки**:

- узкий hot-context;
- task/session lifecycle;
- surface-aware gates;
- contract-first task intake;
- plans/memory как durable state;
- context routing;
- skills governance;
- architecture-as-docs;
- telemetry и governance dashboard.

## Быстрый способ использовать пакет

1. Прочитать `01`, `02`, `05`, `06`, `07`.
2. Создать skeleton repo по `appendices/A_DIRECTORY_TEMPLATE.md`.
3. Переносить core files по `03_MIGRATION_SCOPE_AND_FILE_INVENTORY.md`.
4. Подключать skills только после стабилизации базового shell по `04_SKILLS_TRANSFER_MATRIX.md`.
5. Работать в Codex строго по `09_CODEX_IMPLEMENTATION_RUNBOOK.md`.

## Нулевая рекомендация

**Не переносить историю как есть.**  
Переносить **схему, автоматы, шаблоны, валидаторы и policy docs**, а не накопленные доменные планы/инциденты/решения исходного трейдингового продукта.

## Покрытие инвентаризации

| Категория | Количество элементов |
| --- | --- |
| Root governance | 6 |
| Core docs / hot source of truth | 9 |
| Checklists / planning / workflows / runbooks | 9 |
| Agent contexts | 9 |
| Architecture | 4 |
| Durable state & task artifacts | 9 |
| Process scripts / entrypoints | 25 |
| Validators (process-layer) | 15 |
| Tests (process/harness layer) | 12 |
