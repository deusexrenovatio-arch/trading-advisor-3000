# ТЗ: перенос AI Delivery Shell в новый репозиторий Trading Advisor 3000

## 1. Цель

Создать новый репозиторий приложения **Trading Advisor 3000**, в который будет перенесён и адаптирован слой:

- методологии разработки;
- агентных навыков и правил их маршрутизации;
- тестовых подходов;
- архитектурной дисциплины;
- документации, работающей как source-of-truth;
- инструментов, ускоряющих разработку в **чисто-AI среде**.

## 2. Что считается предметом переноса

В scope входят не бизнес-фичи исходного продукта, а его **AI delivery shell**:

1. Hot/Warm/Cold document hierarchy.
2. Session lifecycle (`task_session begin/status/end`).
3. Task request contract + first-time-right + repetition control.
4. Context router и context cards.
5. Loop / PR / Nightly gates.
6. Plans registry и durable memory.
7. Skills governance и skills catalog.
8. Architecture-as-docs.
9. Local hooks, PR-only main policy, CODEOWNERS.
10. Telemetry, dashboard, baseline metrics и process reports.

## 3. Что не входит в scope

Не переносим как часть базовой поставки:

- трейдинговую логику;
- news / MOEX / spread-arbitrage / signal-specific код;
- исторические записи из `plans/items/`, `memory/*`, `docs/tasks/archive/*`, кроме truly generic templates;
- доменные skills, завязанные на торговлю, новости, инструменты биржи или UI конкретного продукта;
- любые legacy wrapper flows, если их функция уже покрывается `task_session.py` и gate family.

## 4. Ожидаемый результат

В результате должен появиться новый репозиторий, в котором:

- есть минимальный модуль приложения `src/trading_advisor_3000/`;
- есть полный AI delivery shell;
- Codex может выполнять изменение по стандартному циклу без ручных обходных путей;
- gates работают по change surface и не запускают лишнее;
- документация управляет процессом, а не дублирует его;
- состояние задачи живёт в репозитории, а не только в ephemeral chat context.

## 5. Принципы решения

### 5.1. Переносим каркас, а не историю
Исторические планы, решения и инциденты исходного продукта не должны загрязнять новый репозиторий.  
Переносятся schema, conventions, templates, generators и validators.

### 5.2. Source-of-truth должен быть узким
В горячем контексте должны жить только документы, без которых агент не может стартовать работу. Всё остальное — warm/cold и читается по сигналу.

### 5.3. Governance должен быть machine-enforced
Если правило важно, оно должно проверяться:
- git hook,
- validator,
- loop/pr/nightly gate,
- test.

### 5.4. Архитектура — это исполняемая документация
Архитектурные карты, слои, сущности и boundary rules должны быть связаны с code review и validation.

### 5.5. Не должно быть “магических обходов”
Никаких скрытых shortcut-путей, ручных “просто поправил файл” и устаревших альтернативных entrypoints.

## 6. Обязательные deliverables

1. Набор root governance files.
2. Hot docs (`docs/agent/*` + индексы).
3. Context cards и router.
4. Scripts for task lifecycle and gates.
5. Validators for process artifacts.
6. Tests for harness/process layer.
7. Durable state directories (`plans`, `memory`, `docs/tasks`).
8. Architecture docs для Trading Advisor 3000.
9. CI lanes для loop/pr/nightly.
10. Bootstrap/runbook пакет для Codex.

## 7. Минимальные нефункциональные требования

- Любой новый агент должен стартовать за один проход по hot docs.
- Любая задача должна иметь task note и validated contract.
- Direct push в `main` должен быть запрещён по умолчанию.
- Любой PR должен проходить через PR gate.
- Nightly lane должна ловить гигиенические деградации.
- Новый repo не должен требовать чтения полного архива документов для базовой работы.
- Любой высокорисковый change surface должен быть явно размечен и routed.

## 8. Success criteria

Проект считается успешным, когда:

1. Codex может поднять shell и выполнить пилотную задачу end-to-end:
   `begin → task note → contract validation → loop gate → PR gate → end`.
2. В новом repo нет legacy references на удалённый `run_lean_gate.py` и доменные MOEX-only переменные.
3. `plans/items/` является каноническим registry.
4. `docs/session_handoff.md` работает как pointer shim.
5. Skill catalog синхронизирован с локальным `.cursor/skills`.
6. Architecture docs и context cards согласованы.
7. Есть измеряемые process KPIs и baseline.

## 9. Формат поставки для Codex

Codex должен получить не только backlog, но и:
- карту переноса файл-за-файлом;
- пошаговую последовательность фаз;
- DoD по фазам;
- runbook запуска;
- стартовый prompt;
- шаблон структуры нового репозитория.
