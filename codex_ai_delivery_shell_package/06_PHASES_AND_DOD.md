# Фазы переноса и подробный DoD

## Phase 0 — Discovery и boundary freeze

### Цель
Зафиксировать, что именно относится к AI delivery shell, а что остаётся в исходном домене.

### Deliverables
- Файл-инвентаризация.
- Матрица skills.
- Freeze list для domain-specific assets.
- Нейтральный glossary/env-var mapping.

### DoD
- Есть утверждённый список файлов/каталогов, входящих в shell.
- Для каждого элемента задан статус: migrate/adapt/template/exclude.
- Доменные активы отделены от процессных.
- Зафиксировано, что `run_lean_gate.py` и `MOEX_CARRY_ALLOW_MAIN_PUSH` не переносятся.
- Определён target naming для нового repo (`Trading Advisor 3000`, `AI_SHELL_*` env vars).
- Есть решение по context card rename (`CTX-DOMAIN`, `CTX-EXTERNAL-SOURCES`).

### Evidence
- `03_MIGRATION_SCOPE_AND_FILE_INVENTORY.md`
- `04_SKILLS_TRANSFER_MATRIX.md`
- `appendices/C_RISKS_ASSUMPTIONS_NON_GOALS.md`

---

## Phase 1 — Root governance shell

### Цель
Сделать новый repo “управляемым” до переноса любой прикладной логики.

### Deliverables
- `AGENTS.md`
- `docs/README.md`
- `docs/DEV_WORKFLOW.md`
- `docs/agent/*`
- `CODEOWNERS`
- `.cursorignore`
- `.githooks/pre-push`
- `scripts/install_git_hooks.py`

### DoD
- В корне repo есть рабочий `AGENTS.md`, который:
  - указывает hot/warm/cold source-of-truth;
  - описывает не-negotiable loop;
  - описывает PR-only main policy;
  - ссылается только на существующие документы.
- `.cursorignore` по умолчанию держит cold context закрытым и оставляет hot docs узкими.
- `pre-push` hook:
  - запрещает direct push в `main`;
  - поддерживает только нейтральный emergency override;
  - требует non-empty reason;
  - запускает scoped `run_loop_gate.py`.
- `CODEOWNERS` покрывает как минимум:
  - docs/agent;
  - scripts;
  - plans;
  - memory;
  - tests;
  - src/trading_advisor_3000.
- `docs/README.md` и `docs/DEV_WORKFLOW.md` не противоречат `AGENTS.md`.
- В документации отсутствуют ссылки на legacy flow и удалённые скрипты.
- Есть команда bootstrap для установки hooks.

### Validation
- `python scripts/install_git_hooks.py`
- статическая проверка ссылок docs
- smoke check pre-push hook в dry-run/controlled mode

### Exit evidence
- Новый repo не принимает прямой push в `main`.
- Агент может понять правила входа, читая только hot docs.

---

## Phase 2 — Session lifecycle и task artifacts

### Цель
Сделать так, чтобы любая задача имела контролируемый lifecycle и durable state.

### Deliverables
- `scripts/task_session.py`
- `scripts/handoff_resolver.py`
- `docs/session_handoff.md`
- `docs/tasks/active/` templates
- `docs/tasks/archive/` templates
- `scripts/validate_session_handoff.py`
- `scripts/validate_task_request_contract.py`
- `docs/checklists/task-request-contract.md`
- `docs/checklists/first-time-right-gate.md`

### DoD
- `task_session.py begin` создаёт или активирует task note.
- `task_session.py status` показывает состояние текущей сессии/локи/пути.
- `task_session.py end`:
  - требует closeout evidence;
  - переносит/архивирует task note по правилам;
  - обновляет handoff pointer;
  - синхронизирует task outcomes/state.
- `docs/session_handoff.md` не содержит длинный narrative, а только:
  - ссылку/указатель на активную task note;
  - mode/status;
  - краткий список required commands.
- `validate_task_request_contract.py` блокирует task note без:
  - objective;
  - scope;
  - constraints;
  - done evidence;
  - priority/sequence signal.
- Попытка ссылаться на archived/completed note без closeout evidence ловится валидатором.
- There is no manual context file workaround outside the standard lifecycle.

### Validation
- `python scripts/task_session.py begin --task demo-shell-bootstrap`
- `python scripts/validate_task_request_contract.py --task demo-shell-bootstrap`
- `python scripts/validate_session_handoff.py`
- `python scripts/task_session.py end --task demo-shell-bootstrap`

### Exit evidence
- Можно пройти полный begin/status/end на демонстрационной задаче.
- После `end` остаётся корректный pointer shim, а не битая ссылка.

---

## Phase 3 — Context routing и bounded contexts

### Цель
Минимизировать контекст и сделать routing предсказуемым и автоматизируемым.

### Deliverables
- `docs/agent-contexts/README.md`
- context cards
- `scripts/context_router.py`
- `scripts/validate_agent_contexts.py`
- обновлённый `docs/agent/domains.md`

### DoD
- Для каждой крупной зоны repo существует context card с:
  - scope;
  - owned paths;
  - guarded paths;
  - minimum checks;
  - escalation rules.
- `context_router.py` умеет по changed files определить relevant contexts.
- High-risk surface `CTX-CONTRACTS` требует ordered patch series и специальный минимум проверок.
- Переименованные контексты (`CTX-DOMAIN`, `CTX-EXTERNAL-SOURCES`) не несут лишней доменной семантики.
- Нет “ничейных” путей: каждый значимый путь покрыт хотя бы одним контекстом.
- Mixed-surface changes явно распознаются и не маскируются под single-context change.

### Validation
- `pytest tests/process/test_context_router.py`
- synthetic routing fixtures for each context
- validator on context coverage / path ownership

### Exit evidence
- Для произвольного diff можно получить deterministic список context cards и required checks.

---

## Phase 4 — Gate framework и validators

### Цель
Собрать быстрый, многоуровневый и surface-aware механизм проверки изменений.

### Deliverables
- `scripts/compute_change_surface.py`
- `scripts/gate_common.py`
- `scripts/run_loop_gate.py`
- `scripts/run_pr_gate.py`
- `scripts/run_nightly_gate.py`
- `scripts/validate_pr_only_policy.py`
- `scripts/validate_process_regressions.py`
- `scripts/nightly_root_hygiene.py`
- базовый набор process validators

### DoD
- `run_loop_gate.py`:
  - работает быстро;
  - маршрутизирует проверки по change surface;
  - не тянет cold-hygiene проверки без нужды.
- `run_pr_gate.py` является супермножеством `run_loop_gate.py`.
- `run_nightly_gate.py` добавляет cold-hygiene, scorecards, docs and ownership maintenance.
- `compute_change_surface.py` корректно отличает:
  - docs-only;
  - process-only;
  - contracts/high-risk;
  - mixed changes.
- `validate_pr_only_policy.py` проверяет:
  - запрет direct-main path;
  - policy docs;
  - отсутствие deprecated override vars.
- В коде и документации нет ссылок на `run_lean_gate.py`.
- Gates дают одинаковый машиночитаемый формат результата.
- Ошибки сообщают, что именно чинить, а не просто “failed”.

### Validation
- `pytest tests/process/test_compute_change_surface.py`
- `pytest tests/process/test_gate_scope_routing.py`
- `pytest tests/process/test_harness_contracts.py`
- manual dry-runs:
  - docs-only diff
  - process-only diff
  - contract/high-risk diff

### Exit evidence
- Один и тот же diff получает ожидаемый набор проверок в loop/pr/nightly lanes.
- Pre-push hook использует именно `run_loop_gate.py`.

---

## Phase 5 — Durable state, telemetry и governance analytics

### Цель
Сделать AI-разработку измеримой и воспроизводимой между сессиями.

### Deliverables
- `plans/items/` templates
- `plans/PLANS.yaml` generation flow
- `memory/*` schemas
- `scripts/validate_plans.py`
- `scripts/validate_agent_memory.py`
- `scripts/validate_task_outcomes.py`
- `scripts/measure_dev_loop.py`
- `scripts/agent_process_telemetry.py`
- `scripts/process_improvement_report.py`
- `scripts/build_governance_dashboard.py`
- `scripts/harness_baseline_metrics.py`
- `scripts/autonomy_kpi_report.py`

### DoD
- `plans/items/` — канонический source-of-truth, а не `PLANS.yaml`.
- `PLANS.yaml` генерируется или синхронизируется из `plans/items/`.
- `memory/agent_memory.yaml` стартует как seed/template, не как импорт исторического шума.
- `memory/task_outcomes.yaml` обновляется при closeout.
- `memory/decisions`, `memory/incidents`, `memory/patterns` имеют шаблоны и conventions.
- `measure_dev_loop.py` умеет выдавать baseline для скорости цикла.
- `agent_process_telemetry.py` фиксирует ключевые process-сигналы.
- `process_improvement_report.py` формирует actionable report, а не просто dump.
- `build_governance_dashboard.py` собирает обзор состояния shell.
- Репозиторий может пережить смену агентной сессии без потери состояния.

### Validation
- `pytest tests/process/test_measure_dev_loop.py`
- `pytest tests/process/test_agent_process_telemetry.py`
- `pytest tests/process/test_process_reports.py`
- `python scripts/validate_plans.py`
- `python scripts/validate_agent_memory.py`
- `python scripts/validate_task_outcomes.py`

### Exit evidence
- Есть baseline метрики “до/после”.
- Есть пример законченной задачи, оставившей корректный trace в plans/memory/outcomes.

---

## Phase 6 — Skills governance

### Цель
Сделать skills частью управляемой платформы, а не неструктурированного каталога markdown.

### Deliverables
- baseline subset `.cursor/skills/*/SKILL.md`
- `docs/agent/skills-catalog.md`
- `docs/agent/skills-routing.md`
- `docs/workflows/skill-governance-sync.md`
- `scripts/skill_update_decision.py`
- `scripts/skill_precommit_gate.py`
- `scripts/validate_skills.py`

### DoD
- Локальный `.cursor/skills` — primary runtime catalog.
- `skills-catalog.md` генерируется/синхронизируется из локального каталога.
- В baseline подключены только generic skills.
- Доменные skills не попали в hot path случайно.
- Любое изменение skill-файла проверяется на необходимость обновления catalog/routing docs.
- `validate_skills.py` ловит рассинхронизацию.
- `.cursorignore` не открывает весь skills corpus в hot context.
- Есть documented flow: как добавить новый skill и как подтвердить его актуальность.

### Validation
- `python scripts/skill_update_decision.py ...`
- `python scripts/skill_precommit_gate.py`
- `python scripts/validate_skills.py`
- synthetic change against one core skill

### Exit evidence
- Добавление/изменение skill проходит через policy, validator и catalog sync.

---

## Phase 7 — Architecture package и placeholder application

### Цель
Встроить shell в реальный repo так, чтобы он управлял приложением, а не висел рядом с ним.

### Deliverables
- `docs/architecture/trading-advisor-3000.md`
- `docs/architecture/layers-v2.md`
- `docs/architecture/entities-v2.md`
- `docs/architecture/architecture-map-v2.md`
- `scripts/sync_architecture_map.py`
- `src/trading_advisor_3000/` skeleton
- базовые architecture tests

### DoD
- Есть архитектурный narrative документ для Trading Advisor 3000.
- Есть layered map без доменной утечки из старого продукта.
- Есть entity template/model without copied trading entities.
- `sync_architecture_map.py` синхронизирует map artifacts из source docs.
- Приложение отделено от control plane:
  - `src/trading_advisor_3000/` содержит placeholder module;
  - process shell живёт вне прикладного модуля.
- Architecture docs согласованы с context cards.
- Есть минимум один тест/validator на boundary rules.

### Validation
- architecture docs review
- sync script run
- cross-layer validation smoke
- import smoke для `src/trading_advisor_3000/`

### Exit evidence
- Shell может управлять изменениями в приложении, даже если прикладная логика ещё пустая.

---

## Phase 8 — CI, pilot task и operational proving

### Цель
Подтвердить, что shell работает как production-grade способ разработки, а не как набор локальных идей.

### Deliverables
- CI workflows для loop/pr/nightly
- dashboard lane
- pilot task
- runbook updates
- post-pilot report

### DoD
- В CI есть отдельные lanes для:
  - loop gate;
  - PR gate;
  - nightly gate;
  - dashboard/report refresh.
- Pilot task проходит стандартный цикл:
  - begin;
  - contract validation;
  - implementation;
  - loop gate;
  - PR gate;
  - end;
  - dashboard/report update.
- Нет ручных bypass, не зафиксированных policy docs.
- На pilot task есть:
  - task note;
  - task outcome;
  - evidence in plans/memory;
  - CI traces;
  - short postmortem.
- Есть baseline comparison “до/после” по времени цикла и количеству ручных действий.
- Документация обновлена после пилота.

### Validation
- full CI run
- real pilot task
- post-pilot process improvement report
- dashboard regeneration

### Exit evidence
- Новый репозиторий готов для реальной Codex-first разработки.
