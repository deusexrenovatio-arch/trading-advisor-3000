# Инвентаризация файлов слоя AI Delivery Shell

Ниже перечислены файлы и каталоги, которые по структуре репозитория, hot docs, runbooks, tests и PR #43 относятся к **методологическому / governance / AI-delivery слою**.

> Важно: это не инвентаризация всего репозитория.  
> Доменные торговые и исследовательские скрипты сознательно исключены.

## Легенда решений переноса

- `ADAPT_CORE` — переносить в первую волну, адаптируя имена и ссылки.
- `ADAPT_TEMPLATE` — переносить как шаблон/форму, содержимое перегенерировать.
- `ADAPT_REWRITE` — сохранить структуру документа, но переписать содержание почти полностью.
- `ADAPT_RENAME` — перенести с переименованием для снятия доменной привязки.
- `KEEP_STRUCTURE` — перенести только layout/templates/schema.
- `KEEP_GENERATED` — сохранять как generated artifact, не редактируемый вручную.
- `TEMPLATE_ONLY` — оставить пустой seed/template, не переносить исторические данные.
- `ADAPT_OPTIONAL` — переносить после стабилизации ядра.
- `DEPRECATE_OR_THIN_ALIAS` — не делать каноническим, при необходимости оставить тонкий alias.
- `EXCLUDE_DOMAIN_KEEP_PATTERN` — исходный файл не переносить, но сохранить абстрактный паттерн.

## Root governance

| Source file / path | Назначение | Решение переноса | Target path | Что сделать | Source signal |
| --- | --- | --- | --- | --- | --- |
| AGENTS.md | Главная инструкция для AI-агента: hot/warm/cold source-of-truth, не-negotiable loop, PR-only policy, skills routing, first-time-right. | ADAPT_CORE | AGENTS.md | Перенести почти целиком как ядро shell, убрать доменные упоминания и переименовать emergency env vars в нейтральные. | repo root + raw file |
| agent-runbook.md | Операционный runbook для параллельных AI-сессий, нагрузочного режима и worktree discipline. | ADAPT_CORE | agent-runbook.md | Сохранить правила high-load/parallel-agent работы. Привязать к новому репозиторию и новым портам/процессам. | repo root + raw file |
| harness-guideline.md | Карта принцип → проверка → владелец → CI job. Определяет архитектуру governance-harness. | ADAPT_CORE | harness-guideline.md | Перенести как нормативный документ для Codex. Обновить названия lane/job и target repo paths. | repo root + raw file |
| CODEOWNERS | Маршрутизация владения по зонам repo: governance, docs, plans, memory, scripts, tests. | ADAPT_CORE | CODEOWNERS | Сохранить принцип ownership-by-surface; заменить команды/aliases на владельцев нового проекта. | repo root + raw file |
| .cursorignore | Контроль hot-context: cold governance и state исключены из индексации, docs/agent/** остаются доступными. | ADAPT_CORE | .cursorignore | Критично сохранить. Не разрешать весь docs/ и state по умолчанию. Оставить hot docs узкими. | repo root + PR diff + raw file |
| .githooks/pre-push | Локальный предохранитель: запрет прямого push в main, emergency override с reason, scoped loop gate. | ADAPT_CORE | .githooks/pre-push | Перенести как обязательный контроль. Переименовать MOEX-specific env vars в AI_SHELL_*. | raw file |

## Core docs / hot source of truth

| Source file / path | Назначение | Решение переноса | Target path | Что сделать | Source signal |
| --- | --- | --- | --- | --- | --- |
| docs/README.md | Индекс документации agent-first engineering и список основных команд/валидаторов. | ADAPT_CORE | docs/README.md | Сделать входной индекс для Codex и людей. Сохранить карту документации и lanes. | raw file |
| docs/DEV_WORKFLOW.md | Нормативный workflow: preflight, skill governance, FTR, task contract, loop/pr/nightly gates. | ADAPT_CORE | docs/DEV_WORKFLOW.md | Один из ключевых документов. В новой версии убрать любые workaround-пути и legacy wrapper flow. | raw file |
| docs/session_handoff.md | Лёгкий pointer-shim на активную task note, mode/status и базовые validation commands. | ADAPT_CORE | docs/session_handoff.md | Сохранить именно как pointer shim, а не как длинный handoff-документ. | raw file |
| docs/agent/entrypoint.md | Hot entrypoint: что читать первым, как эскалировать context, в каком порядке выполнять цикл. | ADAPT_CORE | docs/agent/entrypoint.md | Перенести почти как есть с адаптацией ссылок и нейтральных именований. | raw file |
| docs/agent/domains.md | Карта bounded contexts / execution domains и правила разделения change surface. | ADAPT_CORE | docs/agent/domains.md | Адаптировать под новые context cards. Обязательно сохранить правило split series: contracts → code → docs. | raw file |
| docs/agent/checks.md | Матрица loop hot-path, PR closeout, nightly/cold hygiene и first-time-right checks. | ADAPT_CORE | docs/agent/checks.md | Перенести как сводную карту проверок и источника truth для gate assembly. | raw file |
| docs/agent/runtime.md | Набор канонических Python entrypoints и правило «no wrapper reintroduction». | ADAPT_CORE | docs/agent/runtime.md | Сохранить one-Python-contract подход. Никаких shell-only wrappers как основной путь. | raw file |
| docs/agent/skills-routing.md | Когда и как подтягивать skills, mandatory flow order и правила обновления skills. | ADAPT_CORE | docs/agent/skills-routing.md | Сохранить как governance for capabilities; доменные навыки подключать позже. | raw file |
| docs/agent/skills-catalog.md | Каталог skills в `.cursor/skills`, используемый как mirror/navigation слой. | ADAPT_TEMPLATE | docs/agent/skills-catalog.md | Перегенерировать после переноса core skills. Не копировать слепо доменные skills в первый этап. | raw file |

## Checklists / planning / workflows / runbooks

| Source file / path | Назначение | Решение переноса | Target path | Что сделать | Source signal |
| --- | --- | --- | --- | --- | --- |
| docs/checklists/first-time-right-gate.md | Правила first-time-right, budget/stop-replan, predictive needs, escalation on repeats. | ADAPT_CORE | docs/checklists/first-time-right-gate.md | Обязательный процессный слой. Сохранить без доменной привязки. | raw file |
| docs/checklists/task-request-contract.md | Структура запроса: objective, scope, constraints, done evidence, priority, repetition control. | ADAPT_CORE | docs/checklists/task-request-contract.md | Ключевой документ для Codex. Поле done evidence должно быть обязательным. | raw file |
| docs/planning/plans-registry.md | Описание канонического registry: `plans/items/` как source of truth, `PLANS.yaml` как generated compatibility output. | ADAPT_CORE | docs/planning/plans-registry.md | Сохранить схему и инварианты. Не переносить старое содержимое планов. | raw file |
| docs/workflows/context-budget.md | Как хранить durable state в repo артефактах и держать handoff lean. | ADAPT_CORE | docs/workflows/context-budget.md | Очень важен для чисто-AI среды. Сохранить правило «state lives in artifacts, not in chat memory». | raw file |
| docs/workflows/skill-governance-sync.md | Синхронизация local `.cursor/skills`, global mirror catalog и precommit guard. | ADAPT_CORE | docs/workflows/skill-governance-sync.md | Нужен, если skills становятся частью delivery platform. Генерировать каталог из локального источника. | raw file |
| docs/workflows/agent-practices-alignment.md | Зафиксированные AI-practices: request contract, FTR report, repetition control; интеграция в loop gate. | ADAPT_CORE | docs/workflows/agent-practices-alignment.md | Сохранить как rationale и policy layer. | raw file |
| docs/workflows/worktree-governance.md | Task session lock, multi-worktree etiquette, dedicated ports/process isolation. | ADAPT_CORE | docs/workflows/worktree-governance.md | Сохранить. Особенно важно для parallel Codex/Cursor/CLI usage. | raw file |
| docs/runbooks/governance-remediation.md | Runbook для разруливания сбоев валидаторов/gates и acknowledged debt rules. | ADAPT_CORE | docs/runbooks/governance-remediation.md | Полезен для практического внедрения: что делать, когда gate красный. | raw file |
| docs/runbooks/flaky-tests-policy.md | Политика по flaky tests: никакого тихого ignore, quarantine metadata, bounded retries. | ADAPT_CORE | docs/runbooks/flaky-tests-policy.md | Перенести без изменений по сути; связать с nightly/pr lanes. | raw file |

## Agent contexts

| Source file / path | Назначение | Решение переноса | Target path | Что сделать | Source signal |
| --- | --- | --- | --- | --- | --- |
| docs/agent-contexts/README.md | Индекс context cards и правил маршрутизации через `context_router.py`. | ADAPT_CORE | docs/agent-contexts/README.md | Сохранить. Обновить набор context cards под новую архитектуру. | raw file |
| docs/agent-contexts/CTX-DATA.md | Контекст для data surface: owned paths, guarded paths, minimum checks. | ADAPT_CORE | docs/agent-contexts/CTX-DATA.md | Оставить как generic data context. | raw file |
| docs/agent-contexts/CTX-STRATEGY.md | Контекст для strategy/domain logic. | ADAPT_RENAME | docs/agent-contexts/CTX-DOMAIN.md | Рекомендуем переименовать в нейтральный доменный контекст. Содержимое переписать без трейдинговых деталей. | raw file |
| docs/agent-contexts/CTX-RESEARCH.md | Контекст для research/experiments/analysis surfaces. | ADAPT_CORE | docs/agent-contexts/CTX-RESEARCH.md | Сохранить как есть по структуре. | raw file |
| docs/agent-contexts/CTX-NEWS.md | Контекст для news/external intelligence surfaces. | ADAPT_RENAME | docs/agent-contexts/CTX-EXTERNAL-SOURCES.md | Вынести в нейтральный контекст внешних источников/feeds/integrations, если продукт потом действительно будет это использовать. | raw file |
| docs/agent-contexts/CTX-ORCHESTRATION.md | Контекст для runtime orchestration, pipelines, workflow assembly. | ADAPT_CORE | docs/agent-contexts/CTX-ORCHESTRATION.md | Ключевой контекст delivery shell; сохранить без доменной привязки. | raw file |
| docs/agent-contexts/CTX-API-UI.md | Контекст для API/UI boundary. | ADAPT_CORE | docs/agent-contexts/CTX-API-UI.md | Сохранить, даже если приложение пока placeholder. | raw file |
| docs/agent-contexts/CTX-CONTRACTS.md | Высокорисковый контекст для schemas/contracts/migrations; требует ordered patch series. | ADAPT_CORE | docs/agent-contexts/CTX-CONTRACTS.md | Критично сохранить high-risk правила и split policy. | raw file |
| docs/agent-contexts/CTX-OPS.md | Контекст для CI/CD, deploy, observability, infra. | ADAPT_CORE | docs/agent-contexts/CTX-OPS.md | Оставить как есть по структуре. | raw file |

## Architecture

| Source file / path | Назначение | Решение переноса | Target path | Что сделать | Source signal |
| --- | --- | --- | --- | --- | --- |
| docs/architecture/trading-advisor.md | Архитектурный narrative документ: цели, stack policy, boundaries, deterministic vs agentic steps. | ADAPT_REWRITE | docs/architecture/trading-advisor-3000.md | Сохранить структуру документа, но переписать содержимое под placeholder-приложение без доменной детализации. | raw file |
| docs/architecture/layers-v2.md | Карта прикладных слоёв и boundary rules. | ADAPT_REWRITE | docs/architecture/layers-v2.md | Сохранить идею layered map, но полностью нейтрализовать доменные слои. | raw file |
| docs/architecture/entities-v2.md | Каноническая entity model. | ADAPT_TEMPLATE | docs/architecture/entities-v2.md | Перенести только форму/шаблон и правила описания сущностей. Не переносить trading entities. | raw file |
| docs/architecture/architecture-map-v2.md | Source-of-truth map для визуализации архитектуры и процедуры синхронизации. | ADAPT_CORE | docs/architecture/architecture-map-v2.md | Сохранить как механизм поддержания архитектуры в актуальном состоянии. | raw file |

## Durable state & task artifacts

| Source file / path | Назначение | Решение переноса | Target path | Что сделать | Source signal |
| --- | --- | --- | --- | --- | --- |
| plans/items/ | Канонический registry планов/эпиков/задач. | KEEP_STRUCTURE | plans/items/ | Перенести схему, шаблоны и naming rules. Не копировать исторические items. | repo tree + docs |
| plans/PLANS.yaml | Generated compatibility output поверх `plans/items/`. | KEEP_GENERATED | plans/PLANS.yaml | Создавать генератором. Не редактировать руками. | repo tree + docs |
| memory/agent_memory.yaml | Сводный state/ledger агентной памяти. | TEMPLATE_ONLY | memory/agent_memory.yaml | Не переносить историческое содержимое. Оставить пустой шаблон или seed. | repo tree |
| memory/task_outcomes.yaml | Ledger результатов задач и closeout evidence. | KEEP_STRUCTURE | memory/task_outcomes.yaml | Перенести схему и автоматическое обновление при end/closeout. | repo tree + PR summary |
| memory/decisions/ | ADR-like durable decisions. | KEEP_STRUCTURE | memory/decisions/ | Схема и conventions нужны, содержимое — только если decision truly generic. | repo tree |
| memory/incidents/ | Журнал process/production incidents. | KEEP_STRUCTURE | memory/incidents/ | Сохраняем структуру и templates. | repo tree |
| memory/patterns/ | Повторно используемые process/code patterns. | KEEP_STRUCTURE | memory/patterns/ | Это одна из главных целей extraction: reusable patterns без доменной истории. | repo tree |
| docs/tasks/active/ | Активные task notes для текущей сессии/задачи. | KEEP_STRUCTURE | docs/tasks/active/ | Нужен template task note и index rules. | docs/workflows + PR summary |
| docs/tasks/archive/ | Архив task notes, на которые нельзя наследоваться без closeout evidence. | KEEP_STRUCTURE | docs/tasks/archive/ | Сохранить policy against archived note inheritance. | PR summary |

## Process scripts / entrypoints

| Source file / path | Назначение | Решение переноса | Target path | Что сделать | Source signal |
| --- | --- | --- | --- | --- | --- |
| scripts/task_session.py | Session lifecycle: begin/status/end, lock, closeout, active task pointer maintenance. | ADAPT_CORE | scripts/task_session.py | Один из центральных файлов. Переносить одним из первых. | docs |
| scripts/compute_change_surface.py | Определение change surface и маршрутизация gates/tests по затронутым путям. | ADAPT_CORE | scripts/compute_change_surface.py | Ключ к scoped validation и ускорению AI-разработки. | repo tree + tests |
| scripts/context_router.py | Маршрутизатор context cards/контекстов на основе surface change. | ADAPT_CORE | scripts/context_router.py | Нужно сохранить связку с `docs/agent-contexts/*`. | repo tree + raw file |
| scripts/handoff_resolver.py | Разрешение указателя session handoff → active task note. | ADAPT_CORE | scripts/handoff_resolver.py | Переносить вместе с `task_session.py` и `docs/session_handoff.md`. | repo tree |
| scripts/gate_common.py | Общая логика исполнения gate-команд, агрегация результатов, surface routing. | ADAPT_CORE | scripts/gate_common.py | Нужно сохранить как ядро для loop/pr/nightly gate family. | repo tree |
| scripts/run_loop_gate.py | Lean hot-path gate для локального цикла разработки. | ADAPT_CORE | scripts/run_loop_gate.py | Канонический быстрый gate. Не возвращать legacy `run_lean_gate.py`. | repo tree + PR summary |
| scripts/run_pr_gate.py | PR closeout gate — superset loop gate. | ADAPT_CORE | scripts/run_pr_gate.py | Оставить как обязательный closeout path. | repo tree + docs |
| scripts/run_nightly_gate.py | Cold-hygiene / nightly validation lane. | ADAPT_CORE | scripts/run_nightly_gate.py | Сохранить для deep hygiene checks, scorecards и docs maintenance. | repo tree + docs |
| scripts/install_git_hooks.py | Установка локальных git hooks. | ADAPT_CORE | scripts/install_git_hooks.py | Переносить безусловно; нужен для fast bootstrap. | repo tree + docs |
| scripts/measure_dev_loop.py | Метрики developer/agent loop, latency и process performance. | ADAPT_CORE | scripts/measure_dev_loop.py | Ключевой инструмент, если цель — ускорение AI-разработки, а не просто набор правил. | repo tree + tests |
| scripts/agent_process_telemetry.py | Сбор process telemetry по агентным сессиям и pipeline. | ADAPT_CORE | scripts/agent_process_telemetry.py | Переносить в phase 5 вместе с dashboard/reporting. | repo tree + tests |
| scripts/agent_bootstrap.py | Начальная подготовка окружения/репозитория для AI-агента. | ADAPT_CORE | scripts/agent_bootstrap.py | Хорошо подходит для one-command repo bootstrapping. | repo tree |
| scripts/agent_review.py | Process/architecture/governance review helper. | ADAPT_OPTIONAL | scripts/agent_review.py | Полезен, но можно переносить после базового shell. | repo tree |
| scripts/agent_smoke.py | Smoketest стартового AI-path. | ADAPT_OPTIONAL | scripts/agent_smoke.py | Рекомендуется после phase 4 для контроля bootstrap path. | repo tree |
| scripts/acceptance_check.py | Acceptance / readiness helper. | ADAPT_OPTIONAL | scripts/acceptance_check.py | Можно встроить в PR/nightly lane или оставить как ручной шаг. | repo tree |
| scripts/process_improvement_report.py | Отчёт по улучшениям процесса, повторам ошибок, bottlenecks. | ADAPT_CORE | scripts/process_improvement_report.py | Переносить вместе с telemetry/memory. | repo tree + tests |
| scripts/build_governance_dashboard.py | Сбор governance dashboard по gates, validations и process KPIs. | ADAPT_CORE | scripts/build_governance_dashboard.py | Нужен для видимости эффективности shell. | repo tree |
| scripts/autonomy_kpi_report.py | KPI-отчёт по автономности AI delivery. | ADAPT_CORE | scripts/autonomy_kpi_report.py | Полезен как objective measure of acceleration. | repo tree |
| scripts/harness_baseline_metrics.py | Базовые метрики harness/quality процесса. | ADAPT_CORE | scripts/harness_baseline_metrics.py | Перенести как baseline before/after migration comparison. | repo tree |
| scripts/doc_gardening_report.py | Проверки doc hygiene и maintenance debt. | ADAPT_OPTIONAL | scripts/doc_gardening_report.py | Полезен для nightly lane. | repo tree |
| scripts/nightly_root_hygiene.py | Проверка root hygiene / repo discipline в nightly lane. | ADAPT_CORE | scripts/nightly_root_hygiene.py | Сохранить как часть cold-hygiene пакета. | repo tree + tests |
| scripts/skill_update_decision.py | Нормативное решение: надо ли обновлять skill при изменении исходников/правил. | ADAPT_CORE | scripts/skill_update_decision.py | Нужно, если skills остаются исполняемым process asset. | repo tree + raw doc |
| scripts/skill_precommit_gate.py | Защита от рассинхронизации skills до коммита. | ADAPT_CORE | scripts/skill_precommit_gate.py | Переносить в phase 6 вместе с catalog generation. | repo tree |
| scripts/sync_architecture_map.py | Синхронизация architecture map из source-of-truth docs/graph definition. | ADAPT_CORE | scripts/sync_architecture_map.py | Переносить после появления архитектурных документов. | repo tree + raw doc |
| scripts/run_dev_workflow.py | Convenience wrapper/legacy orchestration path. | DEPRECATE_OR_THIN_ALIAS | scripts/run_dev_workflow.py (optional) | Не делать каноническим. Если сохранять, то только как thin alias поверх `task_session` + gates. | repo tree |

## Validators (process-layer)

| Source file / path | Назначение | Решение переноса | Target path | Что сделать | Source signal |
| --- | --- | --- | --- | --- | --- |
| scripts/validate_plans.py | Валидация registry plans/items и compatibility output. | ADAPT_CORE | scripts/validate_plans.py | Подтверждён PR #43. Обязателен. | PR summary |
| scripts/validate_task_request_contract.py | Проверка task contract, включая done evidence и policy inheritance. | ADAPT_CORE | scripts/validate_task_request_contract.py | Подтверждён PR #43. Один из ключевых валидаторов. | PR summary |
| scripts/validate_session_handoff.py | Проверка корректности pointer shim / handoff metadata. | ADAPT_CORE | scripts/validate_session_handoff.py | Нужен для защиты от сломанной сессии. | docs |
| scripts/validate_pr_only_policy.py | Проверка запрета direct-main paths и связанных policy docs. | ADAPT_CORE | scripts/validate_pr_only_policy.py | Подтверждён PR #43. Должен покрывать release notes/policy docs. | PR summary |
| scripts/validate_skills.py | Проверка консистентности local skills и catalog/routing policy. | ADAPT_CORE | scripts/validate_skills.py | Переносить вместе со skills governance. | docs |
| scripts/validate_agent_contexts.py | Проверка context cards и routing coverage. | ADAPT_CORE | scripts/validate_agent_contexts.py | Нужен для поддержки `context_router.py`. | docs/harness |
| scripts/validate_agent_memory.py | Валидация структуры durable memory и допустимых обновлений. | ADAPT_CORE | scripts/validate_agent_memory.py | Переносить вместе с memory schema. | docs/harness |
| scripts/validate_task_outcomes.py | Проверка ledger task outcomes/closeout evidence. | ADAPT_CORE | scripts/validate_task_outcomes.py | Нужен для end-of-session completeness. | docs/harness |
| scripts/validate_process_regressions.py | Проверка регрессий process rules/harness behavior. | ADAPT_CORE | scripts/validate_process_regressions.py | Нужен для защиты shell от деградации со временем. | docs/harness |
| scripts/validate_flaky_policy.py | Проверка соблюдения flaky-tests policy. | ADAPT_CORE | scripts/validate_flaky_policy.py | Переносить, если quarantine/retry policy становится standard. | runbook/policy |
| scripts/validate_codeowners.py | Проверка полноты и актуальности CODEOWNERS покрытия. | ADAPT_CORE | scripts/validate_codeowners.py | Связать с ownership coverage для новых путей. | harness guideline |
| scripts/validate_quality_scorecards.py | Проверка scorecards/quality dashboards. | ADAPT_OPTIONAL | scripts/validate_quality_scorecards.py | Полезно после появления dashboard/report artifacts. | harness guideline |
| scripts/validate_dependency_decisions.py | Контроль ADR/решений по зависимостям и boundary violations. | ADAPT_OPTIONAL | scripts/validate_dependency_decisions.py | Рекомендуется, если новая архитектура использует ADR-ledger. | harness guideline |
| scripts/validate_python_style.py | Python style gate. | ADAPT_OPTIONAL | scripts/validate_python_style.py | Может быть заменён на ruff/black job, но название и lane можно сохранить. | docs |
| scripts/validate_structured_logging.py | Проверка политики structured logging. | ADAPT_OPTIONAL | scripts/validate_structured_logging.py | Оставить, если logging является частью architectural rules. | harness guideline |

## Tests (process/harness layer)

| Source file / path | Назначение | Решение переноса | Target path | Что сделать | Source signal |
| --- | --- | --- | --- | --- | --- |
| tests/test_compute_change_surface.py | Покрытие scoped surface detection. | ADAPT_CORE | tests/process/test_compute_change_surface.py | Обязательный тест. | repo tests |
| tests/test_context_router.py | Покрытие маршрутизации контекстов. | ADAPT_CORE | tests/process/test_context_router.py | Обязательный тест. | repo tests |
| tests/test_gate_scope_routing.py | Проверка правильной маршрутизации gate-команд по surface. | ADAPT_CORE | tests/process/test_gate_scope_routing.py | Обязательный тест. | repo tests + PR summary |
| tests/test_harness_contracts.py | Инварианты harness/contract behavior. | ADAPT_CORE | tests/process/test_harness_contracts.py | Обязательный тест. | repo tests + PR summary |
| tests/test_runtime_harness.py | Smoke/contract test runtime entrypoints. | ADAPT_CORE | tests/process/test_runtime_harness.py | Обязательный тест. | repo tests |
| tests/test_measure_dev_loop.py | Проверка метрик dev loop. | ADAPT_CORE | tests/process/test_measure_dev_loop.py | Обязательный тест. | repo tests |
| tests/test_agent_process_telemetry.py | Проверка telemetry collection/report format. | ADAPT_CORE | tests/process/test_agent_process_telemetry.py | Рекомендуется переносить вместе с reports. | repo tests |
| tests/test_process_reports.py | Проверка process dashboards/reports. | ADAPT_CORE | tests/process/test_process_reports.py | Переносить вместе с dashboard/report scripts. | repo tests |
| tests/test_nightly_root_hygiene.py | Покрытие nightly hygiene checks. | ADAPT_CORE | tests/process/test_nightly_root_hygiene.py | Обязательный nightly тест. | repo tests |
| tests/test_validate_plans_contract.py | Проверка инвариантов plans contract. | ADAPT_CORE | tests/process/test_validate_plans_contract.py | Подтверждён PR #43. | PR summary |
| tests/test_validate_task_request_contract.py | Проверка task request contract validator. | ADAPT_CORE | tests/process/test_validate_task_request_contract.py | Подтверждён PR #43. | PR summary |
| tests/test_build_news_golden_top20.py | В исходном repo это доменно-новостной тест; для нового shell он не нужен как feature test, но полезен как пример optional-dependency guard pattern. | EXCLUDE_DOMAIN_KEEP_PATTERN | n/a (replace with tests/process/test_optional_dependency_guards.py) | Не переносить как есть; вынести лишь паттерн optional dependency guard из PR #43. | PR summary |
