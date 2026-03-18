# 00. AI Shell Alignment

## 1. Что нужно считать исходным состоянием репозитория

Репозиторий уже является **AI delivery shell**, а не пустым product repo.
Следовательно, разработка приложения должна идти как **расширение application plane**, а не как переоснование проекта.

### Уже существующие control-plane сущности
- root `AGENTS.md`
- `docs/agent/*` как hot docs
- `docs/DEV_WORKFLOW.md`
- task lifecycle через `scripts/task_session.py`
- loop / PR / nightly gates
- durable plans and memory
- local `.cursor/skills/*`
- `src/trading_advisor_3000/*` как placeholder application plane

## 2. Что добавляет PR #1 как целевое shell-hardening состояние

ТЗ должно учитывать следующие shell допущения как **обязательные или целевые**:
- split CI lanes: `loop-lane`, `pr-lane`, `nightly-lane`, `dashboard-refresh`;
- расширенная QA matrix для `tests/process/*` и `tests/architecture/*`;
- explicit high-risk context routing через `CTX-CONTRACTS`;
- значимые пути и high-risk paths в context coverage;
- Phase 5–7 shell artifacts:
  - dev loop metrics,
  - governance dashboard,
  - skill governance automation,
  - architecture v2 package.

## 3. Практический вывод для product development

### 3.1 Shell остаётся control plane
Product не должен:
- переименовывать canonical shell scripts;
- подменять hot docs;
- тащить торговую логику в shell layer.

### 3.2 Product — это application plane
Product разрабатывается в изолированных путях:
- `src/trading_advisor_3000/*`
- `tests/app/*`
- `docs/architecture/app/*`
- `docs/runbooks/app/*`
- `deployment/*`

### 3.3 Root `configs/*` — не default location для app config
Поскольку `configs/*` уже относятся к high-risk shell contracts, product runtime config следует по умолчанию хранить:
- либо в `src/trading_advisor_3000/config/*`,
- либо в `deployment/*`,
- либо в environment/secrets manager.

### 3.4 `docs/architecture/*` нужно разделить на shell и app
Рекомендуемая структура:
- `docs/architecture/shell/*` — уже существующий governance architecture package;
- `docs/architecture/app/*` — архитектура платформы сигналов;
- `docs/architecture/shared/*` — только при явной необходимости.

### 3.5 `APP-PLACEHOLDER` — текущая shell surface для product code
До тех пор пока shell surface taxonomy не будет расширена, продуктовые изменения внутри `src/trading_advisor_3000/*` и `tests/app/*` фактически проходят через существующий surface `APP-PLACEHOLDER`.
Это допустимо на старте.
После стабилизации Phase 1 можно вынести отдельный governance patch на более точную product surface taxonomy:
- `APP-DATA`
- `APP-RESEARCH`
- `APP-RUNTIME`
- `APP-EXECUTION`

## 4. Предлагаемая стратегия внедрения

### Вариант A — PR #1 merged first
Предпочтительный путь:
1. merge/backport PR #1;
2. зафиксировать shell hardening;
3. стартовать product phases.

### Вариант B — product work parallel to PR #1
Допустимо только если:
- shell-sensitive changes не смешиваются с app changes;
- команды используют те же acceptance assumptions, что описаны в PR;
- Phase 0 явно фиксирует этот разрыв и закрывает его отдельным patch set.

## 5. Фаза 0 как обязательный мост

Перед активной продуктовой реализацией необходимо:
- подтвердить наличие hot docs и canonical runtime entrypoints;
- подтвердить loop/pr/nightly gate flow;
- подтвердить high-risk routing policy;
- подтвердить структуру `tests/process`, `tests/architecture`, `tests/app`;
- закрепить, где именно будут жить product docs, configs, skills и runbooks.

## 6. Что должен сделать Codex

1. Прочитать shell hot docs.
2. Определить, shell-only, product-only или mixed patch нужен.
3. Если patch mixed — разбить его.
4. Начать task session.
5. Работать только внутри активной фазы.
6. Перед PR прогнать shell gate и product-specific checks.
