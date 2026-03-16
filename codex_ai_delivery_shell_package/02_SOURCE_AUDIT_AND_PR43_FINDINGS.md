# Обзор исходного репозитория и выводы из PR #43

## 1. Что в исходном репозитории является настоящим AI delivery shell

По наблюдаемой структуре исходный репозиторий уже содержит отдельный process/governance слой поверх прикладной логики. Его признаки:

- `AGENTS.md` задаёт hot/warm/cold source-of-truth, обязательный цикл работы, PR-only main policy, FTR, worktree safety и routing skills.
- `docs/agent/*` образуют небольшой hot-context для старта работы.
- `docs/README.md` и `docs/DEV_WORKFLOW.md` собирают документацию и команды в нормативный workflow.
- `plans/items/` + `plans/PLANS.yaml` реализуют plans registry с generated compatibility output.
- `memory/{decisions,incidents,patterns}` и `memory/task_outcomes.yaml` формируют durable process state.
- `docs/agent-contexts/*` и `scripts/context_router.py` дают context-aware routing.
- `scripts/run_loop_gate.py`, `scripts/run_pr_gate.py`, `scripts/run_nightly_gate.py` реализуют многоуровневые lanes.
- `tests/` содержит отдельный process/harness test set.

## 2. Главный смысл PR #43

PR #43 не добавляет бизнес-фичу. Он **дочищает хвосты governance-модели**.  
Именно поэтому он особенно важен для extraction в новый AI-first repo.

### Из PR #43 нужно сохранить следующие решения

1. **Канонический hot-path теперь строится вокруг `run_loop_gate.py`, а не вокруг старого `run_lean_gate.py`.**  
   В новом repo нельзя возвращать legacy name или legacy flow.

2. **Task contract стал жёстче.**  
   Нельзя бесконтрольно наследоваться от archived/completed notes без явного closeout evidence.

3. **PR-only policy расширена.**  
   Policy coverage должна охватывать не только код, но и сопровождающие governance/release документы.

4. **Deprecated emergency override удалён.**  
   Старый `MOEX_CARRY_ALLOW_MAIN_PUSH=1` нельзя переносить. В новом repo нужна нейтральная и более строгая emergency-модель.

5. **Optional dependency guards — часть shell-подхода.**  
   Даже доменная история с `feedparser` важна как pattern: optional dependency не должна случайно ломать весь process layer.

6. **Task-session lifecycle доведён до конца.**  
   Begin / active note / closeout / archive / task outcomes / session handoff должны образовывать замкнутый контур.

## 3. Что это означает для нового проекта

### 3.1. Source-of-truth надо сохранить почти буквально
Hot docs, runtime rules, checks и routing — это уже готовый каркас. Его нужно адаптировать, а не перепридумывать.

### 3.2. State model надо перенести структурно, но не содержательно
В новый repo нужны:
- `plans/items/`,
- `memory/decisions`,
- `memory/incidents`,
- `memory/patterns`,
- `memory/task_outcomes.yaml`,
- `docs/tasks/{active,archive}`,
- `docs/session_handoff.md`.

Но содержимое должно стартовать с clean templates.

### 3.3. AI acceleration идёт не от “ещё одного агента”, а от трёх вещей
1. узкий hot context;  
2. surface-aware validation;  
3. durable state в репозитории.

## 4. Что нельзя тащить как есть

- доменные контексты и навыки, если они завязаны на трейдинг;
- доменные архитектурные сущности;
- исторические планы и memory-артефакты;
- obsolete wrappers;
- продуктовые scripts, не имеющие отношения к process layer.

## 5. Рекомендуемая стратегия extraction

### Стратегия A — рекомендуемая
Создать **новый репозиторий с встроенным AI delivery shell** и с placeholder модулем `Trading Advisor 3000`.  
Плюсы:
- можно сразу вычистить доменные следы;
- не нужен premature shared framework package;
- проще проверить shell end-to-end.

### Стратегия B — не первоочередная
Сначала попытаться вынести shell в отдельную shared library/monorepo package.  
Минусы:
- раньше времени абстрагирует живой процесс;
- повышает риск сломать hot path;
- усложняет Codex-first migration.

## 6. Решение

Для первого цикла миграции использовать **Стратегию A**:  
собрать новый standalone repo, где AI delivery shell живёт в корне репозитория, а приложение — отдельным модулем `Trading Advisor 3000`.
