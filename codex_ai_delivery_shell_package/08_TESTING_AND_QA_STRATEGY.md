# Стратегия тестирования и QA для AI Delivery Shell

## 1. Цель тестового слоя

Тесты здесь нужны не только для кода, но и для **процесса**.  
Надо доказать, что:

- маршрутизация по surface работает корректно;
- gates запускают правильные проверки;
- task lifecycle не теряет состояние;
- docs и validators не расходятся;
- shell не деградирует при расширении repo.

## 2. Пирамида тестов

### 2.1. Fast unit/contract tests
Должны покрывать:
- `compute_change_surface.py`
- `context_router.py`
- task contract parsing/validation
- session handoff resolution
- validator helpers
- skill sync logic

Эти тесты обязаны входить в loop gate.

### 2.2. Process integration tests
Должны покрывать:
- `task_session begin/status/end`
- loop/pr gate assembly
- consistency between docs and scripts
- generated artifacts (`PLANS.yaml`, dashboard JSON/markdown outputs)

Эти тесты должны входить в PR gate.

### 2.3. Nightly hygiene tests
Должны покрывать:
- docs hygiene
- ownership coverage
- root layout policy
- flaky policy
- quality scorecards / dashboards
- architecture map sync
- crosslayer/boundary scans

Эти тесты могут быть исключены из hot loop, но обязаны выполняться хотя бы nightly.

## 3. Рекомендуемая структура тестов

```text
tests/
├─ process/
│  ├─ test_compute_change_surface.py
│  ├─ test_context_router.py
│  ├─ test_gate_scope_routing.py
│  ├─ test_harness_contracts.py
│  ├─ test_runtime_harness.py
│  ├─ test_measure_dev_loop.py
│  ├─ test_agent_process_telemetry.py
│  ├─ test_process_reports.py
│  ├─ test_nightly_root_hygiene.py
│  ├─ test_validate_plans_contract.py
│  └─ test_validate_task_request_contract.py
├─ architecture/
│  ├─ test_context_coverage.py
│  ├─ test_codeowners_coverage.py
│  └─ test_crosslayer_boundaries.py
└─ app/
   └─ (placeholder tests for Trading Advisor 3000)
```

## 4. Карта тестов к функциям shell

| Что проверяем | Файлы / функции | Где гоняем |
| --- | --- | --- |
| Surface detection | `compute_change_surface.py` | loop + PR |
| Context routing | `context_router.py`, `docs/agent-contexts/*` | loop + PR |
| Gate assembly | `gate_common.py`, `run_loop_gate.py`, `run_pr_gate.py` | PR |
| Session lifecycle | `task_session.py`, `handoff_resolver.py` | PR |
| Task contract | `validate_task_request_contract.py` | loop + PR |
| Plans schema | `validate_plans.py` | PR + nightly |
| Memory/outcomes | `validate_agent_memory.py`, `validate_task_outcomes.py` | PR + nightly |
| Skill sync | `skill_update_decision.py`, `validate_skills.py` | PR |
| Root hygiene | `nightly_root_hygiene.py` | nightly |
| Reporting | telemetry/report builders | PR + nightly |
| Crosslayer rules | architecture validators | nightly |

## 5. Важные QA-принципы

### 5.1. Docs are test inputs
Если документы являются source-of-truth, они должны участвовать в тестах и валидаторах, а не существовать отдельно.

### 5.2. Scope-aware faster than full-suite
Не все тесты должны гоняться на каждый diff.  
Главная цель AI shell — ускорение, поэтому важна **правильная селекция**.

### 5.3. Flaky tests — это defect, а не шум
Политика:
- без silent ignore;
- quarantine с metadata;
- bounded retries;
- обязательный follow-up.

### 5.4. Optional dependency failures не должны ронять весь shell
Из PR #43 нужно перенести именно этот паттерн.  
Если какая-то интеграционная или вспомогательная зависимость опциональна, process layer обязан:
- ясно это сообщать;
- пропускать соответствующий feature-path корректно;
- не ломать основной governance loop.

## 6. Минимальный smoke pack для Codex

После каждого серьёзного patch set Codex должен уметь прогнать минимум:

```bash
pytest tests/process/test_compute_change_surface.py
pytest tests/process/test_context_router.py
pytest tests/process/test_gate_scope_routing.py
pytest tests/process/test_harness_contracts.py
pytest tests/process/test_validate_task_request_contract.py
```

## 7. Acceptance pack перед первым реальным использованием

Перед стартом реальной разработки в новом repo должен существовать acceptance pack:

1. begin/status/end demo task;
2. docs-only diff route;
3. process-only diff route;
4. contract/high-risk diff route;
5. successful PR gate;
6. nightly gate green;
7. dashboard/report artifact generated.

## 8. Метрики качества shell

Нужно измерять хотя бы:

- median time from task begin to green loop gate;
- median time from last code change to green PR gate;
- число ручных bypass/override событий;
- доля task notes с complete done evidence;
- доля path coverage context cards;
- количество повторяющихся process failures;
- доля flaky tests in quarantine.
