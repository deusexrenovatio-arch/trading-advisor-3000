# Приложение B — матрица команд

| Когда | Команда | Зачем |
| --- | --- | --- |
| В начале новой задачи | `python scripts/task_session.py begin --task <task-id>` | создать/активировать lifecycle задачи |
| Перед началом правок | `python scripts/task_session.py status` | убедиться, что сессия и task note консистентны |
| После обновления task note | `python scripts/validate_task_request_contract.py --task <task-id>` | проверить полноту запроса и done evidence |
| После малого patch set | `python scripts/run_loop_gate.py --base origin/main --head HEAD` | быстрый scoped gate |
| Перед PR/merge | `python scripts/run_pr_gate.py --base origin/main --head HEAD` | полный PR closeout lane |
| По расписанию / перед крупным релизом | `python scripts/run_nightly_gate.py` | cold-hygiene и глубокие проверки |
| После смены архитектурных docs | `python scripts/sync_architecture_map.py` | обновить архитектурную карту |
| После правок skills | `python scripts/skill_update_decision.py ...` | понять, надо ли обновлять каталог/маршрутизацию |
| Перед коммитом skills | `python scripts/skill_precommit_gate.py` | не допустить рассинхронизацию skills |
| Периодически | `python scripts/measure_dev_loop.py` | измерить скорость цикла |
| Периодически | `python scripts/process_improvement_report.py` | выявить повторяющиеся process проблемы |
| Периодически | `python scripts/build_governance_dashboard.py` | собрать dashboard состояния shell |
| В конце задачи | `python scripts/task_session.py end --task <task-id>` | корректно закрыть lifecycle и обновить state |

## Минимальный цикл работы

```bash
python scripts/task_session.py begin --task demo-task
python scripts/validate_task_request_contract.py --task demo-task
python scripts/run_loop_gate.py --base origin/main --head HEAD
python scripts/run_pr_gate.py --base origin/main --head HEAD
python scripts/task_session.py end --task demo-task
```
