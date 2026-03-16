# План переноса для Codex

## 1. Общая стратегия

Codex должен переносить shell **не как “массовое копирование файлов”**, а как серию маленьких, проверяемых patch sets.

Порядок правильный такой:

1. Root governance + hot docs.
2. Session lifecycle и task artifacts.
3. Context routing.
4. Gates + validators.
5. Durable state + telemetry + reports.
6. Skills governance.
7. Architecture docs и placeholder application module.
8. CI and pilot run.

## 2. Главные правила для Codex

1. Не восстанавливать `run_lean_gate.py`.
2. Не переносить доменную логику вместе с shell.
3. Не переносить historical contents из `plans/`, `memory/`, `docs/tasks/archive/`.
4. Не делать `docs/session_handoff.md` длинным narrative-файлом.
5. Не раскрывать cold context по умолчанию в `.cursorignore`.
6. Не держать канонический workflow в shell wrappers — только в Python entrypoints и docs.
7. Все high-risk changes дробить по схеме:
   `contracts → code → docs → reports`.

## 3. Patch series по фазам

### Patch set 1 — root shell scaffold
Перенести:
- `AGENTS.md`
- `docs/README.md`
- `docs/DEV_WORKFLOW.md`
- `docs/agent/*`
- `CODEOWNERS`
- `.cursorignore`
- `.githooks/pre-push`
- `scripts/install_git_hooks.py`

Цель: новый repo начинает жить по тем же governance-правилам.

### Patch set 2 — task/session loop
Перенести:
- `scripts/task_session.py`
- `docs/session_handoff.md`
- `docs/tasks/{active,archive}` templates
- `scripts/handoff_resolver.py`
- `scripts/validate_session_handoff.py`
- `scripts/validate_task_request_contract.py`
- `docs/checklists/task-request-contract.md`
- `docs/checklists/first-time-right-gate.md`

Цель: появляется воспроизводимый рабочий цикл одной задачи.

### Patch set 3 — context routing
Перенести:
- `docs/agent-contexts/*`
- `scripts/context_router.py`
- `scripts/validate_agent_contexts.py`
- нужные части `docs/agent/domains.md`

Цель: изменения маршрутизируются по surface/context, а не “на глаз”.

### Patch set 4 — gates
Перенести:
- `scripts/compute_change_surface.py`
- `scripts/gate_common.py`
- `scripts/run_loop_gate.py`
- `scripts/run_pr_gate.py`
- `scripts/run_nightly_gate.py`
- `scripts/validate_pr_only_policy.py`
- `scripts/validate_process_regressions.py`
- `scripts/nightly_root_hygiene.py`

Цель: оболочка становится самопроверяемой и быстрой.

### Patch set 5 — durable state и governance analytics
Перенести:
- `plans/items/` templates
- `plans/PLANS.yaml` generator/output
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

Цель: процесс становится измеримым.

### Patch set 6 — skills governance
Перенести:
- `.cursor/skills/` baseline subset
- `docs/agent/skills-catalog.md`
- `docs/agent/skills-routing.md`
- `docs/workflows/skill-governance-sync.md`
- `scripts/skill_update_decision.py`
- `scripts/skill_precommit_gate.py`
- `scripts/validate_skills.py`

Цель: skills становятся управляемым и валидируемым слоем, а не складом markdown.

### Patch set 7 — architecture package + placeholder app
Перенести/создать:
- `docs/architecture/*`
- `scripts/sync_architecture_map.py`
- `src/trading_advisor_3000/`
- `tests/` для crosslayer/architecture/process consistency

Цель: shell получает прикладное “место посадки”.

### Patch set 8 — CI and pilot
Создать/настроить:
- `.github/workflows/loop-gate.yml`
- `.github/workflows/pr-gate.yml`
- `.github/workflows/nightly-gate.yml`
- `.github/workflows/dashboard.yml`
- pilot task note
- first full end-to-end task execution

Цель: подтверждение, что shell работает не только локально, но и в CI.

## 4. Что переносить в последнюю очередь

- optional skills;
- stack-specific validators;
- observability/SLO extras;
- advanced dashboards;
- UI-specific checks;
- специальные connector/integration skills.

## 5. Что удалить или переименовать сразу

### Удалить как legacy
- ссылки на `run_lean_gate.py`;
- deprecated env variable `MOEX_CARRY_ALLOW_MAIN_PUSH`.

### Переименовать
- `MOEX_CARRY_EMERGENCY_MAIN_PUSH` → `AI_SHELL_EMERGENCY_MAIN_PUSH`
- `MOEX_CARRY_EMERGENCY_MAIN_PUSH_REASON` → `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON`
- `CTX-STRATEGY` → `CTX-DOMAIN`
- `CTX-NEWS` → `CTX-EXTERNAL-SOURCES`
- `docs/architecture/trading-advisor.md` → `docs/architecture/trading-advisor-3000.md`

## 6. Формат результата каждого patch set

Каждая фаза должна заканчиваться одинаковым выходом:

1. Изменённые файлы.
2. Какие gates/validators/tests запускались.
3. Что именно считается доказательством завершения.
4. Какие риски остались.
5. Нужен ли следующий patch set или rollback/rework.

## 7. Rollout policy

- Не объединять более одной крупной process-концепции в один patch set.
- Если краснеет gate — сначала чинить shell, потом продолжать перенос.
- Если контекст начинает раздуваться — фиксировать durable state и сокращать hot path, а не писать ещё один summary doc.
