# Task Note
Updated: 2026-03-16 20:33 UTC

## Goal
- Deliver: Закрыть Phase 2A data-plane MVP с исправлением замечаний ревью, тестированием и приёмкой.

## Task Request Contract
- Objective: снять блокеры по dedup-логике, canonical контракту и метрикам ingestion в Phase 2A.
- In Scope: `app/contracts`, `app/data_plane`, `spark_jobs`, `tests/app/*`, phase2a docs/checklists.
- Out of Scope: runtime/execution/research фазы 2B/2C/2D, внешние источники и production orchestration.
- Constraints: сохранить phase-aware и PR-only политику; не смешивать сервисный closeout с кодом следующей фазы.
- Done Evidence: целевые unit/integration/contract тесты + `tests/app` + loop/pr gate.
- Priority Rule: соответствие spec/контрактам и воспроизводимость приоритетнее скорости.

## Current Delta
- Устранено расхождение dedup между Python и Spark: выбор дубликата теперь по максимальному `ts_close`.
- Выравнен shape `canonical.bars` с product spec: добавлены `instrument_id`, единый `ts`, `open_interest`.
- Исправлена семантика ingestion-метрик: отчёт теперь разделяет `source_rows` и `whitelisted_rows`.
- Добавлен регрессионный unit-тест на dedup и обновлены интеграционные/manifest/quality тесты.
- Исправления зафиксированы кодовым коммитом: `1bc3d07`.

## First-Time-Right Report
1. Confirmed coverage: закрыты все найденные блокеры ревью и проверены тестами.
2. Missing or risky scenarios: Phase 2B/2C/2D в этой задаче не выполнялись и требуют отдельного task note.
3. Resource/time risks and chosen controls: сервисный closeout вынесен в отдельный коммит, чтобы не смешивать с feature-изменениями.
4. Highest-priority fixes or follow-ups: перейти к фазе 2B с отдельным task lifecycle.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: фиксировать минимальный failing case, затем исправлять контрактно-согласованный слой.
- New Search Space: registry/contracts -> ingestion/canonical -> tests/docs.
- Next Probe: `python -m pytest tests/app/unit/test_phase2a_builder.py -q`.

## Task Outcome
- Outcome Status: completed
- Decision Quality: correct_after_replan
- Final Contexts: CTX-OPS, APP-PLACEHOLDER, CTX-CONTRACTS
- Route Match: matched
- Primary Rework Cause: requirements_gap
- Incident Signature: none
- Improvement Action: test
- Improvement Artifact: tests/app/unit/test_phase2a_builder.py

## Blockers
- No blocker.

## Next Step
- Start Phase 2B on separate task note and commit series.

## Validation
- `python -m pytest tests/app/contracts -q` (13 passed)
- `python -m pytest tests/app/integration/test_phase2a_data_plane.py -q` (2 passed)
- `python -m pytest tests/app/unit/test_phase2a_builder.py -q` (1 passed)
- `python -m pytest tests/app/unit/test_phase2a_quality.py -q` (2 passed)
- `python -m pytest tests/app/unit/test_phase2a_manifests.py -q` (3 passed)
- `python -m pytest tests/app -q` (28 passed)
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD` (OK)
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check` (OK)
