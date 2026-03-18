# Codex package — Trading Advisor 3000 product-plane specification

Этот пакет **не заменяет** существующий AI delivery shell в репозитории.
Он описывает, как развивать **product plane** платформы торговых сигналов
**поверх уже существующего control plane**.

## Что уже есть в репозитории

Базовый репозиторий уже содержит:
- root `AGENTS.md`;
- hot/warm/cold documentation system;
- `docs/agent/*` как горячий слой для Codex;
- task lifecycle через `scripts/task_session.py`;
- surface-aware gates: `run_loop_gate.py`, `run_pr_gate.py`, `run_nightly_gate.py`;
- durable state в `plans/*` и `memory/*`;
- локальные skills в `.cursor/skills/*`;
- placeholder application package `src/trading_advisor_3000/*`.

PR `#1` дополнительно описывает целевое усиление shell:
- split CI lanes: `loop`, `pr`, `nightly`, `dashboard-refresh`;
- расширенную QA matrix;
- high-risk context routing через `CTX-CONTRACTS`;
- Phase 5–7 shell artifacts: metrics/dashboard, skill governance automation, architecture v2 package.

## Как пользоваться этим пакетом

1. Сначала читать **репозиторий**, а не этот пакет:
   - root `AGENTS.md`
   - `docs/agent/entrypoint.md`
   - `docs/agent/domains.md`
   - `docs/agent/checks.md`
   - `docs/agent/runtime.md`
   - `docs/DEV_WORKFLOW.md`
2. Затем читать этот пакет:
   - `docs/architecture/app/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md`
   - `docs/architecture/app/product-plane-spec-v2/00_AI_Shell_Alignment.md`
   - `docs/architecture/app/product-plane-spec-v2/01_Architecture_Overview.md`
   - `docs/architecture/app/product-plane-spec-v2/02_Repository_Structure.md`
   - `docs/architecture/app/product-plane-spec-v2/06_Phases_and_Acceptance_Gates.md`
3. Работать фазами и не смешивать shell changes с product changes в одном patch set.

## Состав пакета

- `docs/architecture/app/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md` — мастер-документ
- `AGENTS.md` — предложенная product-plane надстройка над root AGENTS
- `docs/architecture/app/product-plane-spec-v2/00_AI_Shell_Alignment.md`
- `docs/architecture/app/product-plane-spec-v2/01_Architecture_Overview.md`
- `docs/architecture/app/product-plane-spec-v2/02_Repository_Structure.md`
- `docs/architecture/app/product-plane-spec-v2/03_Data_Model_and_Flows.md`
- `docs/architecture/app/product-plane-spec-v2/04_ADRs.md`
- `docs/architecture/app/product-plane-spec-v2/05_Modules_DoD_and_Parallelization.md`
- `docs/architecture/app/product-plane-spec-v2/06_Phases_and_Acceptance_Gates.md`
- `docs/architecture/app/product-plane-spec-v2/07_Tech_Stack_and_Open_Source.md`
- `docs/architecture/app/product-plane-spec-v2/08_Codex_AI_Shell_Integration.md`
- `docs/architecture/app/product-plane-spec-v2/09_Constraints_Risks_and_NFR.md`
- `docs/architecture/app/product-plane-spec-v2/10_MCP_Deployment_Request.md`

## Базовая идея

- **Control plane** уже существует и управляет процессом разработки.
- Этот пакет добавляет **Application plane**: data, research, runtime, execution.
- Product code не должен ломать shell contracts.
- Product configs по умолчанию **не кладутся** в root `configs/`, если это не изменение shell policy.

