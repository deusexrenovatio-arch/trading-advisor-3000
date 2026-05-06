# Codex package — Trading Advisor 3000 product-plane specification

> Historical / target-shape specification.
>
> This package describes intended product-plane shape and older package-intake
> direction. It is not proof that a capability is implemented now. For current
> product reality, read `docs/project-map/current-truth-map-2026-05-05.md` and
> `docs/project-map/product-reset-audit-2026-05-05.md` first.

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
- repo-local product-plane skills в `.codex/skills/*`;
- application-plane package `src/trading_advisor_3000/*`.

PR `#1` дополнительно описывает целевое усиление shell:
- split CI lanes: `loop`, `pr`, `nightly`, `dashboard-refresh`;
- расширенную QA matrix;
- high-risk context routing через `CTX-CONTRACTS`;
- shell late-stage delivery artifacts: metrics/dashboard, skill governance automation, architecture v2 package.

## Как пользоваться этим пакетом

1. Сначала читать **репозиторий**, а не этот пакет:
   - root `AGENTS.md`
   - `docs/agent/entrypoint.md`
   - `docs/agent/domains.md`
   - `docs/agent/checks.md`
   - `docs/agent/runtime.md`
   - `docs/DEV_WORKFLOW.md`
2. Затем читать этот пакет:
   - `docs/archive/product-plane-spec-v2/2026-05-06/TECHNICAL_REQUIREMENTS.md`
   - `docs/archive/product-plane-spec-v2/2026-05-06/00_AI_Shell_Alignment.md`
   - `docs/archive/product-plane-spec-v2/2026-05-06/01_Architecture_Overview.md`
   - `docs/archive/product-plane-spec-v2/2026-05-06/02_Repository_Structure.md`
   - `docs/archive/product-plane-spec-v2/2026-05-06/06_Capability_Slices_and_Acceptance_Gates.md`
3. Работать фазами и не смешивать shell changes с product changes в одном patch set.

## Состав пакета

- `docs/archive/product-plane-spec-v2/2026-05-06/TECHNICAL_REQUIREMENTS.md` — мастер-документ
- `AGENTS.md` — предложенная product-plane надстройка над root AGENTS
- `docs/archive/product-plane-spec-v2/2026-05-06/00_AI_Shell_Alignment.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/01_Architecture_Overview.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/02_Repository_Structure.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/03_Data_Model_and_Flows.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/04_ADRs.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/05_Modules_DoD_and_Parallelization.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/06_Capability_Slices_and_Acceptance_Gates.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/07_Tech_Stack_and_Open_Source.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/08_Codex_AI_Shell_Integration.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/09_Constraints_Risks_and_NFR.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/10_MCP_Deployment_Request.md`

## Базовая идея

- **Control plane** уже существует и управляет процессом разработки.
- Этот пакет добавляет **Application plane**: data, research, runtime, execution.
- Product code не должен ломать shell contracts.
- Product configs по умолчанию **не кладутся** в root `configs/`, если это не изменение shell policy.
