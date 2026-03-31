# Source evidence manifest

Ниже перечислены основные repo surfaces, которые использовались как evidence basis для приёмки и формирования ТЗ.

## Truth sources and stack governance
- `docs/architecture/app/STATUS.md`
- `docs/architecture/app/stack-conformance-baseline.md`
- `docs/architecture/app/restricted-acceptance-vocabulary.md`
- `registry/stack_conformance.yaml`
- `scripts/validate_stack_conformance.py`
- `tests/process/test_validate_stack_conformance.py`
- `docs/agent/checks.md`
- `.github/workflows/ci.yml`

## F1 and remediation control docs
- `docs/codex/contracts/stack-conformance-remediation.execution-contract.md`
- `docs/codex/modules/stack-conformance-remediation.parent.md`
- `docs/codex/modules/stack-conformance-remediation.phase-01.md`
- `docs/codex/modules/stack-conformance-remediation.phase-02.md`
- `docs/codex/modules/stack-conformance-remediation.phase-03.md`
- `docs/codex/modules/stack-conformance-remediation.phase-04.md`
- `docs/codex/modules/stack-conformance-remediation.phase-05.md`
- `docs/codex/modules/stack-conformance-remediation.phase-06.md`
- `docs/codex/modules/stack-conformance-remediation.phase-07.md`
- `docs/codex/modules/stack-conformance-remediation.phase-08.md`
- `docs/codex/modules/stack-conformance-remediation.phase-09.md`
- `docs/codex/modules/stack-conformance-remediation.phase-10.md`
- `docs/architecture/app/phase10-stack-conformance-reacceptance-report.md`
- `artifacts/acceptance/f1/red-team-review-result.md`
- `artifacts/acceptance/f1/reacceptance-evidence-pack.json`
- `artifacts/codex/orchestration/*-stack-conformance-remediation-phase-07/route-report.md`
- `artifacts/codex/orchestration/*-stack-conformance-remediation-phase-08/route-report.md`
- `artifacts/codex/orchestration/*-stack-conformance-remediation-phase-09/route-report.md`
- `artifacts/codex/orchestration/*-stack-conformance-remediation-phase-10/route-report.md`

## Product stack truth docs
- `docs/architecture/app/product-plane-spec-v2/04_ADRs.md`
- `docs/architecture/app/product-plane-spec-v2/07_Tech_Stack_and_Open_Source.md`
- `docs/architecture/app/CONTRACT_SURFACES.md`
- `docs/architecture/app/README.md`
- `docs/architecture/app/phase0-3-acceptance-verdict-2026-03-17.md`

## Phase D1 evidence
- `src/trading_advisor_3000/app/data_plane/delta_runtime.py`
- `src/trading_advisor_3000/app/data_plane/pipeline.py`
- `tests/app/integration/test_phase2a_data_plane.py`
- `tests/app/integration/test_phase2b_research_plane.py`

## Phase D2 evidence
- `src/trading_advisor_3000/spark_jobs/canonical_bars_job.py`
- `tests/app/integration/test_phase2a_spark_execution.py`
- `scripts/run_phase2a_spark_proof.py`

## Phase D3 evidence
- `src/trading_advisor_3000/dagster_defs/phase2a_assets.py`
- `scripts/run_phase2a_dagster_proof.py`
- `tests/app/integration/test_phase2a_dagster_execution.py`

## Phase R1 evidence
- `src/trading_advisor_3000/app/runtime/bootstrap.py`
- `src/trading_advisor_3000/app/runtime/signal_store/postgres.py`
- `src/trading_advisor_3000/app/interfaces/asgi.py`
- `tests/app/unit/test_phase6_runtime_durable_bootstrap.py`
- `tests/app/unit/test_phase6_fastapi_smoke.py`

## Phase R2 evidence
- `src/trading_advisor_3000/app/runtime/publishing/telegram.py`
- `src/trading_advisor_3000/app/interfaces/telegram/`
- `tests/app/unit/test_phase2c_runtime_components.py`

## Phase E1 evidence
- `deployment/stocksharp-sidecar/TradingAdvisor3000.StockSharpSidecar.sln`
- `deployment/stocksharp-sidecar/src/TradingAdvisor3000.StockSharpSidecar/TradingAdvisor3000.StockSharpSidecar.csproj`
- `deployment/stocksharp-sidecar/tests/TradingAdvisor3000.StockSharpSidecar.Tests/TradingAdvisor3000.StockSharpSidecar.Tests.csproj`
- `deployment/stocksharp-sidecar/src/TradingAdvisor3000.StockSharpSidecar/Program.cs`
- `deployment/stocksharp-sidecar/scripts/prove.ps1`
- `scripts/smoke_stocksharp_sidecar_binary.py`
- `tests/app/unit/test_phase8_dotnet_sidecar_assets.py`
- `artifacts/phase8/stocksharp-sidecar/python-smoke.json`

## Notes
- Пакет не является clone всего репозитория; он является acceptance documentation package + source evidence manifest.
- Для честной re-run приёмки нужно смотреть указанные source files в самом repo.
