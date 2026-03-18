# Phase 7 - Scale-up Readiness

## Goal
Prepare architecture and core contracts for expansion without blocking refactors:
- extension seams for new assets/providers,
- explicit runtime path for fundamentals/news context,
- extension seam for additional execution adapters,
- performance notes and prioritized backlog for next wave.

## Deliverables
- `src/trading_advisor_3000/app/execution/adapters/catalog.py`
- `src/trading_advisor_3000/app/data_plane/providers/registry.py`
- `src/trading_advisor_3000/app/runtime/context/providers.py`
- `tests/app/unit/test_phase7_execution_adapter_catalog.py`
- `tests/app/unit/test_phase7_provider_extension_seams.py`
- `docs/architecture/adr/0002-scale-up-extension-seams.md`
- `docs/architecture/app/phase7-scale-up-readiness.md`
- `docs/checklists/app/phase7-acceptance-checklist.md`

## Design Decisions
1. Adapter/provider extensibility is registry-first: core flow uses catalogs/registries, not hardcoded if-else branches.
2. Fundamentals/news path is introduced as a runtime context seam (`ContextProviderRegistry`) before model-specific decision logic.
3. Execution adapter growth uses `ExecutionAdapterCatalog` with mode support contracts (`supports_live`, `supports_paper`).
4. Scale-up readiness is accepted only with explicit non-blocking architecture path and validated seam tests.

## Performance Notes
- Current replay path is CPU-light and file-contract friendly, but not tuned for high-cardinality provider fan-out.
- Runtime context fan-out should remain bounded per signal decision window.
- Execution adapter dispatch is catalog lookup and O(1), suitable for current expansion scope.

## Performance Backlog (next wave)
1. Add bounded cache/TTL layer for fundamentals/news context requests by (`contract_id`, `as_of_ts`, `provider_id`).
2. Add provider latency/error SLO metrics by provider kind and ID.
3. Add replay benchmark fixture for multi-provider context fan-out.
4. Add adapter capability matrix contract tests when second live adapter is onboarded.

## Acceptance Commands
- `python -m pytest tests/app/unit/test_phase7_execution_adapter_catalog.py -q`
- `python -m pytest tests/app/unit/test_phase7_provider_extension_seams.py -q`
- `python -m pytest tests/app -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## Out of Scope
- onboarding real external fundamentals/news data sources,
- implementing direct additional live broker transport in production,
- performance optimization of all expansion paths in this phase.
