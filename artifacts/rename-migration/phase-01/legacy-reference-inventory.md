# Dual-Surface Rename Phase 1 Inventory

Generated: 2026-04-02T11:51:01.808232Z

## Scope Notes
- Source: tracked files from `git ls-files`.
- Legacy tokens: `docs/architecture/app/`, `tests/app/`, `src/trading_advisor_3000/app/`, `trading_advisor_3000.app`.
- Excluded historical prefixes are counted separately and not assigned as active migration workload.

## Totals

| Metric | Value |
| --- | ---: |
| Total matches | 1794 |
| Active matches | 960 |
| Excluded historical matches | 834 |

## Active Matches By Risk

| Risk | Count |
| --- | ---: |
| high | 204 |
| low | 454 |
| medium | 302 |

## Active Matches By Wave

| Wave | Count |
| --- | ---: |
| phase-02-compatibility-bridge | 133 |
| phase-03-docs-subtree-rename | 454 |
| phase-04-runtime-test-cutover | 371 |
| phase-05-governance-selector-cutover | 2 |

## Wave Clusters

| Cluster | Group | Risk | Wave | Owner | Active refs | Files |
| --- | --- | --- | --- | --- | ---: | ---: |
| documentation-linkage | docs-links | low | phase-03-docs-subtree-rename | architecture | 316 | 189 |
| runtime-namespace-dependencies | imports-code | high | phase-04-runtime-test-cutover | app-core+platform | 187 | 75 |
| test-namespace-dependencies | test-paths-fixtures | medium | phase-04-runtime-test-cutover | app-core+platform | 184 | 181 |
| docs-architecture-linkage | docs-links | low | phase-03-docs-subtree-rename | architecture | 138 | 37 |
| runbook-operational-references | packaging-runbook | medium | phase-02-compatibility-bridge | architecture+platform | 67 | 18 |
| script-path-references | scripts-validators | medium | phase-02-compatibility-bridge | platform | 48 | 3 |
| validator-and-gate-paths | scripts-validators | high | phase-02-compatibility-bridge | platform | 15 | 4 |
| packaging-and-release-references | packaging-runbook | medium | phase-02-compatibility-bridge | platform | 3 | 3 |
| governance-routing-selectors | ci-and-codeowners | high | phase-05-governance-selector-cutover | process+platform | 2 | 1 |

## Active Reference Sample (Top 80)

| File | Line | Token | Group | Risk | Wave |
| --- | ---: | --- | --- | --- | --- |
| `AGENTS.md` | 40 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `AGENTS.md` | 41 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `CODEOWNERS` | 33 | `tests/app/` | ci-and-codeowners | high | phase-05-governance-selector-cutover |
| `CODEOWNERS` | 34 | `docs/architecture/app/` | ci-and-codeowners | high | phase-05-governance-selector-cutover |
| `README.md` | 12 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `README.md` | 12 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `README.md` | 19 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `configs/change_surface_mapping.yaml` | 37 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 38 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 39 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 40 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 41 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 42 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 43 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 44 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 45 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 46 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 47 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 48 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 49 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 50 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 57 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 58 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 61 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 62 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 63 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 64 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/change_surface_mapping.yaml` | 70 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/critical_contours.yaml` | 7 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/critical_contours.yaml` | 8 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/critical_contours.yaml` | 9 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/critical_contours.yaml` | 13 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/critical_contours.yaml` | 34 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/critical_contours.yaml` | 35 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/critical_contours.yaml` | 36 | `src/trading_advisor_3000/app/` | imports-code | high | phase-04-runtime-test-cutover |
| `configs/critical_contours.yaml` | 37 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `configs/critical_contours.yaml` | 38 | `tests/app/` | test-paths-fixtures | medium | phase-04-runtime-test-cutover |
| `deployment/docker/production-like/docker-compose.production-like.yml` | 8 | `trading_advisor_3000.app` | packaging-runbook | medium | phase-02-compatibility-bridge |
| `deployment/docker/staging-gateway/docker-compose.staging-gateway.yml` | 8 | `trading_advisor_3000.app` | packaging-runbook | medium | phase-02-compatibility-bridge |
| `deployment/stocksharp-sidecar/README.md` | 23 | `docs/architecture/app/` | packaging-runbook | medium | phase-02-compatibility-bridge |
| `docs/DEV_WORKFLOW.md` | 16 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/README.md` | 20 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/README.md` | 34 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-API-UI.md` | 7 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-API-UI.md` | 8 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-API-UI.md` | 9 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-API-UI.md` | 10 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-API-UI.md` | 13 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-API-UI.md` | 14 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-API-UI.md` | 15 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-API-UI.md` | 19 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 7 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 9 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 10 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 11 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 12 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 13 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 16 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 17 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 18 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DATA.md` | 22 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DOMAIN.md` | 7 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DOMAIN.md` | 8 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-DOMAIN.md` | 20 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 9 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 10 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 12 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 13 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 14 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 15 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 16 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 19 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 20 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 21 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-ORCHESTRATION.md` | 25 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-RESEARCH.md` | 7 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-RESEARCH.md` | 10 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-RESEARCH.md` | 11 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-RESEARCH.md` | 12 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/agent-contexts/CTX-RESEARCH.md` | 15 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |

## Machine-Readable Artifact
- JSON: `C:/Users/Admin/.codex/worktrees/b4ee-phase1/artifacts/rename-migration/phase-01/legacy-reference-inventory.json`
