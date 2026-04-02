# Dual-Surface Rename Phase 1 Inventory

Generated: 2026-04-02T14:43:44.140361Z

## Scope Notes
- Source: tracked files from `git ls-files`.
- Legacy tokens: `docs/architecture/app/`, `tests/app/`, `src/trading_advisor_3000/app/`, `trading_advisor_3000.app`.
- Excluded historical prefixes are counted separately and not assigned as active migration workload.

## Totals

| Metric | Value |
| --- | ---: |
| Total matches | 5034 |
| Active matches | 138 |
| Excluded historical matches | 4896 |

## Active Matches By Risk

| Risk | Count |
| --- | ---: |
| high | 4 |
| low | 116 |
| medium | 18 |

## Active Matches By Wave

| Wave | Count |
| --- | ---: |
| phase-02-compatibility-bridge | 12 |
| phase-03-docs-subtree-rename | 116 |
| phase-04-runtime-test-cutover | 10 |

## Wave Clusters

| Cluster | Group | Risk | Wave | Owner | Active refs | Files |
| --- | --- | --- | --- | --- | ---: | ---: |
| documentation-linkage | docs-links | low | phase-03-docs-subtree-rename | architecture | 77 | 154 |
| docs-architecture-linkage | docs-links | low | phase-03-docs-subtree-rename | architecture | 39 | 14 |
| test-namespace-dependencies | test-paths-fixtures | medium | phase-04-runtime-test-cutover | app-core+platform | 10 | 130 |
| script-path-references | scripts-validators | medium | phase-02-compatibility-bridge | platform | 8 | 1 |
| validator-and-gate-paths | scripts-validators | high | phase-02-compatibility-bridge | platform | 4 | 1 |
| runtime-namespace-dependencies | imports-code | high | phase-04-runtime-test-cutover | app-core+platform | 0 | 38 |

## Active Reference Sample (Top 80)

| File | Line | Token | Group | Risk | Wave |
| --- | ---: | --- | --- | --- | --- |
| `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md` | 74 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md` | 75 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md` | 76 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md` | 176 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md` | 199 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md` | 201 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/architecture/product-plane/README.md` | 13 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/dual-surface-safe-rename.phase-03.md` | 56 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/dual-surface-safe-rename.phase-04.md` | 55 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/dual-surface-safe-rename.phase-04.md` | 56 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/dual-surface-safe-rename.phase-04.md` | 57 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/dual-surface-safe-rename.phase-05.md` | 63 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/dual-surface-safe-rename.phase-05.md` | 64 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/f1-full-closure.phase-01.md` | 21 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/f1-full-closure.phase-01.md` | 22 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/f1-full-closure.phase-01.md` | 23 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/f1-full-closure.phase-01.md` | 47 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/f1-full-closure.phase-01.md` | 53 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/stack-conformance-remediation.phase-01.md` | 21 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/stack-conformance-remediation.phase-01.md` | 22 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/stack-conformance-remediation.phase-01.md` | 24 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/stack-conformance-remediation.phase-01.md` | 42 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/codex/modules/stack-conformance-remediation.phase-01.md` | 47 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-03-for-stack-conf.md` | 60 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-03-for-stack-conf.md` | 61 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-03-for-stack-conf.md` | 62 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-03-for-stack-conf.md` | 66 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-03-for-stack-conf.md` | 66 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-03-for-stack-conf.md` | 66 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-03-for-stack-conf.md` | 71 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-03-for-stack-conf.md` | 71 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-03-for-stack-conf.md` | 71 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-04-for-stack-conf.md` | 62 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-04-for-stack-conf.md` | 63 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-04-for-stack-conf.md` | 64 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 61 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 62 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 63 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 64 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 67 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 67 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 68 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 68 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 71 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 72 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 73 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 74 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 77 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 77 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 78 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md` | 78 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 63 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 63 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 63 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 64 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 64 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 64 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 65 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 66 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 67 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 68 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 75 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 75 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 75 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 76 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 76 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 76 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 77 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 78 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 79 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-06-for-stack-conf.md` | 80 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` | 62 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` | 62 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` | 62 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` | 63 | `docs/architecture/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` | 63 | `src/trading_advisor_3000/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` | 63 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` | 64 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` | 65 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |
| `docs/tasks/active/TASK-2026-03-27-continue-governed-module-phase-06-worker-route-f.md` | 66 | `tests/app/` | docs-links | low | phase-03-docs-subtree-rename |

## Machine-Readable Artifact
- JSON: `C:/Users/Admin/.codex/worktrees/b4ee-phase6/artifacts/rename-migration/phase6-inventory.json`
