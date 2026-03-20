# Documentation Index

## Scope of this repository
This repository is governance-first and contains two coordinated layers:
- the AI delivery shell for development methodology, architecture governance, validation, gates, and process lifecycle;
- isolated product-plane paths for app code, contracts, tests, and app-specific operational documentation.

Business and trading logic are not allowed inside shell control-plane surfaces, but product-plane work does exist in dedicated app paths.

## Read order
1. `docs/agent/entrypoint.md`
2. `docs/agent/domains.md`
3. `docs/agent/checks.md`
4. `docs/agent/runtime.md`
5. `docs/agent/skills-routing.md`
6. `docs/DEV_WORKFLOW.md`

If the task touches the product plane, continue with:
1. `docs/architecture/app/STATUS.md`
2. `docs/architecture/app/CONTRACT_SURFACES.md`
3. `docs/runbooks/app/bootstrap.md`

## Current phase
- Target scope: Phase 0 through Phase 8 shell baseline, with landed product-plane slices under app paths.
- Current status: governance baseline is active and still being hardened; completion is tracked by gate/test evidence, not optimistic phase labels.
- Ongoing focus: keep governance metrics green, preserve shell/product boundaries, and keep top-level docs aligned with actual product-plane status.

## Sections
- `docs/agent/` - hot policy docs for day-to-day execution.
- `docs/planning/` - migration and baseline planning artifacts.
- `docs/checklists/` - request and first-time-right contracts.
- `docs/workflows/` - workflow playbooks.
- `docs/runbooks/` - remediation and operational guides.
- `docs/architecture/` - architecture-as-docs package.
- `docs/architecture/app/` - product-plane status, contract surfaces, and app phase specs.
- `docs/tasks/` - active/archive lifecycle artifacts.
