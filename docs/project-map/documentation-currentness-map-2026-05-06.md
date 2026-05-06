# Documentation Currentness Map - 2026-05-06

## Purpose

This map classifies repository documentation by currentness. It is about
project documentation, not task-note lifecycle history.

Use it when deciding what to read, rewrite, archive, or ignore while rebuilding
the current TA3000 project picture.

## Current Truth

These documents can answer current project-state questions when they are backed
by code, tests, and data-root evidence:

| Area | Current documents |
| --- | --- |
| Overall project reality | `docs/project-map/current-truth-map-2026-05-05.md`, `docs/project-map/product-reset-audit-2026-05-05.md`, `docs/project-map/status-inventory-2026-05-05.md` |
| Whole-repository architecture | `docs/architecture/trading-advisor-3000.md`, `docs/architecture/repository-surfaces.md` |
| Product-plane implemented reality | `docs/architecture/product-plane/STATUS.md` |
| Product contracts | `docs/architecture/product-plane/CONTRACT_SURFACES.md`, `docs/architecture/product-plane/contract-change-policy.md` |
| Product compute/runtime ownership | `docs/architecture/product-plane/native-runtime-ownership.md` |
| Research factory | `docs/architecture/product-plane/research-plane-platform.md`, `docs/runbooks/app/research-plane-operations.md` |
| MOEX data plane | `docs/architecture/product-plane/moex-historical-route-decision.md`, `docs/runbooks/app/moex-baseline-storage-runbook.md`, `docs/runbooks/app/moex-raw-ingest-runbook.md`, `docs/runbooks/app/moex-canonical-refresh-runbook.md`, `docs/runbooks/app/moex-dagster-route-runbook.md` |
| Signal/runtime/execution boundaries | `docs/architecture/product-plane/sidecar-wire-api-v1.md`, `docs/runbooks/app/real-execution-transport-runbook.md`, `docs/runbooks/app/live-execution-incident-runbook.md` |
| Delivery shell operation | `docs/agent/entrypoint.md`, `docs/agent/checks.md`, `docs/agent/runtime.md`, `docs/DEV_WORKFLOW.md` |

## Supporting Current Docs

These documents are useful, but should be read as supporting material rather
than primary truth:

| Area | Interpretation |
| --- | --- |
| `docs/runbooks/app/**` | operational procedures; current only when they agree with `docs/architecture/product-plane/STATUS.md`, route decisions, and executable code |
| `docs/obsidian/**` | navigation and visual projections over current docs |
| `docs/project-map/state/**` | attention/state graph, not implementation proof |
| `docs/checklists/*.md` outside `app/` | shell/process checklists, not product implementation truth |
| `docs/workflows/**` | delivery workflow guidance |
| `docs/planning/**` | planning memory; not current implementation status |

## Historical Or Target-Shape Docs

These documents can contain useful design intent or acceptance evidence, but
must not answer "what is implemented now?" unless a current truth document
explicitly promotes them.

| Area | Default interpretation |
| --- | --- |
| `docs/archive/product-plane-spec-v2/2026-05-06/**` | archived historical / target-shape product specification |
| `docs/archive/legacy-app-docs/2026-05-06/**` | archived legacy app-path documentation |
| old capability phase docs under `docs/architecture/product-plane/*.md` | historical capability evidence unless named in `docs/architecture/product-plane/STATUS.md` as current truth |
| `docs/archive/product-plane-acceptance-checklists/2026-05-06/**` | archived historical acceptance checklists; do not infer current product readiness from checked boxes |
| `docs/codex/contracts/**` | governed execution contracts; active only for the named governed route, not global product truth |
| `docs/codex/modules/**` | governed phase briefs and historical route plans; not implementation proof by themselves |
| `docs/codex/prompts/**` and `docs/codex/orchestration/**` | delivery-process machinery |
| `docs/archive/**` | off-route archive; do not read unless explicitly doing forensic/audit work |

## Reading Rule

When documents disagree:

1. Current code, tests, and authoritative data roots win.
2. `docs/architecture/product-plane/STATUS.md`,
   `docs/architecture/product-plane/CONTRACT_SURFACES.md`, current route
   decisions, and current runbooks win over old phase/checklist/spec wording.
3. Target-shape specs and governed phase docs are design or process evidence,
   not product state.
4. A checked acceptance box is not enough to claim current readiness.

## Cleanup Rule

Do not delete project documentation before classifying it.

Preferred cleanup order:

1. Classify the document in
   `docs/project-map/documentation-cleanup-candidates-2026-05-06.md`.
2. Rewrite or extract still-valid content into current truth docs.
3. Move obsolete docs to `docs/archive/` after reference scans pass.
4. Delete only duplicates that have a replacement and no current validator,
   route, or proof dependency.

Do not keep obsolete documents in active folders as permanent banner-only
content. Once obsolete status is decided, archive the document batch.
