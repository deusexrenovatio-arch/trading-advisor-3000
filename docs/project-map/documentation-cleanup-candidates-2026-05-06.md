# Documentation Cleanup Candidates - 2026-05-06

## Purpose

This document is the working list for cleaning project documentation without
losing historical evidence.

Rule: once a document is classified as obsolete, move it to `docs/archive/`.
Do not leave obsolete docs in active folders as permanent banner-only clutter.

## Decision Labels

| Label | Meaning |
| --- | --- |
| `current` | Can answer current project-state questions. |
| `supporting` | Useful, but read after current truth docs. |
| `extract_then_archive` | Contains some useful content; extract the useful part into a current truth doc, then archive the old file. |
| `archive_candidate` | Likely obsolete; move to archive after reference scan and index update. |
| `keep_for_validator` | No longer conceptually current, but still referenced by validators or tests. Do not move until those references are retired. |

## Candidate Groups

| Group | Initial classification | Current replacement / owner | Proposed archive target |
| --- | --- | --- | --- |
| `docs/archive/product-plane-spec-v2/2026-05-06/**` | `archived_2026-05-06` | `docs/project-map/product-reset-audit-2026-05-05.md`, `docs/architecture/product-plane/STATUS.md`, `docs/architecture/product-plane/research-plane-platform.md`, `docs/architecture/product-plane/stack-conformance-baseline.md` | done |
| `docs/archive/legacy-app-docs/2026-05-06/**` | `archived_2026-05-06` | `docs/architecture/product-plane/**` | done |
| old product-plane capability phase docs | `extract_then_archive` | `docs/architecture/product-plane/STATUS.md`, current route decisions, current runbooks | `docs/archive/product-plane-capability-history/2026-05-06/` |
| `docs/archive/product-plane-acceptance-checklists/2026-05-06/**` | `archived_2026-05-06` | `docs/architecture/product-plane/STATUS.md`, `docs/architecture/product-plane/CONTRACT_SURFACES.md`, `docs/architecture/product-plane/stack-conformance-baseline.md` | done |
| old governed route modules under `docs/codex/modules/**` | mixed: `archive_candidate` / `supporting` | active route contracts, `docs/architecture/product-plane/STATUS.md`, current route decisions | `docs/archive/governed-route-history/2026-05-06/` |
| `docs/codex/contracts/**` | mixed: `supporting` / `keep_for_validator` | governed launcher and current route contracts | do not move until route references are audited |
| duplicated package-intake extracts under `artifacts/codex/package-intake/**` | `archive_candidate` outside docs | current truth docs and package manifests | later artifact cleanup, not this docs batch |
| shell rename / migration specs | `keep_for_validator` | validator allowlists, architecture policy docs | do not move until validators stop referencing them |

## First Move Candidates

Safest first documentation moves after reference scan:

1. old product-plane capability phase docs
2. old governed route modules under `docs/codex/modules/**`
3. historical package-intake extracts under `artifacts/codex/package-intake/**`

These groups are high-noise and already have current replacements. They should
be moved in separate batches so broken references are easy to diagnose.

## Do Not Move Yet

Do not move these until a specific replacement is proven:

- `docs/architecture/product-plane/STATUS.md`
- `docs/architecture/product-plane/CONTRACT_SURFACES.md`
- `docs/architecture/product-plane/native-runtime-ownership.md`
- `docs/architecture/product-plane/research-plane-platform.md`
- `docs/architecture/product-plane/moex-historical-route-decision.md`
- current runbooks under `docs/runbooks/app/**`
- governed contracts that current scripts or validators still reference

## Batch Acceptance

Every archive batch must prove:

- old files no longer appear in active read-order indexes;
- links either point to current replacements or to archive batch README only
  when historical evidence is intentionally needed;
- `python scripts/validate_docs_links.py --roots AGENTS.md docs` passes;
- `python scripts/context_router.py --from-git --format text` does not promote
  archived files as hot context;
- relevant project-map checks pass when project-map docs changed.
