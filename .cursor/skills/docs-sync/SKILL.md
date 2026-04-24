---
name: docs-sync
description: Synchronize documentation with runtime behavior, contracts, and architecture entry-map policy.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-OPS
scope: docs-as-source-of-truth synchronization with architecture entry-map alignment
routing_triggers:
  - "documentation"
  - "sync docs"
  - "docs as code"
  - "policy docs"
---

# Docs Sync

## Purpose
Synchronize documentation with runtime behavior, contracts, and governance policy.

## Start Here In This Repository
- Use the first whole-system map listed in `docs/architecture/README.md` as the canonical architecture entry map.
- Use `docs/architecture/repository-surfaces.md` for path ownership and change-surface boundaries.
- Use `docs/architecture/product-plane/STATUS.md` for implemented-reality claims.
- Use `docs/architecture/product-plane/CONTRACT_SURFACES.md` for release-blocking boundary claims.

## Workflow
1. Confirm whether the requested update is about `shell`, `product-plane`, or `mixed` documentation.
2. Identify the document that should act as reader entrypoint, then update detail docs only where they add distinct truth.
3. If the task changes architecture understanding, keep the canonical map aligned with:
   - boundary ownership,
   - implementation status,
   - detailed target-shape docs.
4. Preserve the difference between:
   - orientation docs,
   - implementation-truth docs,
   - contract-truth docs.
5. Keep changes small, deterministic, and review-friendly.
6. Record assumptions and residual risks in task artifacts.

## Architecture Docs Rules
- A single reader-facing entry map should exist for whole-system orientation.
- Do not claim product implementation from target-spec text alone.
- When architecture navigation changes, update nearby indexes and hubs so the new entrypoint is discoverable.
- Prefer cross-links over duplicate long-form explanation when another document already owns the detailed truth.

## Validation
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
- `python scripts/validate_skills.py --strict`
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`

## Boundaries

This skill should NOT:
- turn an orientation doc into a second contract inventory.
- silently collapse implemented reality and target design into one claim set.
- update generated skill artifacts manually instead of using the supported sync/check flow.
