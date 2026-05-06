# Current Truth Map - 2026-05-05

## Purpose

This map separates current TA3000 truth from historical or superseded project
state. Use it before reading old task notes, TZ packages, phase reports, or
target-shape specifications.

The goal is simple: old intent must not masquerade as current product reality.

## Current Product Truth

Use these sources for current product-state questions:

| Question | Read first |
| --- | --- |
| What is TA3000 now? | `docs/project-map/product-reset-audit-2026-05-05.md` |
| Which code/data state is authoritative? | `docs/project-map/status-inventory-2026-05-05.md` |
| Which worktrees are candidates or noise? | `docs/project-map/worktree-diff-digest-2026-05-05.md` |
| Which docs are current vs stale? | `docs/project-map/documentation-reality-audit-2026-05-05.md` |
| Which project docs should be read as current? | `docs/project-map/documentation-currentness-map-2026-05-06.md` |
| Which docs are cleanup/archive candidates? | `docs/project-map/documentation-cleanup-candidates-2026-05-06.md` |
| What product-plane surfaces are implemented? | `docs/architecture/product-plane/STATUS.md` |
| Which contracts are release-blocking? | `docs/architecture/product-plane/CONTRACT_SURFACES.md` |
| How does the current research plane run? | `docs/architecture/product-plane/research-plane-platform.md` and `docs/runbooks/app/research-plane-operations.md` |
| What data is physically real? | `D:/TA3000-data/trading-advisor-3000-nightly/...` |

## Current Product Frame

The active product direction is:

```text
Research Factory
  -> validated strategy set
  -> Signal-to-Action lifecycle
  -> Telegram advisory signals
  -> manual repetition by the user
  -> later paper / semi-auto / live robot modes
```

Current out of scope:

- news and fundamental analysis;
- broad advisor cockpit UI;
- HFT;
- live execution before validated strategy evidence and Telegram advisory mode.

## Historical Evidence

The following surfaces may contain useful evidence, but they are not current truth
unless a current truth document explicitly promotes them:

| Surface | Default interpretation |
| --- | --- |
| `docs/tasks/active/*.md` | current process notes only when tied to a live governed session; otherwise historical clues |
| `docs/archive/**` | single off-route archive; forensic only, do not read by default |
| `docs/tasks/archive/*.md` | historical task evidence |
| `docs/archive/legacy-app-docs/2026-05-06/**` | archived legacy app-path docs; historical only |
| `docs/archive/product-plane-acceptance-checklists/2026-05-06/**` | archived historical acceptance checklist evidence; not current readiness proof |
| `docs/codex/modules/**` | governed phase briefs and route plans, not implementation proof |
| `docs/session_handoff.md` | lightweight pointer shim, not product state |
| `artifacts/codex/package-intake/**` | historical package/TZ intake evidence |
| `artifacts/codex/orchestration/**` | historical governed-attempt evidence |
| `codex_ai_delivery_shell_package/**` | historical shell package source material |
| `docs/archive/product-plane-spec-v2/2026-05-06/**` | archived target-shape specification; forensic only, not implementation proof |
| `artifacts/rename-migration/**` | historical rename evidence, still referenced by validation scripts |

## Reading Rule

When two documents disagree:

1. Current code/tests and authoritative data roots win over docs.
2. Current product reality docs win over target-shape specs.
3. Product reset docs win over old task notes.
4. Task notes and package-intake artifacts are evidence, not active direction.

## Cleanup Rule

Do not mass-delete old documentation before classification.

If a document is classified as obsolete, move it to `docs/archive/`. Do not keep
obsolete docs in active folders as permanent banner-only clutter.

Allowed cleanup order:

1. Classify stale surfaces.
2. Extract still-valid content into current truth docs.
3. Move obsolete docs to `docs/archive/`.
4. Remove only after reference scans and relevant tests pass.

## Cleanup Status

- 2026-05-06: moved 39 stale `docs/tasks/active/TASK-*.md` notes to
  `docs/archive/historical-task-notes/2026-05-06/`.
- 2026-05-06: reset `docs/tasks/active/index.yaml` to an empty active registry.
- 2026-05-06: moved 12 generated task-blocker project-map items to
  `docs/archive/historical-project-map-items/2026-05-06/`.
