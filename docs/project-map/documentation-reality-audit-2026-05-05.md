# Documentation Reality Audit - 2026-05-05

## Purpose

This document records a separate problem found during the TA3000 product reset:
repository documentation now contains too many old TZs, task notes, package-intake
artifacts, target-shape specs, and partial implementation records.

That material distorts project reality for both humans and agents. It makes old
intent look like current truth and makes unfinished or superseded work look active.

This audit does not delete files yet. It defines the cleanup policy and the first
delete/archive candidates.

Scope correction, 2026-05-06:

Task notes are a known noisy surface, but they are not the main cleanup target.
The primary target is project documentation that shapes human and agent
understanding of the implemented system: architecture docs, runbooks, specs,
checklists, and governed route documents.

Archive decision, 2026-05-06:

If project documentation is classified as obsolete, it should move to the single
off-route archive under `docs/archive/`. Active folders should not permanently
keep obsolete docs with warning banners.

## Product Reality Rule

For the reset direction, the current truth order is:

1. `origin/main` code and tests.
2. Authoritative runtime/data roots under `D:/TA3000-data`.
3. Current product reality docs:
   - `docs/project-map/current-truth-map-2026-05-05.md`
   - `docs/project-map/documentation-currentness-map-2026-05-06.md`
   - `docs/project-map/documentation-cleanup-candidates-2026-05-06.md`
   - `docs/architecture/product-plane/STATUS.md`
   - `docs/architecture/product-plane/CONTRACT_SURFACES.md`
   - `docs/architecture/product-plane/research-plane-platform.md`
   - `docs/runbooks/app/research-plane-operations.md`
   - `docs/project-map/product-reset-audit-2026-05-05.md`
4. Current project recovery docs:
   - `docs/project-map/status-inventory-2026-05-05.md`
   - `docs/project-map/worktree-diff-digest-2026-05-05.md`
5. Task notes, package-intake artifacts, generated acceptance artifacts, old TZs,
   and historical phase docs: historical evidence only, not current truth.

## Initial Inventory

Tracked repository snapshot:

| Area | Count |
| --- | ---: |
| tracked files | 1,806 |
| tracked Markdown files | 519 |
| `docs/tasks/active` | 40 files, 39 Markdown |
| `docs/tasks/archive` | 38 files, 37 Markdown |
| archived product-plane spec v2 package | 13 Markdown |
| `artifacts/codex/package-intake` | 57 files, 39 Markdown |
| `docs/architecture/product-plane` root | 30 Markdown |
| `docs/runbooks/app` | 22 files, 21 Markdown |
| `docs/agent` | 8 Markdown |

Noise indicators:

| Signal | Count |
| --- | ---: |
| files containing `ТЗ` | 14 |
| files containing `attempt` | 201 |
| files containing `phase` | 378 |
| files containing `blocker` | 240 |
| files containing `legacy` | 61 |
| files containing `governed` | 247 |

Interpretation:

The repo still carries a lot of delivery-process history. That is useful for
forensics, but harmful as an active reading surface.

## Keep

Keep and protect:

- release-blocking contracts, schemas, fixtures, and tests;
- current product-plane implementation docs;
- current app runbooks;
- current data/research/runtime proof docs;
- current architecture entrypoints;
- current skills under `.codex/skills` that encode TA3000-specific domain,
  data, compute, strategy, or signal-to-action knowledge;
- generated compatibility outputs that current scripts/tests still require.

These are allowed to remain in the active reading path.

## Rewrite Or Reclassify

These areas are likely still valuable, but need clearer labels:

| Area | Problem | Proposed action |
| --- | --- | --- |
| archived product-plane spec v2 package | target-shape spec can be mistaken for current implemented reality | archived under `docs/archive/product-plane-spec-v2/2026-05-06/`; current replacements are listed in the archive manifest |
| `docs/tasks/active` | old active notes make stale work look current | move closed/superseded notes out of active, keep only live governed task notes |
| `docs/session_handoff.md` | lightweight shim can accidentally anchor routing to stale CTX-OPS state | keep as pointer only; do not let it claim product state |
| `docs/architecture/README.md` and architecture indexes | may link old technical specs as if active | update reading order to prefer reset/current reality docs |
| old product-plane phase docs | phase closure language can overclaim implementation status | relabel as historical acceptance evidence |

## Delete Or External-Archive Candidates

Deletion should happen only after a reference scan and a focused test run. The
first candidates are:

| Area | Why candidate | Current reference risk |
| --- | --- | --- |
| duplicated package-intake extracts under `artifacts/codex/package-intake` | mostly historical TZ/package evidence; large and noisy | medium: manifests reference each other and some acceptance docs link them |
| `artifacts/codex/chat-intake/package-intake` | old chat intake packages, not active product truth | medium: may be useful only for forensics |
| old acceptance attempt artifacts under `artifacts/codex/orchestration` | many attempt/acceptor/worker prompts; high noise | medium: useful for process forensics, not product truth |
| duplicate F1 closure package extracts from 2026-03-30 | repeated TZ copies with the same large content | low-to-medium after backup |
| stale task-note files under `docs/tasks/active` | active location is misleading | low for product truth, but move/archive before delete |
| obsolete shell-package remnants under `codex_ai_delivery_shell_package` | old package source material, not product runtime | high: still referenced by scripts/tests, so do not delete first |
| rename-migration artifacts | historical rename evidence | high: still referenced by validation scripts and architecture docs |

Important:

`codex_ai_delivery_shell_package` and `artifacts/rename-migration` are noisy, but
they are not safe first deletes because current scripts/tests still reference them.

## Current Reference Scan

Known references found:

| Target | Reference count | Notes |
| --- | ---: | --- |
| `codex_ai_delivery_shell_package` | 11 | referenced by `AGENTS.md`, `README.md`, scripts, and architecture tests |
| `artifacts/codex/package-intake` | 26 | references are mostly package manifests and acceptance/process docs |
| product-plane spec v2 package | 10 | archived after active references were rerouted to current truth docs and stack conformance baseline |
| `artifacts/rename-migration` | 7 | referenced by validation scripts and rename docs |
| `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md` | 6 | referenced by architecture/codex contract docs |
| `docs/architecture/governed-pipeline-hardening-technical-specification.md` | 2 | referenced by architecture/codex contract docs |

## Proposed Cleanup Phases

### Phase 1: Label Reality

Goal: stop misleading reads before deleting anything.

Actions:

- update architecture reading order to put reset/current docs above old specs;
- classify old target-shape specs and package-intake docs;
- move obsolete docs to `docs/archive/` after reference scans;
- make active task notes explicitly non-authoritative unless tied to a current
  governed session.

### Phase 2: Move Misplaced Active Notes

Goal: make `docs/tasks/active` mean active again.

Actions:

- classify every active task note as `current`, `closed`, `superseded`,
  `historical`, or `unknown`;
- move closed/superseded notes to archive;
- keep a small index with reason and replacement source.

Status:

- Started 2026-05-06.
- 39 stale `docs/tasks/active/TASK-*.md` notes were moved to
  `docs/archive/historical-task-notes/2026-05-06/`.
- `docs/tasks/active/index.yaml` was reset to `items: []`.
- 12 generated project-map task-blocker items were moved to
  `docs/archive/historical-project-map-items/2026-05-06/` so the current
  cockpit no longer reads old worktree blockers as active project state.

### Phase 3: Package-Intake Deduplication

Goal: remove repeated TZ/package copies from active repository perception.

Actions:

- identify duplicate package-intake extracts by hash/name/date;
- keep one compressed or indexed copy if still needed for forensic evidence;
- remove duplicate extracted Markdown copies after reference update;
- keep manifests only where scripts still need them.

### Phase 4: Legacy Spec Retirement

Goal: separate current product reset from historical target-shape specs.

Actions:

- audit and archive `product-plane-spec-v2`;
- extract still-valid ideas into the reset docs if needed;
- move obsolete specs to `docs/archive/` after extracting still-valid content;
- update links so agents read current reality first.

### Phase 5: Hard Delete With Tests

Goal: delete only after proof.

Required checks before deletion:

- reference scan for deleted paths;
- `python scripts/context_router.py --from-git --format text`;
- relevant process tests if routing/indexes change;
- product smoke tests if product docs/contracts are touched.

## Delete Safety Rules

Do not delete a document if any of these are true:

- it is a release-blocking schema, fixture, or contract test;
- it is directly referenced by a current validation script;
- it is the only proof for a merged PR or current data/runtime route;
- it is the current source of truth for architecture, contracts, runtime,
  research, data roots, or runbooks;
- removing it changes context-router behavior without an updated replacement.

Safe deletion usually means:

- historical duplicate;
- referenced only from historical manifests;
- replacement/current source exists;
- references have been updated or intentionally removed;
- tests and context routing still pass.

Safe archive is the default before deletion. Archived docs remain available for
explicit forensic work, but are off-route for normal agents.

## Recommended First PR

Do not start with mass deletion.

First PR should be small:

1. Add this audit.
2. Add archive policy and cleanup candidate classification for highest-risk old
   specs.
3. Update the canonical reading order to point to product reset/current reality
   before target-shape specs.
4. No file deletion yet.

Second PR:

1. Classify `docs/tasks/active`.
2. Move obviously closed/superseded notes.
3. Keep an index of moved notes.

Third PR:

1. Deduplicate package-intake artifacts.
2. Remove repeated extracted TZ copies only after reference updates.

## Current Decision

Documentation cleanup is now a product-recovery task, not cosmetic cleanup.

The goal is to prevent old intentions, phase notes, and unfinished implementations
from appearing as current TA3000 reality.
