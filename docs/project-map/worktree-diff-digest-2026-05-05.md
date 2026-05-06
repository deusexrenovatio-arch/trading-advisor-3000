# Worktree Diff Digest - 2026-05-05

## Purpose

This digest continues `docs/project-map/status-inventory-2026-05-05.md`.

It classifies local worktrees by diff shape and recovery value. It is not a full
code review and does not certify that any candidate is ready to merge.

## Source Basis

- Base: `origin/main` at `08f2a49a`.
- Scope: unmerged stale worktrees from the inventory, plus the fresh
  `codex/volume-plan` candidate.
- Signals used:
  - ahead/behind against `origin/main`;
  - `git cherry origin/main HEAD`;
  - committed diff stat;
  - working-tree diff stat;
  - changed path groups;
  - commit subjects.

## Executive Decision

| Priority | Worktree | Decision | Why |
| ---: | --- | --- | --- |
| 1 | `D:/CodexHome/worktrees/fd4a/trading advisor 3000` | extract first | fresh on current `origin/main`; focused volume-profile research changes |
| 2 | `D:/trading advisor 3000-spark-dagster-pr` | extract separate | large MOEX Spark/Dagster route candidate; not merged; high value but stale |
| 3 | `D:/trading advisor 3000-moex-cleanup` | quarantine | huge dirty workspace; overlaps Spark/Dagster route; unsafe wholesale |
| 4 | `D:/trading advisor 3000` | quarantine/superseded mix | stale dirty main worktree; key research commit is already patch-equivalent upstream |
| 5 | `D:/CodexHome/worktrees/10a9/trading advisor 3000` and `D:/CodexHome/worktrees/49a3/trading advisor 3000` | low-priority review or archive | duplicate detached governance prompt-policy work |
| 6 | all patch-equivalent routing/task/process leftovers | archive after backup | `git cherry` says commits are already represented upstream |

## Priority 1: Volume Profile Candidate

Worktree:

- path: `D:/CodexHome/worktrees/fd4a/trading advisor 3000`
- branch: `codex/volume-plan`
- head: `08f2a49a`
- relation to `origin/main`: `0/0`
- committed diff: none
- dirty tracked diff: `8` files, `383` insertions, `11` deletions
- untracked file: `derived_indicators/volume_profile.py`, `362` lines

Changed surface:

- derived indicator materialization;
- derived indicator registry/store exports;
- backtest input-bundle loading;
- continuous-front indicator roll rules;
- unit tests for derived indicators and backtest layer.

Important new file:

- `src/trading_advisor_3000/product_plane/research/derived_indicators/volume_profile.py`

Observed public objects:

- `VolumeProfileSettings`
- `compute_volume_profile_features`
- value-area and cluster helpers
- quality codes for missing source, low coverage, and volume mismatch

Decision:

Extract first into a fresh branch from `origin/main`. This is the only candidate
that is already based on current main and looks like a focused product-plane
research slice.

Required next proof:

- review exact semantics of POC/value area/cluster outputs;
- confirm no strategy logic leaks into generic derived-indicator materialization;
- run focused unit tests for derived indicators and backtest input loading;
- if accepted, create a PR as a product-plane change.

## Priority 2: MOEX Spark/Dagster Candidate

Worktree:

- path: `D:/trading advisor 3000-spark-dagster-pr`
- branch: `codex/moex-spark-dagster-route`
- head: `ebf2520e`
- relation to `origin/main`: `10/190`
- cherry status: `10` commits not patch-equivalent upstream
- committed diff: `107` files, `14,552` insertions, `1,923` deletions
- dirty diff: `2` files, `13` insertions, `3` deletions

Commit shape:

- historical route handoff contracts;
- Spark canonicalization and Dagster cutover;
- historical refresh runbooks;
- ingest/canonicalization/cutover tests;
- removal of repo-local storage fallbacks;
- operator naming normalization;
- daily baseline updater;
- baseline catchup hardening;
- Dagster staging deployment;
- architecture surface classification.

Decision:

Extract separate. This is too large and stale to merge directly, but it is the
highest-value unmerged technical candidate after the volume-profile slice.

Required next proof:

- compare against current MOEX baseline/data-root docs and current
  `D:/TA3000-data` layout;
- split into contracts, runtime, orchestration, tests, and docs;
- rebuild from fresh `origin/main`, not by rebasing the stale dirty worktree;
- use real storage proof before claiming route acceptance.

## Priority 3: MOEX Hard Cleanup

Worktree:

- path: `D:/trading advisor 3000-moex-cleanup`
- branch: `codex/moex-hard-cleanup`
- head: `8acc3a03`
- relation to `origin/main`: `1/190`
- cherry status: `1` commit not patch-equivalent upstream
- committed diff: `107` files, `14,552` insertions, `1,923` deletions
- dirty diff: `185` files, `1,243` insertions, `9,137` deletions
- staged diff: `108` files

Decision:

Quarantine. This appears to overlap heavily with the Spark/Dagster route while
also carrying a very large dirty cleanup. Do not merge or rebase this worktree.

Acceptable use:

- compare against `D:/trading advisor 3000-spark-dagster-pr`;
- extract only clearly named cleanup decisions after the Spark/Dagster route is
  decomposed;
- preserve before any destructive cleanup.

## Stale Main Worktree

Worktree:

- path: `D:/trading advisor 3000`
- branch: `codex/materialize-indicators-derived-job`
- head: `ca0a24d7`
- relation to `origin/main`: `6/78`
- cherry status: `4` not patch-equivalent, `2` patch-equivalent
- committed diff: `30` files, `3,047` insertions, `233` deletions
- dirty diff: `8` files, `213` insertions, `4` deletions
- dirty entries: `28`

Important cherry finding:

- `fix(research): materialize indicators incrementally` is patch-equivalent
  upstream.
- several skill-routing/governance commits remain non-equivalent, but PR #96
  already merged the active Superpowers-first routing work.

Decision:

Do not use this worktree as a base. Treat it as a mixed salvage surface. The
research materialization part appears already represented upstream; remaining
value, if any, is likely docs/process residue and should not block product-plane
work.

## Patch-Equivalent Or Superseded Worktrees

These worktrees should not be promoted as active candidates unless a later manual
diff proves a missing decision.

| Worktree | Cherry signal | Decision |
| --- | --- | --- |
| `D:/CodexHome/worktrees/6146/ta3000-pr-stack` | `7` patch-equivalent commits | archive after backup; feature-layer removal already represented upstream |
| `D:/CodexHome/worktrees/serena-policy-pr` | `1` patch-equivalent commit, dirty process files | likely superseded by current routing docs / PR #96 |
| `D:/CodexHome/worktrees/ta3000-skill-routing-pr` | `1` patch-equivalent commit | superseded by current skill-routing state |
| `D:/ta3000-codex-moex-canonical-data-health` | `1` patch-equivalent commit | data-health fix already represented upstream |
| `D:/ta3000-pr-push` | `1` patch-equivalent commit | archive process/intake residue |
| `D:/ta3000-scoring-quality-pr` | `1` patch-equivalent commit | archive or reopen only if governance scoring becomes active again |
| `D:/worktrees/moex-data-root-truth-sync-fix` | `1` patch-equivalent commit | data-root truth docs already represented upstream |

## Duplicate Detached Governance Work

Worktrees:

- `D:/CodexHome/worktrees/10a9/trading advisor 3000`
- `D:/CodexHome/worktrees/49a3/trading advisor 3000`

Both point to `ea277324`, are clean, and carry the same two non-equivalent
governance prompt-policy commits:

- `feat(governed): harden prompt policy and pin model matrix`
- `chore(process): archive publish task state`

Decision:

Low-priority review or archive. They are duplicate detached worktrees from an
older governance lane. They should not interrupt product-plane recovery unless
the governed prompt/model-policy work is explicitly reopened.

## Recommended Recovery Order

1. Review and extract `codex/volume-plan`.
2. Create a clean branch from `origin/main` for the accepted part of the
   volume-profile work.
3. After that, start a separate decomposition of
   `D:/trading advisor 3000-spark-dagster-pr`.
4. Use `D:/trading advisor 3000-moex-cleanup` only as a comparison source.
5. Archive patch-equivalent worktrees after backup or after the user confirms
   they are no longer needed.

## Do Not Do

- Do not merge the stale main worktree.
- Do not rebase the MOEX cleanup worktree wholesale.
- Do not treat task-note dirt as project state.
- Do not infer accepted product behavior from a verification root unless the code
  is merged and the current route was refreshed.

## Acceptance For This Digest

- Worktrees were compared to refreshed `origin/main`.
- `git cherry` was checked for patch-equivalence.
- The fresh `volume-plan` candidate was inspected more closely.
- The two ambiguous `D:/trading advisor 3000-...` worktree paths were corrected
  from the previous inventory table.
- No code was changed.
- No worktree was deleted, rebased, or merged.
