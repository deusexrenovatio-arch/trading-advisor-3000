# Project Status Inventory - 2026-05-05

## Purpose

This is a recovery inventory for the current TA3000 project state.

It intentionally does not use active task notes as the source of truth. The active
task-note layer is treated as historical/process context only, because recent work
was split across multiple worktrees and not consistently merged through the older
task-note flow.

## Source Priority

1. `origin/main` and merged PR history.
2. Live worktree and branch state.
3. Authoritative runtime/data roots under `D:/TA3000-data`.
4. Current product-plane status and runbook docs.
5. Task notes and session handoff files, only as historical clues.

## Snapshot

- Snapshot date: 2026-05-05.
- Change surface of this inventory: mixed process/product-plane documentation.
- Remote refs were refreshed before inspection.
- `origin/main`: `08f2a49a`.
- Open PRs: `0`.
- Worktrees discovered: `22`.
- Active task files discovered under `docs/tasks/active`: `41`.

## Current Code Truth

The authoritative merged code state is `origin/main` at `08f2a49a`.

Recent merged PRs show that the latest accepted stack includes:

- PR #96, `codex/superpowers-skill-routing-main`, merged 2026-05-05.
- PR #95, process regression burn-in, merged 2026-04-30.
- PR #94 and #93, continuous-front indicator evidence/runtime, merged 2026-04-30.
- PR #85 through #92, Optuna/vectorbt preintegration stack, merged 2026-04-30.

There are no open PRs at the time of this inventory. That means there is no remote
review queue representing the local leftover worktrees.

## Current Local Workspace Risk

The main local workspace is not a clean source of truth:

- path: `D:/trading advisor 3000`
- branch: `codex/materialize-indicators-derived-job`
- head: `ca0a24d7`
- relation to `origin/main`: `6` ahead, `78` behind
- merged to `origin/main`: no
- contains current `origin/main`: no
- dirty entries: `28`
- remote branch: gone

Decision: do not merge or publish this worktree as a unit. Treat it as a salvage
surface. Any useful work must be extracted into a fresh branch from current
`origin/main`.

## Worktree Inventory

| Worktree | Branch | Head | Relation to `origin/main` | State | Dirty | Inventory decision |
| --- | --- | --- | --- | --- | ---: | --- |
| `D:/trading advisor 3000` | `codex/materialize-indicators-derived-job` | `ca0a24d7` | `6/78` | unmerged, stale | 28 | quarantine, salvage only |
| `D:/CodexHome/worktrees/10a9` | detached | `ea277324` | `2/194` | unmerged, stale | 0 | likely historical; diff before archive |
| `D:/CodexHome/worktrees/49a3` | detached | `ea277324` | `2/194` | unmerged, stale | 0 | duplicate historical candidate |
| `D:/CodexHome/worktrees/6146/ta3000-pr-stack` | `codex/research-06-remove-active-feature-layer` | `fa0de134` | `7/88` | unmerged, stale | 0 | candidate; needs diff against current main |
| `D:/CodexHome/worktrees/6146` | `codex/vectorbt-optuna-09-task-notes` | `7b10e1b0` | `0/32` | merged, stale | 1 | mostly historical; dirty `.serena` only needs cleanup |
| `D:/CodexHome/worktrees/7a37` | detached | `35aa5dfa` | `0/6` | merged, stale | 0 | historical proof/context |
| `D:/CodexHome/worktrees/8c7a` | `codex/restart-ta3000-strategy-storage-20260416` | `5e88482c` | `0/190` | merged, stale | 4 | historical intake leftovers |
| `D:/CodexHome/worktrees/cf-session-active-timeline-20260502` | `codex/cf-session-active-timeline` | `35aa5dfa` | `0/6` | merged, stale | 14 | continuous-front investigation context |
| `D:/CodexHome/worktrees/continuous-front-refresh-20260502` | detached | `35aa5dfa` | `0/6` | merged, stale | 1 | continuous-front refresh context |
| `D:/CodexHome/worktrees/dd79` | detached | `54185c58` | `0/194` | merged, stale | 0 | historical |
| `D:/CodexHome/worktrees/fd4a` | `codex/volume-plan` | `08f2a49a` | `0/0` | fresh from main | 8 | active candidate; review first |
| `D:/CodexHome/worktrees/native-runtime-ownership` | `main` | `6f7e6b86` | `0/74` | merged, stale | 0 | historical |
| `D:/CodexHome/worktrees/serena-policy-pr` | `codex/tighten-semantic-navigation-policy` | `0e56cd63` | `1/105` | unmerged, stale | 5 | likely superseded; verify before discard |
| `D:/CodexHome/worktrees/serena-worktree-bootstrap-20260429` | `codex/serena-worktree-bootstrap-20260429` | `16a99e12` | `0/73` | merged, stale | 4 | published flow residue |
| `D:/CodexHome/worktrees/superpowers-skill-routing-main` | `codex/superpowers-skill-routing-main` | `d40fec5a` | `0/1` | merged, stale | 0 | recent PR context |
| `D:/CodexHome/worktrees/ta3000-skill-routing-pr` | `codex/skill-runtime-routing` | `438f6a6a` | `1/75` | unmerged, stale | 0 | likely superseded by PR #96; verify |
| `D:/ta3000-codex-moex-canonical-data-health` | `codex/moex-canonical-data-health` | `b8eae321` | `1/107` | unmerged, stale | 5 | data-health candidate; review |
| `D:/ta3000-pr-push` | `codex/intake-human-checkpoint-rebased` | `e38ff914` | `1/191` | unmerged, stale | 4 | process/intake residue |
| `D:/ta3000-scoring-quality-pr` | `codex/scoring-quality-split` | `171424d9` | `1/194` | unmerged, stale | 4 | scoring/process candidate |
| `D:/trading advisor 3000-moex-cleanup` | `codex/moex-hard-cleanup` | `8acc3a03` | `1/190` | unmerged, stale | 209 | high-risk quarantine |
| `D:/trading advisor 3000-spark-dagster-pr` | `codex/moex-spark-dagster-route` | `ebf2520e` | `10/190` | unmerged, stale | 4 | high-value candidate; needs focused diff |
| `D:/worktrees/moex-data-root-truth-sync-fix` | detached | `03749a2e` | `1/192` | unmerged, stale | 3 | old data-root fix candidate |

Counts:

- unmerged stale worktrees: `12`
- dirty worktrees: `14`
- fresh worktrees at current `origin/main`: `1`, `D:/CodexHome/worktrees/fd4a`

## Product-Plane Status From Current Docs

Current product-plane status says the product-plane landing and data/research
scaffolding are implemented, but full production hardening is not implied.

The current research route is documented as a materialized pipeline:

```text
canonical data
  -> research_datasets / research_bar_views
  -> research_indicator_frames
  -> research_derived_indicator_frames
  -> strategy registry refresh
  -> research_strategy_* materialization
  -> research_backtest_* outputs
  -> research_strategy_rankings / research_run_findings
  -> research_signal_candidates
  -> runtime
```

The operational route is the campaign runner:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config product-plane/campaigns/fut_br_base_15m.explore.yaml
```

Important boundary: completion claims for data/research/compute/optimization
must prove the native owner path. Python-only smoke tests are not enough for
Spark, Delta Lake, Dagster, pandas-ta-classic, vectorbt, Optuna, or DuckDB
surfaces.

## Authoritative Data Roots

Authoritative root exists:

- `D:/TA3000-data`
- `D:/TA3000-data/trading-advisor-3000-nightly`

Relevant current roots:

- raw MOEX baseline: `D:/TA3000-data/trading-advisor-3000-nightly/raw/moex/baseline-4y-current`
- research gold current: `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current`
- research gold verification: `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/verification`

The current research gold root exists and was modified on 2026-05-04.

## Research Gold Current Evidence

The current research gold root contains real Delta tables with `_delta_log`.

| Table | Rows | Delta version |
| --- | ---: | ---: |
| `continuous_front_bars.delta` | 1,024,973 | 0 |
| `continuous_front_roll_events.delta` | 1,211 | 0 |
| `continuous_front_adjustment_ladder.delta` | 1,211 | 0 |
| `continuous_front_qc_report.delta` | 52 | 0 |
| `cf_indicator_input_frame.delta` | 1,024,973 | 1 |
| `indicator_roll_rules.delta` | 202 | 1 |
| `continuous_front_indicator_frames.delta` | 1,024,973 | 1 |
| `continuous_front_derived_indicator_frames.delta` | 1,024,973 | 1 |
| `continuous_front_indicator_acceptance_report.delta` | 1 | 1 |
| `continuous_front_indicator_qc_observations.delta` | 11 | 1 |
| `continuous_front_indicator_run_manifest.delta` | 1 | 1 |
| `research_datasets.delta` | 2 | 9 |
| `research_bar_views.delta` | 2,029,833 | 9 |
| `research_indicator_frames.delta` | 1,024,973 | 12 |
| `research_derived_indicator_frames.delta` | 1,024,973 | 554 |
| `research_feature_frames.delta` | 2,543,712 | 0 |
| `research_instrument_tree.delta` | 17 | 9 |

Interpretation:

- continuous-front data and indicator outputs are physically materialized.
- research data-prep tables are physically materialized.
- `research_feature_frames.delta` still exists in current storage. It must not be
  treated as active without route verification, because recent work moved active
  semantics away from a feature-layer primary route.

## Verification Roots

The verification root exists and contains `22` run folders.

Recent or important verification folders include:

- `continuous_front_session_0900_2350_full_audit_linux`, modified 2026-05-02,
  with `8` Delta tables.
- `continuous_front_session_0900_2350_audit_linux`, modified 2026-05-02,
  with `8` Delta tables.
- `cf-param-probe-20260502-br15m-volume-cb2`, modified 2026-05-02,
  with `4` Delta tables.
- `optuna-signal-research-br-forced-proof`, modified 2026-04-30,
  with `5` Delta tables.
- `continuous-front-indicator-short-proof-20260430-br-202204-202206-causal-indicators`,
  modified 2026-04-30, with `14` Delta tables.
- `continuous-front-codex-proof-20260428-br-202203-repair`,
  modified 2026-04-28, with `13` Delta tables.

Decision: verification roots are valid evidence for specific branches/runs, but
they are not automatically current unless the code is merged and the accepted
route has been refreshed into `research/gold/current`.

## Main Problems

1. No single project-status source exists.
   The current truth is split between `origin/main`, local worktrees, product docs,
   and `D:/TA3000-data`.

2. Active task notes are not reliable as current truth.
   There are `41` active task files. The context router still points to CTX-OPS and
   active task-note surfaces, but the user's observation is correct: this layer is
   stale for actual merged/current state.

3. Local worktree sprawl is now a real risk.
   There are `22` worktrees; `12` are unmerged and stale. Several are dirty. Some
   are likely superseded by merged PRs, but that must be proven by diff review.

4. The main local workspace is unsafe as a base branch.
   It is dirty, stale, ahead of old history, and tracks a gone remote branch.

5. Data current and code current are separate concepts.
   `research/gold/current` has real Delta outputs, but worktree-only code changes
   must not be inferred as accepted just because verification artifacts exist.

6. Legacy research surfaces still need classification.
   The current storage still contains `research_feature_frames.delta`. It may be
   legacy evidence, compatibility residue, or still consumed somewhere. That needs
   route-backed verification before cleanup or acceptance claims.

## Recovery Ledger

| Item | Current classification | Evidence | Next action |
| --- | --- | --- | --- |
| `origin/main` | authoritative code baseline | fetched `08f2a49a`, open PRs `0` | use as base for all new integration |
| Product-plane status docs | current documentation baseline | `docs/architecture/product-plane/STATUS.md`, research platform doc, operations runbook | keep, but align after worktree triage |
| `D:/TA3000-data/.../research/gold/current` | authoritative current data root | Delta logs and row counts present | use as data proof baseline |
| active task notes | historical/process context | `41` active files; route points to CTX-OPS | do not use as truth |
| `codex/volume-plan` worktree | active candidate | fresh from main, dirty product research files | first candidate to review |
| `codex/research-06-remove-active-feature-layer` | stale candidate | unmerged, clean, `7/88` from main | compare against merged PRs and current route |
| MOEX Spark/Dagster worktree | stale high-value candidate | unmerged, `10/190`, dirty | review separately; do not mix with cleanup |
| MOEX hard cleanup worktree | high-risk quarantine | unmerged, dirty `209` entries | preserve, summarize, do not merge wholesale |
| skill/Serena routing leftovers | likely superseded | PR #96 merged; old routing branches remain | verify then archive/delete |
| detached old worktrees | likely historical | stale, mostly clean | archive after diff digest |

## Recommended Next Slice

Do not start by updating task notes.

Recommended next slice:

1. Create a worktree diff digest for the `12` unmerged stale worktrees.
2. Classify each as `preserve`, `extract`, `superseded`, or `discard-after-backup`.
3. Start with `D:/CodexHome/worktrees/fd4a` because it is the only fresh
   current-main candidate and appears to contain focused volume-profile research
   changes.
4. Then review the MOEX Spark/Dagster worktree as a separate high-value candidate.
5. Only after that, update the official status docs and optionally retire or archive
   old task-note surfaces.

## Acceptance For This Inventory

- Git remote refs were refreshed.
- Worktree state was enumerated.
- GitHub PR state was checked.
- `D:/TA3000-data` roots were checked.
- Delta `_delta_log` presence and row counts were checked for current research gold.
- No code path was changed.
- No task notes were rewritten.
