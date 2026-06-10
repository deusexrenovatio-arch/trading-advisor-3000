# Worktree Governance

## Purpose
Keep delivery deterministic and recoverable without forcing unnecessary worktree sprawl.

## Default Topology
1. Keep one primary worktree per active delivery stream by default.
2. Create extra worktrees only for truly parallel, conflict-heavy, or high-risk tracks.
3. Prune detached or idle worktrees once their branch is merged/abandoned.
4. For large refactors, temporary expansion is allowed; collapse back to baseline after integration.

## Multi-Worktree Etiquette
1. One active delivery stream per branch by default.
2. Use separate worktrees only when branch-level isolation is not enough.
3. Keep patch sets small and ownership-scoped.
4. Resolve integration conflicts in a dedicated integration branch/worktree.

## Data Ownership
- Gate artifacts are owned by the current worktree root:
  - `.runlogs/*`
  - `artifacts/*`
- Do not treat mutable `.runlogs` or `artifacts` outputs as shared across concurrent worktrees.

## Isolation Rules
- Ports:
  - assign dedicated service ports per worktree,
  - do not reuse ports across concurrent worktrees.
- Processes:
  - avoid shared long-running mutable processes between worktrees.
- Artifacts:
  - keep outputs deterministic and owned by the active worktree.

## Merge Discipline
1. Rebase feature worktrees on `origin/main` regularly.
2. Run loop/pr gates in the active worktree before push.
3. Merge by ordered patch sets for high-risk surfaces:
   - contracts -> code -> docs.
