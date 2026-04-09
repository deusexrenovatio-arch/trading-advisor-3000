# Worktree Governance

## Purpose
Keep delivery deterministic and recoverable without forcing unnecessary worktree sprawl.

## Task Session Lock
1. Start work from the branch you are actively using:
   - `python scripts/task_session.py begin --request "<request>"`
   - default binding is `branch-shared` (branch identity is strict, worktree path can move)
   - use `--binding worktree-strict` only when hard path isolation is required
2. Verify session identity before long runs:
   - `python scripts/task_session.py status`
3. If identity mismatch is detected:
   - branch mismatch: stop and switch to the expected branch or start a new session,
   - strict worktree mismatch: switch to the expected worktree or restart with the intended binding.

## Default Topology
1. Keep one primary worktree per active delivery stream by default.
2. Create extra worktrees only for truly parallel, conflict-heavy, or high-risk tracks.
3. Prune detached or idle worktrees once their branch is merged/abandoned.
4. For large refactors, temporary expansion is allowed; collapse back to baseline after integration.

## Multi-Worktree Etiquette
1. One active task per branch/session identity.
2. Use separate worktrees only when branch-level isolation is not enough.
3. Keep patch sets small and ownership-scoped.
4. Resolve integration conflicts in a dedicated integration branch/worktree.

## Data Ownership
- Session lock and lifecycle state are stored in the active repo root:
  - `.runlogs/task-session/session-lock.json`
- Governed and gate artifacts are owned by the current worktree root:
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
