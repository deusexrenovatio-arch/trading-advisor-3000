# Worktree Governance

## Purpose
Keep multi-worktree development deterministic, isolated, and recoverable.

## Task Session Lock
1. Start work from the actual worktree/branch in use:
   - `python scripts/task_session.py begin --request "<request>"`
2. Verify session identity before long runs:
   - `python scripts/task_session.py status`
3. If identity mismatch is detected:
   - stop execution in the wrong worktree,
   - switch to correct worktree,
   - start a new session from that location.

## Multi-Worktree Etiquette
1. One active task per worktree branch.
2. Keep unrelated changes in separate worktrees.
3. Keep patch sets small and ownership-scoped.
4. Resolve integration conflicts in a dedicated integration branch/worktree.

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
