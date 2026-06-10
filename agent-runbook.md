# Agent Runbook

## Goal
Keep parallel AI sessions predictable, isolated, and recoverable.

Primary source-of-truth for multi-worktree policy:
- `docs/workflows/worktree-governance.md`

## Session Rules
1. One task per active branch.
2. Do not mix governance-shell and product logic in one patch set.
3. Keep change surface small and explicit.
4. Record assumptions in planning artifacts before scaling the scope.
5. Use durable task notes only when the user or a validator explicitly needs
   a written intent/evidence artifact.

## Parallel Work Rules
1. Default to one primary worktree; add extra worktrees only when branch isolation is insufficient.
2. No shared mutable temporary artifacts across tasks.
3. If a policy check fails twice, pause feature work and remediate process first.
4. Use canonical gate order: focused verification -> loop -> PR -> nightly.

## Escalation
Escalate when:
- a patch needs emergency main override,
- high-risk surfaces are mixed in one change,
- required validation flow is not available.
