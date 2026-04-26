---
name: ta3000-governed-package-intake
description: Use for TA3000 governed package or TZ intake when the user asks to take work into the governed flow, create package-backed artifacts, choose package versus continue routing, or interpret blocked/checkpoint outcomes from real intake artifacts.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-ORCHESTRATION
scope: TA3000 governed package intake and route integrity
routing_triggers:
  - governed package intake
  - governed flow
  - package intake
  - technical intake
  - product intake
  - continue route
  - intake gate
---

# TA3000 Governed Package Intake

## When To Use
- The task is in `trading advisor 3000` and needs governed intake artifacts rather than chat-only analysis.
- The user provides a package, TZ, spec, or plan and wants it moved into the governed route.
- You need to decide whether the next route is `package`, `continue`, `stacked-followup`, or a blocked intake checkpoint.
- A prior governed attempt has route-integrity, clean-worktree, deferred-work, or timeout ambiguity.

Do not use this for simple local edits with no governed routing, or when the tree is so broadly dirty that proceeding would silently mix scopes. Stop and confirm scope in that case.

## Source-Of-Truth Order
1. `AGENTS.md`
2. `docs/agent/entrypoint.md`
3. `docs/DEV_WORKFLOW.md`
4. The governed launcher output and generated artifacts

## Procedure
1. Start from the canonical governed launcher. Do not improvise a manual chat-only continuation when the task asked for governed flow.
2. Normalize the source artifact. If package intake is required and the source is a bare `.md` / TZ / plan, wrap it into an explicit zip first.
3. Choose the route explicitly. Prefer `--route package` for new intake and avoid auto-routing when active modules could make continuation win by mistake.
4. Capture the artifact root and inspect actual outputs:
   - `manifest.md`
   - `manifest.json`
   - `intake-gate.json`
   - `intake-handoff.json`
   - technical/product lane payloads
5. If the command times out, inspect the generated bundle before retrying; timeout after artifact materialization is not the same as no work done.
6. Interpret outcomes honestly:
   - preserve blocked gates;
   - name human checkpoints as intended pauses;
   - continue only when artifacts, route, and worktree scope support it.
7. Before continuation or merge work, check worktree hygiene and avoid mixing unrelated diffs.

## Failure Patterns
- Auto-routing selects `continue` when the user intended package intake. Rerun with explicit `--route package`.
- A markdown/TZ source fails package intake because it was not zip-wrapped.
- The worker evidence leaves deferred critical work, so acceptance cannot pass even if local tests are green.
- A non-dry-run route stops at clean-worktree check, so route closure is not proven.
- Rollback starts touching unrelated changes; restore only session-owned files.

## Verification
- The route used the canonical launcher.
- The selected route matches the user request.
- Package intake used a zip when required.
- Generated artifacts exist and explain blocked/checkpoint/pass status.
- Continuation is not attempted from a silently mixed worktree scope.
