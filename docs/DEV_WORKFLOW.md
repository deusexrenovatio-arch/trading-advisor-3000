# Development Workflow (AI Delivery Shell)

## Objective
Provide a predictable and enforceable workflow for AI-first governance delivery.

## Canonical loop
1. Read hot docs (`docs/agent/*`).
2. Confirm shell-only scope (no domain logic).
3. Start task session: `python scripts/task_session.py begin --request "<request>"`.
4. Keep task contract current in active task note.
5. Run loop gate: `python scripts/run_loop_gate.py --from-git --git-ref HEAD`.
6. Run PR gate before closeout.
7. Close lifecycle with `python scripts/task_session.py end` when outcome is terminal.

## Gate order
`begin -> task note -> task contract validation -> loop gate -> pr gate -> nightly gate -> dashboard refresh -> end`

## CI lane model
1. `loop-lane` for fast surface-aware feedback.
2. `pr-lane` for closeout confidence and full QA matrix.
3. `nightly-lane` for hygiene, telemetry, and report generation.
4. `dashboard-refresh` for deterministic report/dashboard rebuild.

## Guardrails
1. No direct main pushes by default.
2. No legacy gate aliases.
3. No manual bypass when enforced scripts exist.
4. No domain-specific logic inside shell control-plane files.

## Change management policy
1. One patch set equals one major governance concept.
2. High-risk surfaces follow ordered series: `contracts -> code -> docs`.
3. Repeated failure rule:
   - First failure: fix and retry.
   - Second failure: stop scope expansion and remediate process.

## Emergency policy
Direct-main emergency requires both:
- `AI_SHELL_EMERGENCY_MAIN_PUSH=1`
- `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON=<non-empty>`

Emergency path is for incident containment only.
