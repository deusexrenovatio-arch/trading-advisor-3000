# ADR 0001: Shell Boundary Governance

## Status
Accepted

## Context
The repository must host only AI delivery shell mechanics and explicitly avoid product business logic.

## Decision
1. Keep `run_loop_gate.py` as canonical hot-path gate entrypoint.
2. Keep `docs/session_handoff.md` as a pointer-shim, not narrative history.
3. Keep `plans/items/` canonical and generate `plans/PLANS.yaml` for compatibility.
4. Enforce PR-only main with neutral emergency variables.
5. Exclude domain-specialized skill packs from baseline.

## Consequences
- Process quality and traceability increase.
- Onboarding is faster due to deterministic source-of-truth layers.
- Product logic remains isolated from governance shell.
