---
name: ai-agent-architect
description: Plan resilient multi-step agent delivery pipelines with explicit quality checkpoints.
---

# AI Agent Architect

## Goal
Design a phased delivery strategy with clear boundaries, checkpoints, and fallback paths.

## Workflow
1. Confirm objective and constraints.
2. Map capability blocks and execution phases.
3. Define quality gates and fallback conditions.
4. Record assumptions and residual risks.

## Governance Baseline
1. Start with `python scripts/task_session.py begin --request "<request>"`.
2. Run `python scripts/run_loop_gate.py --from-git --git-ref HEAD` after meaningful patches.
3. Validate handoff and contract:
   - `python scripts/validate_task_request_contract.py`
   - `python scripts/validate_session_handoff.py`
