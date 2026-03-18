# Agent Practices Alignment

## Purpose
Align operator request quality with machine-enforced loop behavior.

## Request Contract
Every active task note must include:
- `## Task Request Contract`
- measurable objective and done evidence
- explicit in-scope and out-of-scope boundaries
- constraints and priority rule

Validation:
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`

## First-Time-Right
Before implementation:
1. confirm scenario coverage (success, edge, failure),
2. confirm acceptance evidence commands,
3. confirm budget and stop/replan threshold.

Enforcement lives in:
- `docs/checklists/first-time-right-gate.md`
- `python scripts/validate_task_request_contract.py`

## Repetition Control
If the same path fails twice:
1. stop scope expansion,
2. execute remediation flow,
3. reopen with narrowed hypothesis and explicit next probe.

Primary references:
- `configs/agent_incident_policy.yaml`
- `docs/runbooks/governance-remediation.md`

## Loop Gate Linkage
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD` is canonical hot-path enforcement.
- Request contract and repetition controls are non-optional inputs to loop execution.
