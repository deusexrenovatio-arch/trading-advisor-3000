# Planning Gate Contract Checklist

## Purpose
- Block lazy phase slicing between a hard target TZ and execution-phase briefs.
- Keep release-oriented planning anchored to the real target environment and proof class.

## Mandatory Execution Contract
- `## Release Target Contract`
- `- Target Decision:` final decision being pursued, for example `ALLOW_RELEASE_READINESS`.
- `- Target Environment:` the real environment the final decision is about.
- `- Mandatory Real Contours:` real external contours that must exist before `ALLOW`.
- `- Forbidden Proof Substitutes:` evidence classes that are not enough for the target.
- `- Release-Blocking Surfaces:` surfaces that must reach a terminal state before `ALLOW`.
- `- Release-Ready Proof Class:` the minimum proof class required for the final `ALLOW`.

## Mandatory Phase Brief Addendum
- `## Release Gate Impact`
- `- Surface Transition:` which release-blocking surface changes state in this phase, or `none`.
- `- Minimum Proof Class:` one of `doc`, `schema`, `unit`, `integration`, `staging-real`, `live-real`.
- `- Accepted State Label:` one of `prep_closed`, `real_contour_closed`, `release_decision`.
- `## What This Phase Does Not Prove`

## Hard Rules
- `ALLOW_RELEASE_READINESS` must bind to a real target environment, not a demo or abstract contour.
- `live-real` target proof must not be silently downgraded to `staging-real`, `integration`, `schema`, or `doc`.
- A phase that closes only planning, docs, contracts, or validator work must not use `release_decision` as its accepted state label.
- A phase brief must say explicitly what it does not prove; otherwise the phase is invalid for release planning.
- A phase that does not move any release-blocking surface must not be presented as release closure.

## Rejection Rules
- Missing `## Release Target Contract` in the execution contract.
- Missing `## Release Gate Impact` or `## What This Phase Does Not Prove` in a phase brief.
- Invalid proof class or accepted state label.
- `Release-Ready Proof Class` weaker than `live-real` for an `ALLOW_RELEASE_READINESS` target.
- Phase brief implies final release closure while its accepted state label is only `prep_closed` or `real_contour_closed`.

## Enforcement
- Validate with:
  - `python scripts/validate_phase_planning_contract.py`
  - `python scripts/validate_task_request_contract.py`
  - `python scripts/validate_session_handoff.py`
