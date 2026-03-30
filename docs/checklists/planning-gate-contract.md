# Planning Gate Contract Checklist

## Purpose
- Block lazy phase slicing between a hard target TZ and execution-phase briefs.
- Keep release-oriented planning anchored to the real target environment and proof class.

## Mandatory Execution Contract
- `## Release Target Contract`
- `- Target Decision:` final decision being pursued, for example `ALLOW_RELEASE_READINESS`.
- `- Target Environment:` the real environment the final decision is about.
- `- Forbidden Proof Substitutes:` evidence classes that are not enough for the target.
- `- Release-Ready Proof Class:` the minimum proof class required for the final `ALLOW`.
- `## Mandatory Real Contours`
- one or more bullets in the form `- <contour_id>: <what must be real>`
- `## Release Surface Matrix`
- one or more bullets in the form:
  `- Surface: <surface_id> | Owner Phase: <phase_id> | Required Proof Class: <proof_class> | Must Reach: <terminal_state>`

## Mandatory Phase Brief Addendum
- `## Release Gate Impact`
- `- Surface Transition:` which release-blocking surface changes state in this phase, or `none`.
- `- Minimum Proof Class:` one of `doc`, `schema`, `unit`, `integration`, `staging-real`, `live-real`.
- `- Accepted State Label:` one of `prep_closed`, `real_contour_closed`, `release_decision`.
- `## Release Surface Ownership`
- `- Owned Surfaces:` comma-separated release surface ids from the execution contract matrix.
- `- Delivered Proof Class:` one of `doc`, `schema`, `unit`, `integration`, `staging-real`, `live-real`.
- `- Required Real Bindings:` explicit real systems/channels/environments, or `none` only for non-real surfaces.
- `- Target Downgrade Is Forbidden:` `yes` or `no`
- `## What This Phase Does Not Prove`

## Hard Rules
- `ALLOW_RELEASE_READINESS` must bind to a real target environment, not a demo or abstract contour.
- `live-real` target proof must not be silently downgraded to `staging-real`, `integration`, `schema`, or `doc`.
- Every mandatory real contour must appear in the release surface matrix and be owned by a non-`prep_closed` phase.
- A phase that closes only planning, docs, contracts, or validator work must not use `release_decision` as its accepted state label.
- A phase brief must say explicitly what it does not prove; otherwise the phase is invalid for release planning.
- A phase that does not move any release-blocking surface must not be presented as release closure.
- A release surface may have only one owning phase in the matrix.
- A phase may not claim ownership of a release surface that is missing from the matrix.

## Rejection Rules
- Missing `## Release Target Contract` in the execution contract.
- Missing `## Mandatory Real Contours` or `## Release Surface Matrix` in the execution contract.
- Missing `## Release Gate Impact` or `## What This Phase Does Not Prove` in a phase brief.
- Missing `## Release Surface Ownership` in a phase brief.
- Invalid proof class or accepted state label.
- `Release-Ready Proof Class` weaker than `live-real` for an `ALLOW_RELEASE_READINESS` target.
- Phase brief implies final release closure while its accepted state label is only `prep_closed` or `real_contour_closed`.
- Mandatory real contour mapped only to `doc`, `schema`, `unit`, or `integration`.
- Mandatory real contour owned by a `prep_closed` phase.

## Enforcement
- Validate with:
  - `python scripts/validate_phase_planning_contract.py`
  - `python scripts/validate_task_request_contract.py`
  - `python scripts/validate_session_handoff.py`
