# Test Audit Guide

## Source Of Truth

- Current pytest collection: `python -m pytest --collect-only -q`.
- Generated master matrix: `docs/agent/audits/test-audit-matrix.csv`.
- Generated rollup: `docs/agent/audits/test-audit-summary.md`.
- Agent-owned updates: `docs/agent/audits/test-audit-updates/<BLOCK>.csv`.
- Synchronizer: `python scripts/sync_test_audit_matrix.py --write`.
- Drift check: `python scripts/sync_test_audit_matrix.py --check`.
- Campaign closeout: `python scripts/sync_test_audit_matrix.py --check --require-complete`.

The master CSV is generated. Agents must not edit it directly. Each agent owns one block update
fragment; the orchestrator regenerates the master after integrating fragments.

## Good Test Contract

A good test satisfies all of these principles:

1. **Observable behavior.** It exercises a stable public seam and asserts a result a caller,
   operator, downstream dataset, or external system can observe.
2. **Independent oracle.** Expected values come from a specification, worked example, fixed
   fixture, or independently derived invariant. The assertion must not repeat production logic.
3. **Mutation sensitivity.** A plausible behavior-breaking change makes the test fail for the
   intended reason.
4. **Refactor tolerance.** Internal names, helper layout, call order, and private collaborators may
   change without breaking the test while behavior remains unchanged.
5. **Realistic boundary.** Mocks replace expensive or uncontrollable external boundaries, not the
   behavior under test. Integration contracts use real serializers, storage formats, or runtime
   adapters where those are the contract.
6. **Deterministic isolation.** Time, randomness, filesystem state, environment, and test order are
   controlled. Repeated runs produce the same result.
7. **Meaningful failure coverage.** Important rejection, rollback, idempotency, and invalid-input
   behavior is asserted, not only the happy path.
8. **Correct execution lane.** Fast logic runs locally. Spark, Delta, PostgreSQL, Docker, Linux, or
   external-service behavior is proven in the lane that can execute it honestly.
9. **Owned skip.** A skip has a named infrastructure/capability reason and a mandatory proof lane.
   A permanent skip with no proof owner is a defect.
10. **Focused specification.** The name states one behavior and failures identify the broken
    contract without requiring source inspection to understand the expectation.

Source or AST inspection is acceptable only when source layout itself is the public governance
contract. Otherwise `source-structure` is a rewrite signal, not behavior proof.

## Decisions

| Decision | Meaning | Terminal state |
| --- | --- | --- |
| `good` | Test already proves a unique behavior contract | `reviewed` or `verified` |
| `rewrite` | Valuable contract, but current test proves the wrong thing | `verified` after replacement proof |
| `delete` | No unique contract, exact duplicate, obsolete behavior, or irredeemable noise | `retired` + `verified` |
| `infra-proof` | Test is valid only in an owned non-local lane | `verified` with lane evidence and zero mandatory skips |

`fixed` means code changed but final proof is still pending. It is not campaign completion.

## Problem Codes

| Code | Problem |
| --- | --- |
| `none` | No material defect; required for `good` |
| `implementation-coupled` | Asserts private calls, helper layout, call order, or internal names |
| `tautological` | Expected value repeats production logic or a constant asserts itself |
| `duplicate` | Another test proves the same contract with equal or stronger sensitivity |
| `weak-assertion` | Only checks non-null, type, count, exit zero, or mock invocation without semantics |
| `wrong-seam` | Observes a side channel instead of the supported interface |
| `mock-heavy` | Internal mocks replace the behavior that should be exercised |
| `source-structure` | Reads implementation source where layout is not the contract |
| `skip-unowned` | Skip has no mandatory proof lane or owner |
| `environment-mismatch` | Test is assigned to a lane that cannot execute its real dependency |
| `nondeterministic` | Outcome depends on time, randomness, order, network, or residual state |
| `slow-unbounded` | Runtime or data scope has no explicit bound appropriate to its lane |

Multiple problems use `|`, for example `implementation-coupled|weak-assertion`.

## Skip Policy

| Value | Use |
| --- | --- |
| `none` | Test has no skip behavior |
| `unreviewed` | Generated triage state; never terminal |
| `local-infra` | Local host cannot provide the required runtime; another mandatory lane does |
| `linux-docker-proof` | Docker/Linux lane executes the same behavior with zero skips |
| `optional-capability` | Capability is explicitly optional and absence is part of the contract |
| `external-service` | Owned external service controls availability and has a separate proof lane |
| `invalid` | Skip is unjustified and must be removed or rewritten |

## Required Evidence

Every terminal row records:

- `behavior_contract`: one concrete observable behavior, not an implementation description;
- `evidence`: exact focused command and result, plus lane for infrastructure proof;
- `reviewed_by`: stable agent or reviewer identifier;
- `problem_codes`: `none` for good tests, otherwise the diagnosed defects;
- `skip_policy`: resolved for every row carrying `skip-site`.

For deletion, evidence names the stronger remaining test or obsolete contract and confirms the
focused suite still passes. For rewrite, evidence names the replacement behavior test. For
`infra-proof`, evidence includes the mandatory lane result and `0 skipped`.

## Agent Workflow

1. Claim one block and touch only its tests, owned production files, and `<BLOCK>.csv` fragment.
2. Change fragment rows from `pending` to `in_review` while inspecting the public seam and contract.
3. Record one decision per nodeid. Static signals are triage hints, never automatic verdicts.
4. Apply fixes in vertical slices: failing behavior test, minimum implementation if needed, focused
   proof, then the next test.
5. Use structured CSV parsing/writing. Preserve the exact update schema and deterministic nodeid
   order.
6. Run `--write` and `--check` locally. Do not stage the generated master in an agent commit.
7. Stage only the package fragment and owned code/tests. Report changed files, commands, results,
   residual risks, and commit SHA.
8. The orchestrator integrates non-overlapping commits, regenerates the master once, runs all
   affected lanes, and publishes the wave PR.

Update fragment header:

```csv
nodeid,collection_state,audit_status,decision,problem_codes,skip_policy,behavior_contract,evidence,reviewed_by
```
