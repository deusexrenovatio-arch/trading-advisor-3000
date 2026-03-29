# Anti-self-deception controls

## 1. Problem to prevent

The repository drifted because proxy evidence was allowed to stand in for runtime truth.

This control package prevents a repeat.

## 2. Evidence hierarchy

From weakest to strongest:

1. design intent in spec/docs
2. file existence
3. manifest / schema / contract declaration
4. unit tests over in-memory objects
5. dependency declaration
6. bootable entrypoint
7. black-box execution against a real process/runtime
8. generated artifact inspection
9. negative / disprover test
10. release-lane reproducibility

**Rule:** no closure decision may rely only on layers 1–4.

## 3. No-proxy-evidence table

| Claimed surface | Proxy evidence that must not count | Required real proof |
| --- | --- | --- |
| Delta | schema manifest, `format="delta"`, JSONL outputs | physical Delta table with `_delta_log` and successful read |
| Spark | SQL plan strings | executed Spark job |
| Dagster | asset spec metadata | asset materialization through Dagster runtime |
| FastAPI | plain Python API class | booted ASGI app and HTTP smoke |
| aiogram | in-memory publication engine | bot adapter proof using aiogram or ADR removal |
| .NET sidecar | README + contract + Python adapter | compiled sidecar process implementing the contract |
| OpenTelemetry | metrics/log sample files | exported telemetry via OTel path or ADR removal |
| vectorbt | “research engine exists” | actual vectorbt use or ADR removal |
| Alembic | any migration file | Alembic runtime use or ADR removal |

## 4. Registry-first acceptance

All closure is mediated by `registry/stack_conformance.yaml`.

Each entry must include:

- `surface`
- `category` (`architecture_critical` or `replaceable`)
- `target_status`
- `current_status`
- `decision` (`implement` / `removed_by_adr`)
- `runtime_entrypoints`
- `tests`
- `artifacts`
- `adr`
- `docs`
- `owner`

If any required field is missing, validator fails.

## 5. Disproof-oriented review

Every phase must contain at least one test that tries to prove the phase **did not** really close.

Examples:

- Delta phase: delete `_delta_log`, keep manifest -> fail expected
- Spark phase: bypass execution, keep SQL plan -> fail expected
- Sidecar phase: kill compiled sidecar, keep Python adapter -> fail expected
- Runtime phase: remove Postgres DSN, ensure no fallback to in-memory in staging/prod profile

## 6. Claim linting

Introduce a doc linter that rejects these phrases unless registry allows them:

- `full DoD`
- `full acceptance`
- `operational readiness`
- `production readiness`
- `live-ready`

This linter must support allowlists only when:
- registry says all required surfaces are implemented, and
- an acceptance artifact exists for the same git SHA.

## 7. Acceptance artifacts are immutable evidence

Store acceptance artifacts as generated outputs with:

- git SHA
- generated timestamp
- phase
- commands
- exit codes
- artifact hashes
- validator summary

Do not hand-edit them.

## 8. PR template controls

Every PR touching product-plane runtime must answer:

1. Which target-stack surfaces are touched?
2. For each touched surface, what is its registry state before/after?
3. What disprover was added?
4. Which runtime entrypoint was executed?
5. Which acceptance artifact was generated?

Missing answers => PR not acceptable for closure.

## 9. Separation of concerns

Do not mix:

- governance repair,
- stack-conformance validator changes,
- runtime technology closure,
- production rollout claims

in one patch set.

This keeps reviews honest and makes failures easier to localize.

## 10. Release board rule

A release or “go live” review must consume:

- stack conformance report
- phase acceptance artifact pack
- red-team checklist result
- open ADR replacement list
- known limitations list

No release decision from green unit tests alone.
