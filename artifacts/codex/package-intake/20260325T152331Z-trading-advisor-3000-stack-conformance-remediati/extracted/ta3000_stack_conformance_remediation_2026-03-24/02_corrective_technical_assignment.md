# Corrective technical assignment

## 1. Objective

Repair the repository so that:

1. acceptance claims cannot outrun executable reality;
2. every declared technology surface is either:
   - truly implemented and proven by runtime evidence, or
   - formally removed/replaced through an ADR and spec update;
3. future product work can safely build on the repo without inheriting false closure.

## 2. Scope

This assignment covers:

- acceptance vocabulary and truth-source repair;
- machine-verifiable stack-conformance registry and validators;
- closure of architecture-critical surfaces;
- explicit implement-or-de-scope decisions for replaceable technologies;
- re-acceptance of product phases under the repaired model.

This assignment does **not** require broad feature expansion beyond what is necessary to restore architectural honesty and future-safe foundations.

## 3. Hard rules

### 3.1 No proxy evidence

The following must not count as closure on their own:

- manifest != Delta runtime
- SQL string != Spark execution
- asset spec != Dagster materialization
- plain Python class != FastAPI service
- in-memory publication engine != aiogram integration
- wire contract README != .NET sidecar
- sample metrics/log files != OpenTelemetry
- custom backtest engine != vectorbt usage
- custom checksum runner != Alembic usage

### 3.2 Implement-or-ADR

For every technology marked `chosen` / `selected` in the product spec:

- either land executable proof,
- or land an ADR that replaces/removes it and update:
  - product spec,
  - app docs,
  - checklists,
  - stack-conformance registry.

### 3.3 Acceptance vocabulary is restricted

Allowed terms:

- `implemented`
- `partial`
- `scaffold`
- `removed_by_adr`
- `accepted_as_scaffold`
- `accepted_as_runtime_closure`
- `accepted_as_operational_closure`

Forbidden terms until full proof exists:

- `full DoD`
- `full acceptance`
- `production ready`
- `live ready`

### 3.4 Shell-sensitive changes remain isolated

Any change touching:

- `.github/workflows/*`
- `docs/agent/*`
- `scripts/*`
- root governance configs

must land as a separate governance patch from product-plane implementation patches.

### 3.5 Re-acceptance is evidence-led

No phase may be re-closed from narrative review alone. Each phase requires:

- direct executable proof,
- generated evidence artifact,
- at least one disprover / negative test,
- registry status update.

## 4. Required deliverables

### Governance / truth-model deliverables

- `docs/architecture/app/STACK_CONFORMANCE.md`
- `docs/architecture/app/ACCEPTANCE_VOCABULARY.md`
- `registry/stack_conformance.yaml`
- `scripts/validate_stack_conformance.py`
- `tests/process/test_validate_stack_conformance.py`
- PR template / acceptance template updates
- checklist rewrites removing false “full DoD” wording

### Architecture-critical implementation deliverables

- physical Delta write/read path
- runnable Spark local execution path
- runnable Dagster materialization path
- default durable runtime entrypoint with Postgres
- real in-repo .NET sidecar project implementing current wire contract

### Replaceable-stack decision deliverables

For each of:
`FastAPI`, `aiogram`, `vectorbt`, `Alembic`, `OpenTelemetry`, `Polars`, `DuckDB`

one of:

- implementation patch + executable proof, or
- ADR + spec/docs/registry update proving the replacement choice.

## 5. Success criteria

The assignment is complete only when all are true:

1. No reviewed document overclaims beyond registry state.
2. `validate_stack_conformance.py` fails on ghost technologies and mismatched claims.
3. Architecture-critical surfaces are no longer `scaffold`.
4. Replaceable surfaces are either implemented or removed by ADR.
5. Phase checklists and acceptance notes are regenerated under the repaired model.
6. A black-box re-acceptance pack is produced for release review.

## 6. Non-negotiable acceptance artifact fields

Every phase closure artifact must contain:

- git SHA
- phase id
- surfaces touched
- commands executed
- command exit codes
- produced artifacts
- negative tests executed
- registry diff
- reviewer verdict

Missing any field => closure invalid.

## 7. Decision policy for ambiguous technologies

### Architecture-critical

These must not remain ambiguous:

- Delta
- Spark
- Dagster
- default durable runtime
- real sidecar

### Replaceable

These may be replaced, but only formally:

- FastAPI
- aiogram
- vectorbt
- Alembic
- OpenTelemetry
- Polars
- DuckDB

## 8. Expected outcome

After this remediation, the repository can serve as a reliable foundation for staging and later production expansion without leaking scaffold assumptions into live operations.
