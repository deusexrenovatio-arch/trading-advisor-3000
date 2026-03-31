# Anti-self-deception controls для приёмки и закрытия F1

Дата: 2026-03-30

## 1. Иерархия truth sources
Порядок доверия должен быть жёстким:
1. runtime code + executable tests + immutable artifacts
2. `registry/stack_conformance.yaml`
3. `STATUS.md`
4. stack spec + ADR set
5. phase reports / red-team notes / route reports
6. prose docs and runbooks

Нижестоящий слой не имеет права переопределять вышестоящий.

## 2. Что НЕ считается доказательством
- README
- phase report
- checklist
- route-report
- существование scripts без факта их исполнения
- sample JSON/manifest without runtime read/write
- folder skeleton
- dependency in `pyproject.toml` without entrypoint/test evidence
- unit test that checks only file existence or doc wording

## 3. Required evidence model
Для каждой accepting phase обязателен пакет:
- `evidence-pack.json`
- список команд с exit-code
- SHA commit
- hashes всех produced artifacts
- environment manifest
- список negative tests
- mapping `claim -> evidence artifact`

## 4. Required negative tests by class

### Docs / claims
- unsupported removal claim in report
- forbidden closure wording while blockers remain
- drift between STATUS and registry

### Runtime surfaces
- durable profile without Postgres
- FastAPI entrypoint missing while claim says implemented
- Telegram chosen in spec but runtime remains custom-only
- sidecar smoke pointed to non-compiled stub
- contract payload changed without schema/fixture/test update

### Broker path
- no credentials
- connector unavailable
- kill-switch active
- recovery after restart
- invalid order replace/cancel path

## 5. Release blocker taxonomy
В registry должен появиться явный release taxonomy:
- `architecture_critical`
- `replaceable`
- `supporting_bounded`

И дополнительный признак:
- `release_gate_required=true|false`

Без этого F1 неизбежно будет интерпретироваться по-разному разными документами.

## 6. CI discipline
Нужны минимум три независимых acceptance lanes:
1. Linux / Python lane:
   - validator
   - contracts
   - Delta
   - Spark proof
   - Dagster proof
   - runtime/API tests
2. Windows /.NET sidecar lane:
   - build
   - test
   - publish
   - compiled-binary smoke
3. Release decision lane:
   - aggregate immutable artifacts
   - run red-team checks
   - emit explicit `ALLOW` or `DENY`

## 7. Acceptance wording discipline
Разрешены только claims, поддержанные truth-source state.
Нужно запрещать:
- `full acceptance`
- `production ready`
- `release ready`
- `live ready`
- `final closure`
если хоть один release-blocking surface ещё не закрыт.

## 8. Human process rule
Если report, route or checklist говорят `accepted`, а truth-source bundle показывает `planned/partial/not accepted`, верить надо bundle, а не report.
