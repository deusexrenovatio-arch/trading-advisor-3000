# Architecture As Docs

This package documents architecture for both repository surfaces:
- Delivery Shell architecture (governance/control-plane concerns)
- Product Plane architecture (application/runtime concerns)

Use `docs/architecture/trading-advisor-3000.md` as the single entry map when you
need a coherent whole-system view before reading the more specific documents.

## Start map
- `docs/project-map/current-truth-map-2026-05-05.md`
- `docs/project-map/documentation-currentness-map-2026-05-06.md`
- `docs/architecture/trading-advisor-3000.md`
- `docs/architecture/repository-surfaces.md`
- `docs/architecture/layers-v2.md`
- `docs/architecture/modules.md`

## Delivery Shell architecture index
- `docs/architecture/layers.md`
- `docs/architecture/architecture-map.md`
- `docs/architecture/layers-v2.md`
- `docs/architecture/entities-v2.md`
- `docs/architecture/architecture-map-v2.md`
- `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md`

## Product Plane architecture index
- `docs/architecture/product-plane/architecture-standard.md`
  - substantive review baseline consolidated from current accepted owners;
    exact schemas/APIs and implementation status remain with their owners
- `docs/architecture/product-plane/README.md`
- `docs/architecture/product-plane/STATUS.md`
- `docs/architecture/product-plane/CONTRACT_SURFACES.md`
- `docs/architecture/product-plane/product-plane-module-charters.md`
  - target modular structure: module ownership, hidden decisions, storage,
    runtime owners, proof surfaces, and dependency rules
- `docs/architecture/product-plane/product-plane-module-apis.md`
  - public communication contract between deep modules
- `docs/architecture/product-plane/research-plane-platform.md`
- `docs/architecture/product-plane/stack-conformance-baseline.md`

## Retired historical docs
- Retired legacy app-path docs are archived at
  `docs/archive/legacy-app-docs/2026-05-06/README.md`. Use
  `docs/architecture/product-plane/**` for current product-plane truth.
- The old product-plane spec v2 package is archived at
  `docs/archive/product-plane-spec-v2/2026-05-06/README.md`; do not use it as a
  current implementation source.
- The old governed-pipeline hardening specification was removed from the active
  architecture index during the 2026-06-10 process reset.

## Decision history and owners

Reconstructed 2026-10-25 from all 231 PRs opened through that date: 218 merged
(212 into main, 6 into production), 4 open, 9 closed without merge. Sources were
PR bodies, immutable file scopes and targeted owning code/tests; not every
review comment was recovered. Current baseline: main `78917b9efe57` (PR #238).
These are navigation groups, not 24 mandatory ADRs or a second status registry.
The owning document/code defines scope; merged implementation does not prove
deployment, dataset quality or live readiness. Unknown historical reasons stay
unknown. Read the [record threshold](docs/agent/architecture-review.md) before
adding or revising a choice.

| Area / reason for the choice | Owning explanation | History and current boundary |
| --- | --- | --- |
| Shell/product separation; hide decisions behind module APIs | [Surfaces](docs/architecture/repository-surfaces.md), [module APIs](docs/architecture/product-plane/product-plane-module-apis.md) | #2, #23–32 namespace cutover; #128 deep modules. Folder migration alone does not close tolerated API bridges. |
| Direct reproducible delivery; remove session machinery | [Workflow](docs/DEV_WORKFLOW.md) | #9–19/#39–51 built governed sessions; #122–127 retired default package/intake/session runtime; #174 direct workflow. |
| Tools/memory guide navigation; actual owners retain authority | [Entrypoint](docs/agent/entrypoint.md) | #44 MemPalace → #107 removal; #123–124 reset; #174 removed global skill dependency. Available Serena-first routing remains. |
| Native owners avoid competing writers and Python history scans | [Native ownership](docs/architecture/product-plane/native-runtime-ownership.md) | #72, #108–115, #145–149. Importing a library alone proves no owning execution path. |
| Materialize reusable L0/L1/L2 outside strategy loops | [Research platform](docs/architecture/product-plane/research-plane-platform.md) | #48, #63–66. Closed #57–59 are predecessors, not delivered changes. |
| Exchange/session semantics separate from physical UTC layout | [Sessions](docs/runbooks/app/canonical-session-delivery.md), [clock](docs/runbooks/app/moscow-clock-delivery.md) | #61/#114/#117–118/#171/#181/#183; UTC layout #186. Session/date and close-year carriers have different meanings. |
| Causal continuous signal prices; native execution economics | [Continuous front](docs/architecture/product-plane/continuous-front-indicator-storage.md) | #73–105, #139. #101–102 calendar/session policy supersedes a blanket OI-default claim; effective route/config still matters. |
| Missing economics stay explicit; official and modelled inputs differ | [Economics](docs/runbooks/app/historical-economics-delivery.md) | #164/#166/#168/#182/#184/#189/#193/#203/#222. Price acceptance alone does not establish monetary readiness. |
| Stable Delta identities/history; governed physical layout | [Storage owner index](docs/architecture/product-plane/README.md) | #178, #190–198. Isolated finalizer proof does not establish complete deployed-table acceptance. |
| Immutable snapshots/exact readers; CAS publication authority | [Publication](docs/runbooks/app/data-research-snapshot-publication.md) | #190, #192, #231. Physical descriptors adapt separately from immutable v1/logical compute meaning. |
| Dagster owns campaigns; data freshness is a separate prerequisite | [Campaign route](docs/runbooks/app/research-campaign-route.md) | #152–153, #176–177, #202, #212. Legacy run_campaign is retired; checkpoints require exact input pins. |
| Native family evaluation and adaptive search | [Research platform](docs/architecture/product-plane/research-plane-platform.md) | #79–91. vectorbt owns simulation; Optuna owns adaptive search; Delta owns accepted artifacts. |
| Validation precedes promotion; blind confirmation stays hidden | [Evaluation](docs/architecture/product-plane/strategy-evaluation-layer-v1.md) | #130–131 replace closed #129; optional nested walk-forward stays explicit. policy_pass is not live readiness. |
| Futures monetary authority split is explicit | [Money authority](docs/architecture/product-plane/futures-backtest-money-authority.md) | #175. Ledger monetary metrics coexist with vectorbt ratios; mixed metric basis remains a validation question. |
| Read, compute, write and publication increments need distinct proof | [Indicator refresh](docs/runbooks/app/indicator-refresh.md) | #214–228 reduce work in different layers. #232 tail compute remains an open draft at audit cutoff; suffix writes prove no full-path speedup. |
| Native multi-TF strategies without persisted duplicate overlays | [Indicator storage](docs/architecture/product-plane/continuous-front-indicator-storage.md) | #188 repaired old projections; #225 retired persisted MTF; #226 adopted metadata. Native multi-TF input contracts remain. |
| Numerical semantics are distinct from release/QC/physical changes | [Semantic contract](docs/architecture/product-plane/semantic-compute-contract.md) | #224/#229 finite bridges → #230 general contract; #231 descriptors; #236 bounded audited alias. |
| Daily admission preserves exact dirty ancestry; rebuild is explicit | [Refresh admission](docs/architecture/product-plane/research-refresh-admission.md) | #173/#210/#218–220 → #234–235 NOOP/SCOPED/BLOCKED; unknown scope no longer authorizes daily full fallback. |
| Recover unknown ancestry through a native audited metadata bridge | [Scope recovery](docs/architecture/product-plane/data-scope-recovery.md) | #236. Old manifests stay immutable; Data CAS is not an atomic transaction across Data and Research registries. |
| Runtime owns candidate/event/publication state | [Runtime lifecycle](docs/architecture/product-plane/runtime-lifecycle.md), [status](docs/architecture/product-plane/STATUS.md) | #2 replay baseline → #15 durable/transport paths. Memory MVP prose is historical; bootstrap /ready does not prove downstream health. |
| Replaceable broker boundary; live input has strict evidence gates | [Execution](docs/architecture/product-plane/execution-flow.md), [status](docs/architecture/product-plane/STATUS.md) | #15/#20/#37. Controlled/stub evidence is distinct from bound real-broker submit/cancel/fill lifecycle. |
| Bound local reuse; fresh CI and actual behavior proof | [Checks](docs/agent/checks.md) | #207. Green mocks or local cache alone cannot establish native product/runtime readiness. |
| Independent architecture admission within existing PR lane | [Review policy](docs/agent/architecture-review.md), [ADR 0003](docs/architecture/adr/0003-durable-decision-records.md) | #233 and this change. #201 disabled automatic CodeRabbit reviews; no new lane/registry. |
| Journal/Trade Workspace remains a separate candidate line | [Accepted product status](docs/architecture/product-plane/STATUS.md) | No merged source/public route in the audited main baseline. Local candidate/WIP is not main integration or deployment proof. |

PR numbers refer to this repository's [pull request history](https://github.com/deusexrenovatio-arch/trading-advisor-3000/pulls?q=is%3Apr).
Production-target #146 is implementation, alongside five later promotion PRs;
do not flatten all six into release-only history. PR number order is not merge
chronology. Historical commit identity differs for #48/#52/#53, but each recorded
merge has an exact full-tree equivalent on main; the cause is unknown.

## Shared references
- `docs/architecture/glossary.md`
- `docs/architecture/trading-advisor-3000.md`

## ADRs
- `docs/architecture/adr/README.md`
- `docs/architecture/adr/0001-shell-boundaries.md`

## Sync flow
- source docs:
  - `docs/architecture/layers-v2.md`
  - `docs/architecture/entities-v2.md`
- generated map:
  - `docs/architecture/architecture-map-v2.md`
- command:
  - `python scripts/sync_architecture_map.py`

## Validation
- `python scripts/validate_architecture_policy.py`
- `python -m pytest tests/architecture -q`
