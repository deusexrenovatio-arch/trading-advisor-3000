# Research Semantic Compute Contract

Date: 2026-10-25. Change surface: product-plane.
Owner: Research accepted-snapshot and compute-contract owners.

## Status and scope

This records the existing choice delivered by [PR #230](https://github.com/deusexrenovatio-arch/trading-advisor-3000/pull/230),
with scoped adaptations in [#231](https://github.com/deusexrenovatio-arch/trading-advisor-3000/pull/231)
and [#236](https://github.com/deusexrenovatio-arch/trading-advisor-3000/pull/236).
Implementation is present in main `78917b9efe5743332a94098c2906a8b9b7d995e2`.
This record establishes no current runtime binding, fresh numerical parity or
production publication. Compatibility still requires the owning admission proof.

## Context and alternatives

Whole-application source hashes invalidated accepted Research when only writers
or observation-only telemetry changed. Finite audited release transitions in
[#224](https://github.com/deusexrenovatio-arch/trading-advisor-3000/pull/224) and
[#229](https://github.com/deusexrenovatio-arch/trading-advisor-3000/pull/229) handled
particular migrations, but a new release alias for every operational edit did
not explain which numerical meaning had changed. PR #230 introduced a persisted
semantic contract while retaining exact runtime provenance for its own purpose.

## Decision and authority

`parameters.semantic_compute_contract` in Research pending checkpoints and
accepted manifests carries `research-compute.v1`. Its versioned components bind
the fingerprint authority, numerical kernel dependencies, active profiles,
logical schemas, numerical library versions, quality rules and proof codec.
Kernel identity follows bounded resolved project-symbol/AST dependencies;
unknown or unsupported dynamic dependencies fail closed.

| Evidence | Meaning |
| --- | --- |
| Numerical kernels, profiles and dependencies | Whether prior numerical work remains compatible |
| Quality-rule identity | Whether accepted quality evidence may be reused |
| Logical schema and proof identity | Whether metadata/proof interpretation remains compatible |
| Physical snapshot descriptor | Exact native fields/types/partitions at the pinned Delta version |
| Raw source/runtime provenance | Exact operation, resume, handoff and publication binding |

Physical descriptors and logical compute schemas have different owners and
purposes. Neither a matching semantic digest nor a physical descriptor alone
proves source coverage, accepted QC or permission to publish.

## Changed components and consequences

- A kernel/profile/numerical-library change invalidates the old numerical
  contract. Compatible writers or observation-only edits do not do so merely
  because an application source release changed.
- Quality-only changes require current pinned-output revalidation. Schema/proof
  changes require supported native adaptation. These are explicit admission
  outcomes; they do not silently authorize a numerical full rebuild.
- A changed fingerprint authority requires a supported migration. The semantic
  resolver is a versioned trust boundary, not a general proof of mathematical
  equivalence between arbitrary formulas or refactors.
- Raw exact provenance remains strict within a run, resume, CF handoff and
  publication. `native_compute_scope` captures one operation identity and checks
  raw code/dependency drift at exit; it is not a process-wide release freeze.
- Source pins, complete ancestry/scope, successful owners, QC and publication
  checks remain independently mandatory. An explicit rebuild has separate intent.

## Scoped compatibility history

The presemantic bootstrap admits only its audited legacy inputs/recipe against
a fixed baseline. It is not an expandable list of generally equivalent releases.
PR #231 separately proves a specific logical source-schema correction against
native physical pins; old immutable manifests are not rewritten.

PR #236 permits one exact audited contextvars engine / sidecar pair-restriction
transition. `numerical_engine_identity` maps only the recorded engine identity;
raw semantic contracts and entry/exit drift checks retain the actual engine.
Unknown engines, changed kernels/dependencies and unrelated contract components
remain distinct. This allowance is a bounded migration, not a universal alias.

## Explicit isolated contract migration

The CF tail/checkpoint rollout changes CF/L1/L2 kernels, the fingerprint
authority, proof interpretation and logical schemas. It is not covered by the
operational allowance above. Old numerical outputs, checkpoints and quality
evidence must not be relabelled as compatible.

An explicit migration plans a complete `operation="rebuild"` in a new isolated
root. The plan binds the accepted source Research manifest, current Data parent
and native input pins, declared calculation domain, target compute digest and
all output roots. Apply revalidates these bindings before writing. Unknown or
stale evidence, overlapping roots and consumed/nonempty targets fail closed.

The existing complete data-prep asset selection, native runtimes, quality owners
and snapshot publisher perform the rebuild. Only the new verification registry
is published; source manifests and current pointers remain unchanged. Subsequent
daily admission must consume that newly accepted contract. This path does not
authorize production promotion or an automatic rebuild from a blocked sensor.

Bounded CF bars retain absolute roll epochs. Their adjustment ladder must retain
the causal prefix through the end bound, including entries before the requested
bar start. Direct bounded replacement deletes the same ladder prefix before
writing it again; refresh-window tail replacement retains its existing bounds.
This preserves L1's input contract without duplicate historical ladder keys.

Literal hashes, CLI and operational application evidence belong to the
[snapshot publication runbook](docs/runbooks/app/data-research-snapshot-publication.md).
Rollback must retain compatible code, immutable manifests and native pins;
this document authorizes no metadata rewrite or exemption from admission.

## Open questions and evidence

Supported future authority versions and the removal/expiry of migration
allowances require an owner decision with scoped proof. Similar-looking source
or matching output samples do not establish general equivalence.

- [Compute contract](src/trading_advisor_3000/product_plane/research/compute_contract.py): `semantic_compute_contract`, `compare_contracts`, `native_compute_scope`.
- [Fingerprint authority](src/trading_advisor_3000/product_plane/research/semantic_fingerprint.py) and [compatibility owner](src/trading_advisor_3000/product_plane/research/hash_compatibility.py).
- [Contract tests](tests/product-plane/unit/test_research_compute_contract.py): future operational releases, real helper mutations, quality/proof changes and raw drift.
- [Admission migration tests](tests/product-plane/unit/test_admission_compatibility_migration.py): exact engine allowance, invalidation and old-carrier suffix preservation.
- [Physical descriptor tests](tests/product-plane/unit/test_explicit_manifest_schemas.py).
- [Research operations](docs/runbooks/app/research-plane-operations.md) retains execution procedures and evidence boundaries.

These locations establish what to verify; no new execution result is claimed.
