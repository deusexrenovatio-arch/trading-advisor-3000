# Expired contract descriptions in daily native history renewal

Change surface: product-plane. Architecture impact: within-contract.
Owner: Market Data Foundation / MOEX native history renewal.
Decision status: selected, subject to independent PR design admission.
Implementation/deployment status: established separately by the PR checks and
production readback; this decision record does not claim either is complete.

The daily owner remains `native_history_refresh.refresh_native_history_support`.
Its current securities/dated-MC crosscheck still validates all reviewed assets.
When an expired contract has disappeared from current securities, resolve its
description from the prepared input TableID/version already admitted by
`validate_contract_parameter_evidence` and `bind_contract_parameter_input`.
Never substitute the mutable latest prepared version or omit the historical row.

Read only the exact contract, asset, RFUD board and prepared source, at or before
the requested history date, with Delta predicates and column projection. Select
the unique latest observation. Require reviewed finite positive MINSTEP/LOTVOLUME,
a known last-trade date covering the history date and preceding the acquisition
date in Moscow, coherent delivery date, and intact stored provenance. Derive that
Moscow date once from the recorded acquisition timestamp. The stored document
hash binds the original provenance JSON bytes, not a reserialization. Preserve
the original description hash and exact archived table/version/row reference.
Keep that evidence separate from today's asset-level crosscheck; neither is a
daily official historical specification. Raw MOEX history/OI remains unmodified.

This reuses admitted static-parameter continuation, including the existing fresh
bundle (`bounded-tail-20261010-v1`, prepared version 1). Descriptions may retain
the earlier admitted receipt; do not relabel them with a later receipt. Missing,
conflicting, corrupt, not-yet-expired, post-expiry or unit-mismatched descriptions
fail before support writes. A future contract absent from the admitted version
needs separate source adoption; there is no latest fallback.

Alternatives: keeping current-only lookup breaks on NGV6 expiration; skipping
history loses official observations; mutable latest or a new unreviewed external
descriptor does not retain admitted input identity. Reuse the existing input
instead. Cost is a bounded metadata projection per missing contract/history date.
No new state owner, writer, schema, economics formula or publication override.
Migration: no schema or admission-bundle migration. Existing rows change only
through the normal scoped renewal/retry, without a separate historical rewrite.
Rollback reverts this resolver; saved pending windows remain the recovery input.

Sources: [renewal owner](../../../src/trading_advisor_3000/product_plane/data_plane/moex/native_history_refresh.py),
[admission](../../../src/trading_advisor_3000/product_plane/data_plane/moex/contract_parameter_continuation.py),
[regression tests](../../../tests/product-plane/unit/test_moex_native_history_refresh.py),
[operator runbook](docs/runbooks/app/moex-contract-parameter-continuation.md).

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: none
- Closure Evidence: NGV6 regression and native Delta integration test; bounded
  readback of the admitted production input. Production baseline, canonical
  dataset and downstream research publication proof follow PR promotion.
- Shortcut Waiver: none

Chosen path: reuse the admitted immutable input and existing Delta/Dagster owners.
This is partial closure for expired contracts already represented in that input;
future shape preserved: explicit evidence adoption for additional descriptions.
Unit fixtures establish behavior, not production materialization. Admission uses
the [architecture standard](docs/architecture/product-plane/architecture-standard.md) and
[native runtime ownership](docs/architecture/product-plane/native-runtime-ownership.md).
