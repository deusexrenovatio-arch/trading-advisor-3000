# Contract Change Policy

## Scope
This policy governs every release-blocking public envelope listed in:

- `src/trading_advisor_3000/product_plane/contracts/schemas/release_blocking_contracts.v1.yaml`
- `docs/architecture/product-plane/CONTRACT_SURFACES.md`

## Non-Negotiable Rule
Any public payload change is invalid unless schema, fixture, and tests are updated together in one patch.

## Compatibility Classes
1. `strict`
- Existing fields are immutable in meaning and type inside the same major version.
- Removing or renaming a field requires a new major schema version.
- Tightening enum/validation for an existing field requires a new major schema version.

2. `additive`
- Optional metadata can be added in the same major version.
- Existing field semantics cannot be repurposed.
- Removing existing fields or changing existing field meaning requires a new major schema version.

## Versioning Rules
1. `v1` files are immutable once merged.
2. Breaking changes must add a new `vN` file and keep previous versions available.
3. Fixtures must exist per active schema version.
4. Tests must cover schema-valid fixtures and at least one fail-closed mutation path.

## Merge Gate Expectations
1. Inventory and compatibility map remain complete for all release-blocking boundaries.
2. Contract tests fail when a payload drifts from the declared schema.
3. Truth-source docs and registry claims stay aligned (`contracts_freeze` cannot be marked implemented with missing contract artifacts).
