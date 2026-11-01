# Identity fixtures

All v3 bar fixtures and instrument_namespace.v1 / temporal_mapping.v1 here are
synthetic contract examples. Names SBER, IMOEX, SRZ6 and SRH7 are illustrative
labels; namespace IDs, roles, venue/board assignments, deliverable, expiry dates,
sizes, currencies, prices, recipes and mapping/membership intervals are NOT
confirmed exchange facts. The all-zero source hash identifies no verified source.
No fixture establishes actual listing, index membership, tradability, economics,
historical mapping, source availability or runtime migration.

SBER and IMOEX demonstrate native instruments without contract_id. SRZ6 shows
a distinct concrete derivative. CF spans two component instruments and observes
the first as active at bar open. Tests move that observation to the second
without changing the expanded CF key. Namespace input is a bounded supplied
snapshot for pure validation, never a new runtime registry.

The snapshot_identity_selector.v1 and snapshot_identity_binding.v1 fixtures are
also synthetic: repeated-letter hashes, table/context labels and observation
dates establish no real source or historical applicability. Source-backed
admission is verified separately against the original pinned pilot artifacts.

Snapshot read request/projection examples are synthetic Arrow metadata fixtures.
Their schema bytes, buffer hashes, quantities and dates demonstrate the carrier;
they are not exchange inputs or evidence of historical/PIT applicability.
