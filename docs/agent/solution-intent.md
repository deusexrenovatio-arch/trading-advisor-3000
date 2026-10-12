## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: the accepted L1 table has nonempty NULL-year files because its native writer omits the physical close-year payload. One focused native Arrow/Delta regression distinguishes authoritative UTC close from business open timestamps through both L1 writer entrypoints and the context constructor; it preserves existing carrier types, identity/history and business payload, and rejects missing close before deletion. Existing bad rows require a separate storage-only repair; no indicator calculation is repeated.
- Shortcut Waiver: none

Change surface: mixed. Product-plane L1 physical-storage payload/default and
required shell delivery metadata. Indicator DTOs carry authoritative bar_end_ts
outside their business serialization, equality and hashes. The existing source
constructor transfers that close; both L1 batch writers derive UTC close year
with the shared native Arrow helper and explicitly use the annual layout.
Existing Delta INT/BIGINT carrier types and table identity/history remain.
Legacy layout or missing authoritative close fails before schema/delete writes.

Self-review: the former business-only native append that produced NULL-year
partitions is forbidden. The single physical writer/context regression uses
real Arrow/Delta while indicator computation is mocked. Only existing synthetic
fixture close fields are populated for the stricter writer contract. Indicator
values, cache/hash rules, logical keys, timestamps, raw writers and other table
families are unchanged. The accepted L1@446 needs copy-only storage repair from
the already validated source close-year mapping, separately from this code fix.
