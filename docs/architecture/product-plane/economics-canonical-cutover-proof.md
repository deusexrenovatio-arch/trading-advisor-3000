# Economics canonical cutover: native proof

Surface: product-plane. Date: 2026-11-04. This is pre-production admission
evidence; production activation is a separate recorded operation.

## Accepted numerical baseline

Owner accepts existing production calculations as the migration oracle. Full
native parity receipt: `.tmp/economics-production-parity-20261104/accepted-production-baseline.json`.
Original complete publication: `2d4febedf3724f88a47d7f1bf319a9d4`.
Parameters: TableID `d550b9e4-e3d6-40dc-93f0-fc0e420e3b43`, v20, 124,347 rows.
Economics: TableID `58418381-67b9-4c02-8cb9-8c6d2f8829aa`, v18, 124,256 rows.

Exact physical copies retained table IDs and Delta versions in an isolated
workspace. All other selected tables were pinned read-only. No working table or
production publication was written during these proofs.

## Results

- Final full-history cutover and independent verify: PASS, snapshot
  `a2a5e29265cf42c6aa54f02ecc2c14da`; all 21 original parameter columns and all
  39 economics columns preserved, differences 0. Every initial canonical origin
  resolves to the accepted old pin/key. Historical RAW logical hashes and
  payload identity are verified across all 124,347 rows.
- Native Linux recovery/rollback suite: 7 PASS. Covers native commits before
  acknowledgement, saved-candidate identity loss, immutable acceptance after
  external receipt removal, complete CAS publication, selected PASS-NOOP
  descendants, partial abort and schema/data RESTORE, competing writers and
  contradictory/malformed source refusal.
- Unit/regression suite: 271 PASS. Includes real RAW22/canonical24 renewal,
  received `source_pages`, retry without native rewrites, changed admission,
  untracked executable release refusal, canonical/legacy source-unavailable
  publication aliases and unpublished economics refusal.
- Standalone bootstrap/money regression suite also passes. The old CLI now
  refuses admitted/staged roots before acquisition and holds the shared writer
  lock for both its RAW owning root and requested output root.
- Mandatory quick gate: PASS; formatting/lint/compile plus 363 process and
  architecture tests PASS, 2 skipped.
- Actual captured MOEX history for 2-3 November and current securities/source
  pages exercised the existing native renewal on both routes. Canonical-route
  identical-input native retry made no writes.
- Independent full readback of all 147 renewed canonical parameter rows:
  RAW key/hash/pin and admission binding match; missing/conflicting origins 0.
- Complete v6 producer outputs on identical captured history and pinned stored
  FX/RMS: economics 124,346 rows, asset risk 226,239 rows, FX 45,383 rows on
  each route. Bidirectional Spark business-column differences: 0 for all three
  tables, including historical calibration. Source/provenance documents and
  `created_at` are checked separately, not claimed byte-identical across paths.
  The 124,346-row refresh result is distinct from the unchanged 124,256-row
  initial production oracle.

Native image: `sha256:5ada9f4ddf8b4ac149fab285f3e23ce5cffc02f5c6b5274ddb4191106d0f0ca1`.
Evidence directory: `.tmp/economics-production-cutover-20261104/`.
Full copied-history result: `.tmp/ec-linux-shadow/full-cutover-final-proof.json`.

## Data to Research scope proof, 2026-11-05

The initial activation did not prove the Research admission boundary. A fresh
native check found `BLOCKED: data_table_set_changed`; this follow-up adds a typed
zero-effect transition and native-renewal parameter coverage without changing
the accepted v6 money policy or generic scope-recovery contract.

The actual initial production Data pins are exercised through a read-only data
mount with separate writable registry/cutover-metadata/lock overlays. The real
accepted Research manifest is read unchanged. Thus producer publication and
rollback affect only isolated metadata; all numerical reads use exact production
Delta versions, not rewritten table IDs or fabricated source records.

Native Spark audit: 124,347 complete parameter/origin rows and original RAW
projections agree; 124,256 economics rows retain the same reference. Every other
Data reference stays identical. The ordinary daily admission changes from
`BLOCKED` to `NOOP`, with zero numerical rebuild. Retry returns the same issued
child; metadata rollback returns the original boundary, and reapplication selects
the same child again. Original manifests and Delta histories remain unchanged.

Independent earlier copied-economics proof also compares all history and
reconciles an identical-input retry. Unit/native regressions cover lost intent
after manifest save, lost CAS acknowledgement, a changed candidate envelope,
unknown/native version gaps, unrelated descendants and producer writes.

Evidence: `.tmp/economics-handoff-20261105/native-overlay.json`,
`.tmp/economics-handoff-20261105/native-producer.json`, native test and gate logs
beside those receipts. Isolated proof does not itself claim production promotion
or the result of a later scheduled run; those require fresh deployed readback.
Full producer comparison: `producer-parity.json` in the evidence directory;
source acquisition failure: `shadow-renewal.log`.

## Review boundary and remaining source limitation

The contract moves calculated/restored input into admitted canonical storage,
retains received facts in RAW, and changes the registered updater target.
Unbound clones, fabricated observations, stale aliases and blind rollback over
later commits are forbidden; native and unit regressions cover these cases.
The existing v6 monetary producer/policy remains the owner; v8 is not imported.

Live MOEX RMS for 4 November was absent. The ordinary combined refresh stopped
after successful native history renewal; fresh current-day economics is not
proven. Existing coverage/age/positive-value guards decide DEFERRED versus
BLOCKED. Reuse must retain the actual selected economics pin and explicit source
failure; it cannot silently claim freshness or fabricate RMS.

Historical missing formula/source attachments remain the explicitly accepted
production-baseline limitation. These proofs establish migration parity and
lineage, not independent historical exchange-fact or formula certification.
