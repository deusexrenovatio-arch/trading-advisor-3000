# MOEX early economics inputs

Change surface: `product-plane`.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: native Spark integration test; frozen public source pages and full Delta readback; timestamp-bound risk regression; independent per-bar coverage audit and unchanged source hashes.
- Shortcut Waiver: none

Extend settlement and FX/RMS support into 2018 using isolated staging, preserving
the existing raw and price canonical tables. Extend parameter regimes only where
dated MOEX documents and observed turnover agree.

Launch-day economics become available in the following session. When no earlier
risk observation exists, admit a first RMS observation only if its explicit
publication timestamp is at or before that effective interval's start and its
risk date is no later than the effective session. Preserve the existing priority
of observations dated on/before the settlement date. Missing timestamps and
later publications must not fill the gap. Store the selected timestamp and
selection policy in economics provenance, without changing the risk table schema.

Known limits: ISS RMS history starts 22 May 2018; three post-expiry STEPPRICE
gaps affect zero price bars. No current publication or scheduler change.

Evidence root: `early-economics/17ab91c2-f13b-48cc-9be2-330312fa4a9a`.

## Self-review

- Changed contract: eight explicitly bounded pre-2020 parameter regimes;
  first risk observation can use a later source date only when publication is
  proven before the economics interval begins. Earlier dated risk keeps priority.
- Forbidden old behavior: rejecting an already published first risk snapshot
  solely because its date is later than settlement; accepting missing/late
  publication timestamps or extrapolating parameters outside audited bounds.
- Regression: launch-risk test failed with `None` instead of 0.07, then passed;
  it covers Moscow/UTC normalization, the exact boundary, late and absent times,
  previous-observation priority and original source hash. Pre-2020 boundary test
  failed before adding regimes; native reconstruction also exercises June 2018.
- Physical proof: 118,958 economics rows, 511 contracts, no duplicate keys,
  contiguous intervals. All 13 previous launch gaps now have positive margin;
  the other 112,192 previous rows preserve numeric values. Raw payloads unchanged.
- Coverage: exact timestamp join covers 30,776,722 of 30,780,287 canonical bars.
  The 3,565 exceptions are preserved in a separate audit Delta: 3,490 bars before
  any available economics and 75 RIH0 bars (January-April 2018) without MR1.
  Three post-expiry STEPPRICE gaps affect zero bars. Full downstream readiness
  remains unclaimed; no future MR1 was backdated and no price bars were deleted.
- Residual risk: historical margin remains the existing v6 estimate calibrated
  on 28 July 2026, not a historically observed official margin. The coverage
  audit uses canonical bar timestamps; weekly consumer end-time semantics and
  continuous-front selection require their own downstream proof.

Validation: 26 native Spark integration tests and 15 economics unit tests pass;
PR gate passes. The staged solution-intent/closure validators are run against
this task note explicitly. Source integrity proof verified all seven frozen raw
tables, the extended calendar and 511 metadata sources. Acquisition proof checks
1,412 receipts and 2,965 files, including the four malformed source rows retained
in response quarantine. The existing session filter removes 91 early settlement
rows with zero recorded trade volume; prepared input retains them for audit.
