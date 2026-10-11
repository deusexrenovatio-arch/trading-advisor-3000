# Historical contract economics delivery

Change surface: product-plane. Spark joins, windows and expressions own economics;
Delta owns materialized tables. Python holds bounded source rules and reporting.

Historical economics previously excluded audited early parameter intervals,
ignored flat turnover payloads, and admitted FX cross quotes into RUB conversion.
This delivery adds explicitly dated pre-2022 and pre-2020 intervals, reads nested
and flat history payloads, and filters FX inputs to RUB quotes with RUB/RUB identity.
The existing original regimes, money formulas and buffer policy remain intact.

A first risk observation dated after settlement can fill the effective interval
only when its publication is known at or before that interval starts, its date is
no later than the effective session, and MR1 is positive. Prior dated observations
retain priority. Native ISS timestamps are normalized from Moscow; selection
policy, publication timestamp and original source hash remain provenance.
A missing/zero margin formula stays unavailable even when calibration exists.

The launch path preserves compact native publication history by asset/day before
selecting daily latest. It chooses the latest eligible limits and static snapshot
at its effective cutoff; timestamp/hash ordering resolves ties. A late daily
snapshot cannot hide an earlier eligible publication. The canonical risk writer
projects its original schema and persists one latest row/day, with no history
arrays. Ordinary prior risk values and hashes retain their existing selection.

## Validation scope

Focused unit and native Linux Spark/Delta integration regressions run against
this candidate branch. They cover bounded/non-overlapping regimes, early BR
reconstruction, FX cross quotes, both payload shapes, publication boundaries,
unknown/late timestamps, prior observation priority and missing-formula quality.
A same-day regression covers eligible/late publications, deterministic ties,
late-only static metadata, selected source hashes and physical canonical schema.
Counts and container resource evidence belong to the PR validation record.
The fixtures prove code behavior; they do not promote the primary dataset.

The source notes `docs/tasks/moex-specification-enrichment.md` and
`docs/tasks/moex-early-economics.md` record their earlier executions. Their July
calibration, coverage counts and PASS statements are historical evidence.
They are not current primary-promotion evidence for the October candidate.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream closure; unbounded parameter extrapolation; missing-publication risk backfill
- Closure Evidence: focused native economics regressions and delivery gates; not full target closure of canonical dataset/downstream research or runtime-ready surface
- Shortcut Waiver: none

## Self-review

Changed contract: dated historical regime coverage and publication-aware risk
selection provenance. Forbidden behavior: cross-pair RUB conversion, parameters
outside audited bounds, absent/late launch publication backfill, and calibrated
quality when the computed margin is NULL. Regressions are in
`tests/product-plane/unit/test_moex_contract_economics.py` and
`tests/product-plane/integration/test_moex_contract_economics_spark_job.py`.
Initial proof runtime with a configured 2 GiB Java heap exhausted heap in the existing
merge-history test after 16 passing cases. The corrected fixture runtime retains
a 6 GiB container and uses a measured 4 GiB JVM heap; PR evidence records native
results and cgroup peak. This is not full-size economics/OOM acceptance.
Residual scope: later historical-GO calibration policy, default partition writers,
snapshot publication and full fresh-data composed route need separate delivery
and proof. This patch alone cannot refresh the accepted October tail.
