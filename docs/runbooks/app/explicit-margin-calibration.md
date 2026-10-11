# Explicit historical margin calibration

Change surface: product-plane. Solution class: staged; critical contour:
`data-integration-closure`. Spark SQL owns calculation and gates; Delta owns
materialized economics. The CLI validates and forwards typed inputs.

The default still requires positive official INITIALMARGIN on the latest session.
A historical refresh may explicitly allow modeled latest-session margin only with
both arguments:

```text
--allow-calibrated-latest-margin --margin-calibration-as-of-date 2026-08-14
```

The date is ISO and parsed as a Python `date`. Missing either argument or malformed
dates fail before acquisition, raw writes or Spark startup. The direct Spark job
API also requires paired bool/date parameters and rejects datetime/string dates
before reading source tables or creating outputs.

Calibration uses official/formula pairs from the exact requested session only.
Absent dates or assets have no fallback. Same-asset maturity rank wins; otherwise
the nearest rank wins, with smaller rank breaking a tie. Positive official margins
remain unchanged. The existing v6 formula, zero buffer, calibration provenance,
compact publication history and NULL-formula unavailable guard remain intact.

A modeled latest row must have accepted calibration quality/source, the requested
calibration date no later than its session, and positive finite factor and margin.
The calibration-date availability gate applies only when latest INITIALMARGIN
needs modeling; positive official latest rows need no calibration-date availability
validation. Other required inputs remain mandatory. Missing official-margin counts stay
missing; calibration does not create an official snapshot. Duplicate calibration
pairs remain fail-closed under the existing report gate.

## Bounded proof

Focused CLI/API tests cover paired/type validation before acquisition and forwarding.
Native SQL tests cover date/rank selection, strict default, later-snapshot exclusion,
missing date/asset, NULL/nonpositive formulas, invalid factor/estimate/metadata,
other required inputs and duplicate pairs. The physical Delta fixture retains
14 August official margin, has no later official margin, and materializes the
5 October estimate only with explicit opt-in; input Delta versions remain unchanged.
Existing native economics regressions cover publication cutoffs, same-day history,
prior-risk precedence, schema projection, merge history and execution-economics joins.

These fixtures prove the adapter contract, not the accepted full dataset or
primary publication. Native receipt/resource counts are recorded with delivery
validation; no numerical model revision is introduced.

## Remaining delivery

Scheduled forwarding must explicitly pin and preserve this policy. It is a
separate mandatory group. Bounded later parameter continuation, layout/schema
changes, candidate recomputation/readback and primary promotion require their own
review and physical evidence. This adapter alone does not authorize promotion.

## Self-review

Changed contract: explicit historical latest-margin admission and exact calibration
date passed through the CLI/core. Forbidden behavior: silent fallback calibration,
implicit model acceptance, coercing datetime/string API inputs, labeling NULL
formula as calibrated, or dropping official missingness. Tests above catch each.
Residual risk: scheduled route and full October candidate proof remain outstanding;
historical estimates are retrospective model outputs, not official publications.
