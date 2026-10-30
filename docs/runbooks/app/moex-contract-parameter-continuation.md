# Bounded MOEX contract parameter continuation

Expired-contract resolution during daily renewal follows the
[admitted-description decision](docs/architecture/product-plane/expired-contract-description-resolution.md).
The original bounded bundle contract below remains unchanged.

The direct economics Spark API can explicitly admit the reviewed thirteen-asset
parameter bundle for **2026-07-29 through 2026-10-05 inclusive**. Default additional
tuple is empty; all32 registered historical regimes retain their IDs, values and
bounds. The bundle is static parameter inference, not daily official specification
coverage. This delivery does not renew its cutoff or enable a scheduled adapter.

Runtime source: `product_plane/data_plane/moex/contract_parameter_continuation.py`;
owning runner: `spark_jobs/moex_contract_economics_job.py`.

## Admission before Spark

Pass `additional_contract_parameter_regimes` as a tuple of
`MoexContractParameterRegime`, and `contract_parameter_evidence` as
`MoexContractParameterEvidence(bundle_id, regimes_path, dated_official_tc_path)`.
The only admitted bundle ID is `bounded-tail-20261005-v1`. Existing calibration
bool/date/pair guards run first. The pure tuple and byte checks run before any
Delta source metadata, Spark factory, output directory or report write.

The contract independently pins:

- regime file SHA256 `9eef9156c8d83ebede9ceaa9f7824a71543239d9e0215b9d5cebb8b2990ac590`;
- dated evidence SHA256 `666a0a618b4b1f337b1c874623e14da72a08eda6c35dfccff83057e770b08f63`;
- prepared input TableID `d550b9e4-e3d6-40dc-93f0-fc0e420e3b43`, version0.

Retained accepted metadata lives under
`D:/TA-3000-rebuild/staging/ta3000-5y-20260729/downstream-refresh/20261010/parameter-continuation`:
`bounded-parameter-regimes.json` and `dated-official-tc-evidence.json`. Callers may
point at retained copies with exactly the same bytes. Caller-provided hashes do
not admit arbitrary replacements. Changed evidence or a new cutoff requires a
separately reviewed contract change.

Each metadata file is read once, capped at64KiB, then hashed and parsed from
those same bytes. Reject duplicate JSON keys/nonfinite constants. The parsed
regime list must equal the supplied tuple, including native tick values and the
terminal source-document evidence digest. Exact canonical dates, registered
uppercase assets, unique IDs, finite positive real values (excluding bool), and
inclusive nonoverlap with the registry are required. NASD/RTS/MXI tick values
remain independent of `MINSTEP × LOTVOLUME`.

Native Delta metadata then checks the prepared TableID at version0, before Spark.
Continuation requires an absolute prepared-input path; relative paths are rejected
before Delta so Python and Hadoop cannot bind different roots from different CWDs.
The owning Spark reader uses `versionAsOf=0`; a later append cannot silently
change the read. Retain the source under a stable read-only path. Uncoordinated
deletion/recreation of that root is outside the ownership contract. The existing
empty/default path keeps its previous unversioned reader behavior.

## Calculation and evidence

The dated rule uses exact trade-date positive official step price and TC FX:
`abs(official_step_price / (tick_value_quote × FX_TC) - 1)`.
TC FX is joined by the rule's quote currency and the exact trade date; a different
spec currency cannot substitute its rate or RUB identity for a USD rule.
RUB permits rate1. MC-only FX cannot validate a non-RUB rule; turnover is not a
fallback for the dated method. A dated rule is validated even when its final
step-price source is official. Requested dated profiles need at least one valid
sample and P95≤0.001; absent profiles fail. Report maximum error separately.

The retained evidence receipt has674 samples across13 profiles, not an official
observation for every day. Its SHA verifies the receipt bytes. Native execution
recomputes actual sample errors/counts from the pinned prepared input; these are
reported separately from the receipt's historical aggregate. Raw official input
`23c2930a-77a4-459e-9e0a-eb3e28372bbe`/@134 is retained lineage in the approved
preparation audit; this adapter does not reread or reprove those raw observations.

The report records the complete additional tuple, admitted bundle/hashes,
prepared TableID/version, inference flag and native rule-validation profiles.
Row source flags carry method and source-document ID. Canonical schema projection
excludes private method columns. Money formulas/model version, zero buffer,
eligible risk-publication history/hashes and NULL-formula unavailable quality
retain their existing contracts. Positive official margin stays authoritative.

Historical margin opt-in remains separate: explicitly pair
`allow_calibrated_latest_margin=True` with `margin_calibration_as_of_date`.
This bundle does not manufacture official INITIALMARGIN after14August or choose
a calibration date for the operator. Dates outside its bounds get no continued
regime; missing required inputs still fail.

## Delivery boundary

Focused unit/native fixtures prove admission, pinned reads and calculation
behavior. They do not prove full-dataset freshness, memory/OOM closure, source
observation replay or primary publication. Existing runner still computes its
full economics input and merges canonical outputs; this API adds no range filter.

Source acquisition/preparation, regular evidence renewal, CLI/baseline/Dagster
forwarding and primary promotion remain separate work through the existing route.
Do not treat the historical artifact preparation script as a scheduled producer.
