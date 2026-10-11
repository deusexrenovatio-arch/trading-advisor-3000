# Shared stage timing delivery

Change surface: product-plane. The common helper owns synchronous timing and
JSON event correlation. Runtime retains six compatibility reexports; existing
callers and their reports use the same public functions.

`stage_timer()` still returns a monotonic float. Existing report entries retain
status, elapsed seconds and arbitrary metrics. New optional stage labels emit
start events; `timed_stage` and `timed_step` are available for later integration.
They measure existing work; metrics must never cause extra Spark actions.
Nested durations overlap and must not be added into a total pipeline duration.

## Event contract

The dedicated `ta3000.pipeline_timing` logger emits `pipeline_timing.v1` JSON:
UTC timestamp, PID, event, step IDs, parent ID, run ID and dataset version.
Correlation uses named run/build/context arguments, including signature defaults,
then the parent step, `DAGSTER_RUN_ID` and finally a generated run ID. Nested
steps restore their parent context on success and failure; independent threads
have separate root contexts.

Canonical event/context fields and the emitted stage label override metric
names. Report metrics keep their original values, including reserved field names.
A yielded mutable metrics dictionary therefore cannot change event identity or
mask the original work exception. Public argument names cannot also be supplied
as metric keywords at function invocation; inside `timed_stage`, yielded metrics
can carry those names safely. Existing report status/elapsed metric overrides
remain compatible; failure recording forces FAIL and the original error type.
Log-sink failures neither change the returned object nor replace the work error.

Pending timers retain multiplicity even at equal clock readings or repeated
stage labels. Completed/skipped stages close their pending records; unfinished
stages close as INCOMPLETE on successful return or FAIL on exception. A returned
report status (including BLOCKED) is retained; `success=False` is logged as FAIL.

The helper is synchronous. Async decoration and parent-to-worker context
inheritance are outside its contract. IDs hidden inside a callable's `**kwargs`
are not inspected. The dedicated logger does not modify root handlers. When an
operator preconfigures handlers/levels, that policy is honored: WARNING can
suppress INFO timing events.

## Validation and acceptance boundary

Pure unit regressions cover main's missing helper, frozen R5 event-key/default-ID
and clock-collision failures, nested failure restoration, mutable reserved
metrics, result/exception identity, thread isolation, monotonic elapsed time,
terminal status, and legacy exports/report payloads. Existing named-stage report
assertions remain unchanged. These tests do not start Spark or touch real data.

This is the shared prerequisite for later raw/canonical/research instrumentation.
It does not enable those callers, prove current production visibility, measure
full-route latency, promote a dataset, prove partition readiness or close OOM.
Historical timing runs and counts are not current delivery evidence.

## Solution Intent

- Solution Class: staged
- Critical Contour: runtime-publication-closure
- Forbidden Shortcuts: synthetic upstream closure; telemetry replacing work exceptions; extra Spark actions for timing
- Closure Evidence: focused pure timing regressions, unchanged legacy report assertions and scoped delivery gates; not full data-integration or runtime-ready closure
- Shortcut Waiver: none

## Self-review

Changed contract: shared event correlation and safe terminal timing cleanup.
Forbidden behavior: metric-key collisions changing work results/errors, omitted
default IDs losing correlation, or equal clock readings losing active stages.
Regressions: `tests/product-plane/unit/test_pipeline_timing_events.py` and the
unchanged `tests/product-plane/unit/test_stage_timing_reports.py`.
Residuals: synchronous-only wrappers, externally filtered INFO logs, and later
caller integration/native end-to-end execution require separate proof.
