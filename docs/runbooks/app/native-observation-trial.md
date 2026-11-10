# Unpublished native observation trial

Change surface: product-plane. This is a bounded verification operation under
the [native observation decision](docs/architecture/product-plane/market-identity.md#unpublished-native-observation-trial).
It does not register a production job or publish a snapshot.

Use a fresh unique child of the verification root. Validate its resolved path
against the production checkout/data roots before creating it. Mount that child
alone as writable `/trial`; mount the reviewed checkout read-only as `/workspace`.
Use an immutable image ID, a read-only container filesystem and bounded memory/CPU.
Spark requires executable temporary storage for its native codec. Keep Dagster,
Spark/Ivy and other runtime scratch inside the trial. Do not mount production data,
the production checkout or an existing Dagster instance.

Set `PYTHONPATH=/workspace/src` (plus an isolated directory for declared `dev-test`
dependencies when needed), `DAGSTER_HOME=/trial/runtime/dagster-home`,
`SPARK_LOCAL_IP=127.0.0.1` and `TA3000_SPARK_RUNTIME_ROOT=/trial/runtime/spark`.

## Acquire and materialize

The opt-in definitions are in
`data_plane.moex.observation_job`; they are not imported by production definitions.
Run them with a fresh local Dagster instance, telemetry disabled:

1. Supply `identity-input.json`: explicit existing pilot asset/instrument IDs,
   original assignment artifact hash and complete source route. The acquisition
   revalidates individual assignments against fresh exchange metadata. The old
   registry's CF hashes, time intervals and specification assumptions are not reused.
2. Execute `native_observation_acquisition_job`, op
   `acquire_native_observations`, with `root: /trial`,
   `identity_input: identity-input.json`, `date_from` and `date_till`.
   Original bodies and HTTP receipts are retained before decode; capture failure
   aborts. Eight nonempty slices and their terminal pages are required for the
   four-instrument 1m/1d trial. A source failure cannot become a successful empty slice.
3. Use `build_observation_command` with the returned source/assignment pins and an
   explicit command ID. Save its input immutably with `common.data_snapshots.atomic_json`.
   Execute `native_observation_materialization_job`, op
   `materialize_native_observations`, with that root and relative `intent_path`.
4. Read the returned completion pin with `NativeObservationSnapshot`.
   A saved manifest without a terminal completion receipt is not readable admission.
   Four pinned Delta tables are required: RAW, native bars, observed-day QC, quarantine.

Keep job run IDs and outputs as immutable acquisition/baseline evidence. The native
acceptance harness `tests/product-plane/integration/native_observation_trial.py` consumes
`acquisition-result.json` and `baseline-01-result.json` in that same root; it requires
the acquisition pins/counts and the baseline completion/oracle outputs. Its named
fault commands deliberately fail. Run a new acceptance suite in a fresh root;
never edit an old intent to make an incomplete command accept changed code.

## Verify and consume

The original-byte Decimal oracle checks persisted RAW fields, exact native values,
quantity NULL/state semantics, quarantine accounting and observed-day aggregates.
The suite checks receipt replay without new acquisition/Delta versions, a named
simulated volume correction, partial-write recovery, saved-graph reconciliation,
lost acknowledgement, OS lock release after process death and ambiguous recovery.
Original RAW and previous completed candidates remain immutable and readable.

`NativeObservationSnapshot.read` takes the closed source-time request contract.
Arrow IPC preserves Decimal(38,10), NULLs, original time strings and pinned lineage.
`create_app(observation_reader=...)` explicitly binds the existing Data Inspector's
`POST /api/data-inspector/native-observation-read`; the default app has no binding.
Verify the actual endpoint and IPC roundtrip, then rerun the old frozen
bars/L0/L1/L2 reader against its original pins. Inspect actual container mounts and
absence of publication pointers separately; fixture tests cannot prove containment.

Times remain source-local strings. No UTC close/expiry instant, historical mapping
interval, complete-session Q, CF, PIT or execution eligibility is inferred. Index
volume/OI are not applicable; absent futures OI stays unavailable. Daily source
bars and observed-day QC have different meanings.

Rollback means stopping the opt-in trial and selecting an explicit previous
completion pin. Retain failed attempts and receipts. No current/accepted/latest
pointer, production restore, Research job or promotion belongs to this operation.
