# Economics canonical cutover

Surface: product-plane. Owner: Market Data Foundation.
Decision: `docs/architecture/product-plane/economics-source-lineage.md`.

The owner accepts production computed history as the numerical oracle. This
procedure preserves the existing v6 model and admits that exact baseline; it
is not independent replay of missing historical formulas or exchange sources.

## Before working writes

1. Merge the reviewed cutover through main; pass required CI, freeze its SHA,
   then promote that main SHA to the production branch/checkout. Keep the old
   release. Do not patch production source during execution.
2. Reconcile the acceptance receipt with exact live parameter/economics pins
   and the complete selected snapshot. If any reference advanced, repeat the
   complete parity proof and obtain the corresponding accepted receipt.
3. Record schedule/sensor states and active runs. Temporarily stop baseline
   scheduling and dependent CF/research sensors, then drain existing owning
   runs. Keep the same process-visible data root used by the mounted runtime.
4. Save a host release record from the clean promoted checkout. The container
   lacks Git; it verifies the host SHA and identical source/contract hashes.
   Persist the entire acceptance receipt with the migration evidence.

```powershell
python scripts/run_moex_economics_cutover.py release-record --release-sha <sha> --output <shared-release-record.json>
```

Run subsequent commands in the existing native Spark/Delta image, with the
promoted checkout mounted read-only and the working data root mounted at its
normal container path. Example arguments use that container namespace:

```sh
python scripts/run_moex_economics_cutover.py plan --data-root /ta3000-data/moex-historical --acceptance <acceptance.json> --release-sha <sha> --release-record <release-record.json>
python scripts/run_moex_economics_cutover.py apply --plan /ta3000-data/moex-historical/canonical/economics/.production-cutover/plan.json
python scripts/run_moex_economics_cutover.py verify --plan /ta3000-data/moex-historical/canonical/economics/.production-cutover/plan.json
```

## Publication and normal updates

The plan freezes the previous full snapshot, native input pins, code and policy.
The standalone bootstrap CLI refuses staged/admitted roots and shares the
writer lock for both its RAW owning root and requested output root. Updates of
an admitted working root use the registered Dagster baseline owner.
Apply creates canonical parameters with a new identity, removes restoration
fields from current RAW through the retained payloads, retains old Delta
versions, and publishes a complete child through compare-and-swap. It preserves
unrelated published references and the initial economics output exactly.

Inspect the selected manifest and independently read its native pins. Confirm
`canonical_contract_parameters`, the immutable owner acceptance/binding, RAW
payload hashes, unchanged initial business values, and the registered canonical
writer target. An incomplete activation must reject updater registration.

Run a real bounded renewal through the existing baseline owner. Confirm its
canonical input alias, RAW snapshot/history origins, v6 output QC and consumer
readback. Replay identical captured inputs to check idempotence. The retired
prepared table remains frozen. Restore only previously running controls.

`verify` distinguishes immutable initial parity from current publication
continuity. A later selected snapshot must descend from the issued candidate,
keep the canonical identity and accepted binding, have accepted QC and matching
physical pins. Legitimate later recalibration is not claimed equal to the old
money values. Keep the frozen cutover release available for its verification.

## Interrupted execution and rollback

Retry `apply` with the same plan/release. It recognizes owned commits before
acknowledgement and retains the candidate UUID. Changed/foreign targets, lost
issued identity, modified acceptance and publication races refuse execution.
Do not delete intent files, replace them with new plans, or overwrite conflicts.

Before a new producer commit or unrelated publication, the same quiesced
cutover-aware release can abort partial writes or restore the prior publication:

```sh
python scripts/run_moex_economics_cutover.py rollback --plan /ta3000-data/moex-historical/canonical/economics/.production-cutover/plan.json
```

Rollback restores RAW data and schema through native Spark RESTORE, keeps the
canonical candidate as retained inactive data, and selects the former writer
route. It refuses rollback over later writer commits/publications. Verify the
old complete snapshot/route and restore the previously running controls. Do not
automatically deploy the former binary: its publisher cannot distinguish the
retained inactive canonical candidate. Never delete Delta history for rollback.
