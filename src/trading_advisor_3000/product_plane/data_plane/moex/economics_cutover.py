"""Canonical admission of an owner-accepted production baseline; no new money math."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

from deltalake import DeltaTable

from trading_advisor_3000.product_plane.common.data_snapshots import (
    _hash,
    atomic_json,
    capture_table,
    create_snapshot,
    current_snapshot,
    load_snapshot,
    open_pinned_table,
    publish_snapshot,
    read_spark_delta,
    runtime_provenance,
    save_snapshot,
)

KIND = "economics-canonical-cutover.v1"
PARAMETERS = "canonical_contract_parameters"
BINDING = "accepted-production-baseline.json"
METADATA = ("calculation_origin_json", "source_validation_status", "execution_admissible")


def calculation_policy():
    import hashlib

    from trading_advisor_3000.spark_jobs import moex_contract_economics_job

    from . import economics, native_history_refresh

    return {
        "model_version": economics.MOEX_CONTRACT_ECONOMICS_MODEL_VERSION,
        "buffer_policy_version": economics.MOEX_MARGIN_BUFFER_POLICY_VERSION,
        "sources": {
            module.__name__: hashlib.sha256(
                Path(module.__file__).read_text(encoding="utf-8-sig").replace("\r\n", "\n").encode()
            ).hexdigest()
            for module in (economics, native_history_refresh, moex_contract_economics_job)
        },
    }


@contextmanager
def economics_writer_lock(canonical_root):
    canonical_root = Path(canonical_root)
    if not canonical_root.is_absolute():
        raise ValueError("economics writer requires an absolute root")
    canonical_root.parent.mkdir(parents=True, exist_ok=True)
    with (canonical_root.parent / ".economics-writer.lock").open("a+b") as stream:
        try:
            if os.fstat(stream.fileno()).st_size == 0:
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ValueError("another economics writer owns this root") from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


def serialize_economics_refresh(function):
    @wraps(function)
    def call(**kwargs):
        with economics_writer_lock(kwargs["canonical_economics_root"]):
            return function(**kwargs)

    return call


def _exclusive(function):
    @wraps(function)
    def call(plan_path, *args, **kwargs):
        plan = _load_plan(plan_path)
        with economics_writer_lock(Path(plan["root"]) / "canonical/economics"):
            return function(plan_path, *args, **kwargs)

    return call


def _read(path):
    with Path(path).open("rb") as stream:
        payload = stream.read(64 * 1024 + 1)
    if len(payload) > 64 * 1024:
        raise ValueError("canonical admission metadata exceeds 64KiB")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("canonical admission metadata requires an object")
    return value


def _accepted_receipt(path, digest):
    try:
        value = _read(path)
    except OSError as exc:
        raise ValueError("retained owner acceptance is unavailable") from exc
    if value.get("status") != "ACCEPTED_PRODUCTION_BASELINE" or _hash(value) != digest:
        raise ValueError("retained owner acceptance changed")
    return value


def _copy_policy(path, data):
    import os
    import tempfile

    if path.exists():
        if path.read_bytes() != data:
            raise ValueError("foreign canonical policy metadata")
        return
    descriptor, temporary = tempfile.mkstemp(prefix=".policy-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _owner(table, version):
    entry = next((row for row in table.history() if row["version"] == version), {})
    value = entry.get("userMetadata")
    return json.loads(value) if isinstance(value, str) else value


def load_canonical_binding(path, verified):
    """Keep legacy admission intact; explicitly bind its measured canonical clone."""
    path = Path(path)
    value = _read(path)
    body = {k: v for k, v in value.items() if k != "binding_sha256"}
    if value.get("binding_sha256") != _hash(body):
        raise ValueError("canonical admission digest changed")
    source, canonical = value["source"], value["canonical"]
    receipt_path = path.parent / ".production-cutover/owner-acceptance.json"
    if Path(value["acceptance_path"]).resolve() != receipt_path.resolve():
        raise ValueError("canonical owner acceptance must be retained beside its intent")
    acceptance = _accepted_receipt(receipt_path, value["acceptance_sha256"])
    declared_source = acceptance["production_tables"]["calculated_parameters"]
    if any(source.get(k) != v for k, v in declared_source.items() if k != "table_path"):
        raise ValueError("canonical source differs from owner-accepted production")
    if (
        value.get("status") != "ACCEPTED_PRODUCTION_BASELINE"
        or value.get("checks") != {"production_parity": "PASS", "origins": "PASS"}
        or source["table_id"] != verified["prepared_input_table_id"]
        or source["delta_version"] < verified["prepared_input_version"]
        or source["table_id"] == canonical["table_id"]
        or Path(canonical["table_path"]).resolve() != path.parent / (PARAMETERS + ".delta")
        or any(
            value.get(key) != verified[key]
            for key in ("regimes_sha256", "dated_official_tc_evidence_sha256")
        )
        or value.get("owner", {}).get("kind") != KIND
        or value["owner"].get("role") != "parameters"
        or value.get("calculation_policy") != calculation_policy()
    ):
        raise ValueError("canonical clone admission differs from accepted policy/source")
    old = open_pinned_table(source)
    target = open_pinned_table(canonical)
    if (
        value["business_columns"] != old.to_pyarrow_dataset().schema.names
        or target.to_pyarrow_dataset().schema.names != value["business_columns"] + list(METADATA)
        or _owner(target, canonical["delta_version"]) != value["owner"]
    ):
        raise ValueError("canonical clone physical owner/schema differs from admission")
    return {
        **verified,
        "prepared_input_table_id": canonical["table_id"],
        "prepared_input_version": canonical["delta_version"],
        "canonical_binding": value,
        "canonical_binding_path": path.resolve().as_posix(),
    }


def _parameter_frame(source, reference, owner):
    from pyspark.sql import functions as F

    keys = ["moex_secid", "trade_date"]
    if (
        source.where(F.col(keys[0]).isNull() | F.col(keys[1]).isNull()).limit(1).count()
        or source.groupBy(*keys).count().where("count > 1").limit(1).count()
    ):
        raise ValueError("accepted parameters need unique complete contract/date keys")
    origin = F.concat(
        F.lit(
            '{"kind":"accepted_production_baseline","source_table":'
            + json.dumps(reference, sort_keys=True)
            + ',"owner":'
            + json.dumps(owner, sort_keys=True)
            + ',"key":'
        ),
        F.to_json(F.struct(*keys)),
        F.lit("}"),
    )
    return (
        source.withColumn(METADATA[0], origin)
        .withColumn(METADATA[1], F.lit("ACCEPTED_PRODUCTION_BASELINE"))
        .withColumn(METADATA[2], F.lit(True))
    )


def _original_raw(frame):
    from pyspark.sql import functions as F

    provenance = "enrichment_provenance_json"
    restored = F.col(provenance).isNotNull() | F.lower(F.col("source_id")).contains("enriched")
    original = {
        name: F.get_json_object(provenance, "$." + name)
        for name in ("original_source_id", "original_source_document_id", "original_history_hash")
    }
    identity = [
        F.get_json_object("raw_payload_json", "$." + key)
        for key in ("BOARDID", "SECID", "TRADEDATE")
    ]
    locator = F.when(
        identity[0].isNotNull() & identity[1].isNotNull() & identity[2].isNotNull(),
        F.concat_ws(":", *identity),
    )
    original["original_source_document_id"] = F.coalesce(
        original["original_source_document_id"], locator
    )
    same_identity = (
        (identity[0] == F.col("board"))
        & (identity[1] == F.col("moex_secid"))
        & (identity[2] == F.col("trade_date").cast("string"))
    )
    known_history = (
        F.col("source_url").rlike(
            r"^https://iss[.]moex[.]com/iss/history/engines/futures/markets/forts/boards/RFUD/securities(?:/[^/?#]+)?[.]json(?:[?][^#]*)?$"
        )
        & same_identity
    )
    original["original_source_id"] = F.coalesce(
        original["original_source_id"],
        F.when(known_history, F.lit("moex_iss_history_forts_securities_unmodified")),
    )
    if (
        frame.where(
            restored
            & (
                ~same_identity.eqNullSafe(F.lit(True))
                | ~original["original_source_document_id"].eqNullSafe(locator)
            )
        )
        .limit(1)
        .count()
    ):
        raise ValueError("restored RAW identity contradicts its retained source body")
    if (
        frame.where(
            restored
            & (
                original["original_source_id"].isNull()
                | original["original_source_document_id"].isNull()
                | original["original_history_hash"].isNull()
            )
        )
        .limit(1)
        .count()
    ):
        raise ValueError("restored RAW lacks recoverable original record identity")

    @F.udf("string")
    def logical_digest(payload):
        import hashlib

        try:

            def reject_constant(value):
                raise ValueError("nonfinite source JSON")

            value = json.loads(payload, parse_constant=reject_constant)
            encoded = json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
            )
            return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        except (ValueError, TypeError):
            return None

    # The legacy untyped JSON codec is Python-specific; Spark distributes this hash adapter.
    measured = logical_digest(F.col("raw_payload_json"))
    expected_hash = F.when(restored, original["original_history_hash"]).otherwise(
        F.col("source_document_hash")
    )
    if frame.where(~measured.eqNullSafe(expected_hash) | measured.isNull()).limit(1).count():
        raise ValueError("retained RAW payload differs from its original logical hash")
    result = frame
    for column, key in (
        ("source_id", "original_source_id"),
        ("source_document_id", "original_source_document_id"),
        ("source_document_hash", "original_history_hash"),
    ):
        result = result.withColumn(column, F.when(restored, original[key]).otherwise(F.col(column)))
    fields = {
        "min_step": ("MINSTEP",),
        "lot_volume": ("LOTVOLUME",),
        "last_trade_date": ("LASTTRADEDATE",),
        "last_del_date": ("LASTDELDATE",),
        "official_step_price": ("STEPPRICE",),
        "official_initial_margin": ("INITIALMARGIN",),
        "last_settle_price": ("SETTLEPRICE", "LASTSETTLEPRICE"),
    }
    for name, locators in fields.items():
        result = result.withColumn(
            name,
            F.coalesce(
                *[F.get_json_object("raw_payload_json", "$." + key) for key in locators]
            ).cast(frame.schema[name].dataType),
        )
    result = result.withColumn(provenance, F.lit(None).cast("string")).withColumn(
        "snapshot_metadata_json", F.lit(None).cast("string")
    )
    if (
        result.groupBy("source_id", "board", "moex_secid", "trade_date")
        .count()
        .where("count > 1")
        .limit(1)
        .count()
    ):
        raise ValueError("original RAW projection has ambiguous record keys")
    return result


def _equal(left, right):
    if left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count():
        raise ValueError("native full-row parity failed")


def _table(spark, ref):
    open_pinned_table(ref)
    return read_spark_delta(spark, ref["table_path"], version=ref["delta_version"])


def _stage(spark, frame, path, owner, previous=None):
    if (path / "_delta_log").exists():
        table = DeltaTable(str(path))
        current = capture_table(path)
        if previous is not None and current == previous:
            pass
        elif _owner(table, table.version()) == owner and (
            previous is None
            or (
                table.metadata().id == previous["table_id"]
                and table.version() == previous["delta_version"] + 1
            )
        ):
            _equal(frame, _table(spark, current))
            return current
        else:
            raise ValueError("foreign or advanced cutover target")
    elif previous is not None or (path.exists() and any(path.iterdir())):
        raise ValueError("missing/unowned cutover target")
    frame.write.format("delta").mode("overwrite" if previous else "errorifexists").option(
        "overwriteSchema", "true"
    ).option("userMetadata", json.dumps(owner, sort_keys=True)).save(str(path))
    result = capture_table(path)
    if previous and result["table_id"] != previous["table_id"]:
        raise ValueError("RAW cleanup changed Delta identity")
    _equal(frame, _table(spark, result))
    return result


def plan_cutover(root, acceptance_path, *, code_version):
    """Freeze the complete working snapshot and accepted source heads once."""
    root = Path(root).resolve()
    snapshot_root = root / "canonical/moex/baseline-4y-current/.data_snapshots"
    previous = current_snapshot(snapshot_root)
    if previous is None:
        raise ValueError("cutover requires an existing complete published snapshot")
    directory = root / "canonical/economics/.production-cutover"
    if (directory / "plan.json").exists():
        plan = _load_plan(directory / "plan.json")
        if (
            plan["code_version"] != code_version
            or _hash(_read(acceptance_path)) != plan["acceptance_sha256"]
        ):
            raise ValueError("existing cutover plan differs from requested release/acceptance")
        return plan
    acceptance = _read(acceptance_path)
    if acceptance.get("status") != "ACCEPTED_PRODUCTION_BASELINE":
        raise ValueError("explicit owner-accepted production parity is required")
    sources = {
        "parameters": capture_table(
            root / "provenance/prepared/economics/raw_moex_contract_securities.delta"
        ),
        "raw": capture_table(root / "raw/economics/raw_moex_contract_securities.delta"),
        "economics": capture_table(root / "canonical/economics/canonical_contract_economics.delta"),
    }
    for name, accepted_role in (("parameters", "calculated_parameters"), ("economics", "coverage")):
        expected = acceptance["production_tables"][accepted_role]
        if any(sources[name].get(k) != v for k, v in expected.items() if k != "table_path"):
            raise ValueError(
                "production advanced beyond accepted pins; refresh parity before planning"
            )
    published = previous["tables"].get("canonical_contract_economics")
    if (
        published != sources["economics"]
        or previous["snapshot_id"] != acceptance["production_publication"]["snapshot_id"]
    ):
        raise ValueError("accepted economics is not the current complete publication")
    if (root / "canonical/economics" / (PARAMETERS + ".delta")).exists():
        raise ValueError("canonical parameter target already exists without cutover plan")
    policy_dir = root / "provenance/prepared/economics"
    import hashlib

    policy = {
        name: hashlib.sha256((policy_dir / name).read_bytes()).hexdigest()
        for name in ("bounded-parameter-regimes.json", "dated-official-tc-evidence.json")
    }
    value = {
        "kind": KIND,
        "root": root.as_posix(),
        "sources": sources,
        "previous_snapshot_id": previous["snapshot_id"],
        "previous_manifest_sha256": previous["manifest_sha256"],
        "acceptance_sha256": _hash(acceptance),
        "policy": policy,
        "calculation_policy": calculation_policy(),
        "code_version": code_version,
        "runtime_source_sha256": runtime_provenance()["source_sha256"],
    }
    value["intent_id"] = _hash(value)
    directory.mkdir(parents=True, exist_ok=True)
    atomic_json(directory / "owner-acceptance.json", acceptance, immutable=True)
    atomic_json(directory / "plan.json", value, immutable=True)
    return value


def _paths(plan):
    root = Path(plan["root"])
    canonical = root / "canonical/economics"
    return (
        root,
        canonical,
        canonical / ".production-cutover",
        root / "canonical/moex/baseline-4y-current/.data_snapshots",
    )


def _load_plan(path):
    plan = _load_accepted_plan(path)
    if plan.get("runtime_source_sha256") != runtime_provenance()["source_sha256"]:
        raise ValueError("cutover release source changed; retain the frozen implementation")
    return plan


def _load_accepted_plan(path):
    plan = _read(path)
    if plan.get("kind") != KIND or _hash(
        {k: v for k, v in plan.items() if k != "intent_id"}
    ) != plan.get("intent_id"):
        raise ValueError("cutover intent changed")
    if plan.get("calculation_policy") != calculation_policy():
        raise ValueError("accepted calculation policy/code changed")
    root, _, directory, _ = _paths(plan)
    if not root.is_absolute() or Path(path).resolve() != directory / "plan.json":
        raise ValueError("cutover plan path differs from its owning root")
    for role, relative in (
        ("parameters", "provenance/prepared/economics/raw_moex_contract_securities.delta"),
        ("raw", "raw/economics/raw_moex_contract_securities.delta"),
        ("economics", "canonical/economics/canonical_contract_economics.delta"),
    ):
        if Path(plan["sources"][role]["table_path"]).resolve() != root / relative:
            raise ValueError("cutover source path differs from its declared role")
    _accepted_receipt(directory / "owner-acceptance.json", plan["acceptance_sha256"])
    return plan


def _issued_candidate(plan, directory, snapshot_root):
    intent_path = directory / "candidate-intent.json"
    candidate = load_snapshot(intent_path) if intent_path.exists() else None
    issued = []
    for path in (snapshot_root / "manifests").glob("*.json"):
        header = json.loads(path.read_text(encoding="utf-8"))
        if header.get("run_id") == plan["intent_id"]:
            issued.append(load_snapshot(path))
    if candidate is None and issued:
        raise ValueError("saved cutover candidate lost its issued identity intent")
    if candidate is not None and (
        candidate["run_id"] != plan["intent_id"]
        or candidate["code_version"] != plan["code_version"]
        or candidate["parents"] != [plan["previous_snapshot_id"]]
        or any(value != candidate for value in issued)
    ):
        raise ValueError("issued cutover candidate differs from its frozen intent")
    return candidate


@_exclusive
def apply_cutover(plan_path, spark, *, on_commit=None):
    """Run only while the Dagster writers are quiesced; native owned retry."""
    import hashlib

    plan = _load_plan(plan_path)
    root, canonical, directory, snapshot_root = _paths(plan)
    old = load_snapshot(snapshot_root / "manifests" / (plan["previous_snapshot_id"] + ".json"))
    if old["manifest_sha256"] != plan["previous_manifest_sha256"]:
        raise ValueError("previous full manifest changed")
    current = current_snapshot(snapshot_root)
    intent_path = directory / "candidate-intent.json"
    candidate = _issued_candidate(plan, directory, snapshot_root)
    status_path = directory / "status.json"
    status = _read(status_path) if status_path.exists() else {}
    if status.get("status") == "rolled_back":
        raise ValueError("cutover intent was aborted; retain evidence and plan a fresh activation")
    if candidate is None and status.get("snapshot_id"):
        raise ValueError("issued cutover candidate lost its identity")
    if current["snapshot_id"] != old["snapshot_id"]:
        if not candidate or current != candidate:
            raise ValueError("cutover publication changed")
        return verify_cutover(plan_path, spark)
    for name in ("parameters", "economics"):
        ref = plan["sources"][name]
        if capture_table(Path(ref["table_path"])) != ref:
            raise ValueError("accepted production source changed before cutover")
    atomic_json(directory / "status.json", {"status": "running", "active_step": "native_stage"})
    raw_source = plan["sources"]["raw"]
    clean_raw = _original_raw(_table(spark, raw_source))
    owner = {"kind": KIND, "intent_id": plan["intent_id"], "role": "parameters"}
    source = _table(spark, plan["sources"]["parameters"])
    target = _stage(
        spark,
        _parameter_frame(source, plan["sources"]["parameters"], owner),
        canonical / (PARAMETERS + ".delta"),
        owner,
    )
    if on_commit:
        on_commit("parameters")
    raw_owner = {**owner, "role": "raw"}
    raw = _stage(spark, clean_raw, Path(raw_source["table_path"]), raw_owner, previous=raw_source)
    if on_commit:
        on_commit("raw")
    for name, digest in plan["policy"].items():
        data = (root / "provenance/prepared/economics" / name).read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("accepted v6 policy changed")
        path = canonical / name
        _copy_policy(path, data)
    binding = {
        "status": "ACCEPTED_PRODUCTION_BASELINE",
        "owner": owner,
        "source": plan["sources"]["parameters"],
        "canonical": target,
        "business_columns": source.columns,
        "calculation_policy": plan["calculation_policy"],
        "regimes_sha256": plan["policy"]["bounded-parameter-regimes.json"],
        "dated_official_tc_evidence_sha256": plan["policy"]["dated-official-tc-evidence.json"],
        "acceptance_sha256": plan["acceptance_sha256"],
        "acceptance_path": (directory / "owner-acceptance.json").as_posix(),
        "raw": raw,
        "checks": {"production_parity": "PASS", "origins": "PASS"},
    }
    atomic_json(canonical / BINDING, {**binding, "binding_sha256": _hash(binding)}, immutable=True)
    _equal(source, _table(spark, target).select(*source.columns))
    if (
        capture_table(Path(plan["sources"]["economics"]["table_path"]))
        != plan["sources"]["economics"]
    ):
        raise ValueError("working economics changed during cutover")
    legacy_aliases = {
        name
        for name, ref in old["tables"].items()
        if Path(ref["table_path"]).resolve()
        == Path(plan["sources"]["parameters"]["table_path"]).resolve()
    }
    tables = {
        **{name: ref for name, ref in old["tables"].items() if name not in legacy_aliases},
        PARAMETERS: target,
    }
    for name, ref in list(tables.items()):
        if Path(ref["table_path"]).resolve() == Path(raw["table_path"]).resolve():
            tables[name] = raw
    if candidate is None:
        candidate = create_snapshot(
            tables=tables,
            required_tables=[
                *[name for name in old["required_tables"] if name not in legacy_aliases],
                PARAMETERS,
            ],
            dataset_version=old["dataset_version"],
            run_id=plan["intent_id"],
            code_version=plan["code_version"],
            quality_checks={
                **old["quality_checks"],
                "economics_production_parity": "PASS",
                "canonical_parameter_origins": "PASS",
                "original_raw_projection": "PASS",
                "economics_scope_transition": "PASS",
            },
            parents=[old["snapshot_id"]],
            parameters={
                **old["parameters"],
                "downstream_scope": _transition_scope(
                    plan, old, tables, _read(canonical / BINDING)
                ),
                "economics_canonical_binding": (canonical / BINDING).as_posix(),
                "economics_validation_basis": "accepted_production_baseline",
                "economics_cutover_intent": plan["intent_id"],
            },
        )
        atomic_json(intent_path, candidate, immutable=True)
    elif candidate["tables"] != tables or candidate["run_id"] != plan["intent_id"]:
        raise ValueError("issued cutover candidate changed")
    path = save_snapshot(snapshot_root, candidate)
    if on_commit:
        on_commit("candidate_saved")
    atomic_json(
        status_path,
        {"status": "running", "active_step": "publishing", "snapshot_id": candidate["snapshot_id"]},
    )
    if on_commit:
        on_commit("candidate")
    publish_snapshot(snapshot_root, path, expected_current_id=old["snapshot_id"])
    if on_commit:
        on_commit("published")
    return verify_cutover(plan_path, spark)


def verify_cutover(plan_path, spark):
    """Verify immutable initial equality and selected-publication continuity separately."""
    from pyspark.sql import functions as F

    plan = _load_plan(plan_path)
    _, canonical, directory, snapshot_root = _paths(plan)
    intent = _issued_candidate(plan, directory, snapshot_root)
    current = current_snapshot(snapshot_root)
    if intent is None or current is None:
        raise ValueError("cutover has no selected complete publication")
    cursor, seen = current, set()
    while cursor != intent:
        if cursor["snapshot_id"] in seen or len(cursor["parents"]) != 1:
            raise ValueError("selected publication is not a proven cutover descendant")
        seen.add(cursor["snapshot_id"])
        cursor = load_snapshot(snapshot_root / "manifests" / (cursor["parents"][0] + ".json"))
    _verify_initial_cutover(plan, intent, spark)
    if any(value not in {"PASS", "PASS-NOOP"} for value in current["quality_checks"].values()):
        raise ValueError("selected publication lacks accepted QC")
    ref = intent["tables"][PARAMETERS]
    selected_ref = current["tables"].get(PARAMETERS)
    if (
        selected_ref is None
        or selected_ref["table_path"] != ref["table_path"]
        or selected_ref["table_id"] != ref["table_id"]
        or selected_ref["delta_version"] < ref["delta_version"]
        or "prepared_raw_moex_contract_securities" in current["tables"]
    ):
        raise ValueError("selected publication lost its canonical parameter route")
    for name in (PARAMETERS, "canonical_contract_economics", "raw_moex_contract_securities"):
        active = current["tables"][name]
        if capture_table(Path(active["table_path"])) != active:
            raise ValueError("selected economics route has unpublished producer commits")
        if (
            name != PARAMETERS
            and active["table_id"]
            != plan["sources"]["economics" if name == "canonical_contract_economics" else "raw"][
                "table_id"
            ]
        ):
            raise ValueError("selected economics route changed its table identity")
    selected = _table(spark, selected_ref)
    if (
        selected.where(
            ~F.col("execution_admissible").eqNullSafe(F.lit(True))
            | ~F.coalesce(
                F.col("source_validation_status").isin(
                    "ACCEPTED_PRODUCTION_BASELINE", "CALCULATED_WITH_ACCEPTED_MODEL"
                ),
                F.lit(False),
            )
            | F.col("calculation_origin_json").isNull()
        )
        .limit(1)
        .count()
    ):
        raise ValueError("selected canonical parameters contain unaccepted rows")
    result = {
        "status": "completed",
        "conclusion": "success",
        "snapshot_id": intent["snapshot_id"],
        "selected_snapshot_id": current["snapshot_id"],
        "parameter_rows": ref["row_count"],
        "economics_rows": plan["sources"]["economics"]["row_count"],
        "differences": 0,
        "parity_scope": "initial_owner_accepted_production",
        "selected_publication_continuity": "PASS",
        "source_validation_status": "ACCEPTED_PRODUCTION_BASELINE",
    }
    atomic_json(directory / "status.json", result)
    return result


def _verify_initial_cutover(plan, intent, spark):
    _, canonical, _, snapshot_root = _paths(plan)
    binding = load_canonical_binding(
        canonical / BINDING,
        {
            "prepared_input_table_id": plan["sources"]["parameters"]["table_id"],
            "prepared_input_version": plan["sources"]["parameters"]["delta_version"],
            "regimes_sha256": plan["policy"]["bounded-parameter-regimes.json"],
            "dated_official_tc_evidence_sha256": plan["policy"]["dated-official-tc-evidence.json"],
        },
    )["canonical_binding"]
    ref = intent["tables"][PARAMETERS]
    if binding["canonical"] != ref or binding["owner"]["intent_id"] != plan["intent_id"]:
        raise ValueError("selected initial parameters differ from their accepted binding")
    source = _table(spark, plan["sources"]["parameters"])
    expected = _parameter_frame(source, plan["sources"]["parameters"], binding["owner"])
    _equal(expected, _table(spark, ref))
    _equal(_original_raw(_table(spark, plan["sources"]["raw"])), _table(spark, binding["raw"]))
    if (
        capture_table(Path(plan["sources"]["parameters"]["table_path"]))
        != plan["sources"]["parameters"]
    ):
        raise ValueError("frozen legacy parameters advanced after cutover")
    old = load_snapshot(snapshot_root / "manifests" / (plan["previous_snapshot_id"] + ".json"))
    if old["manifest_sha256"] != plan["previous_manifest_sha256"]:
        raise ValueError("cutover source manifest changed")
    if intent["tables"] != _transition_tables(plan, old, binding):
        raise ValueError("cutover changed unrelated tables or aliases")
    for name, before in old["tables"].items():
        if (
            Path(before["table_path"]).resolve()
            not in {
                Path(plan["sources"][role]["table_path"]).resolve()
                for role in ("raw", "parameters")
            }
            and intent["tables"].get(name) != before
        ):
            raise ValueError("unrelated published table changed during initial cutover")
    return binding


def _transition_tables(plan, source, binding):
    legacy = {
        name for name, ref in source["tables"].items() if ref == plan["sources"]["parameters"]
    }
    if not legacy or source["tables"].get("raw_moex_contract_securities") != plan["sources"]["raw"]:
        raise ValueError("cutover source roles differ from the accepted pins")
    return {
        **{name: ref for name, ref in source["tables"].items() if name not in legacy},
        PARAMETERS: binding["canonical"],
        "raw_moex_contract_securities": binding["raw"],
    }


def _snapshot_identity(manifest):
    return {key: manifest[key] for key in ("snapshot_id", "manifest_sha256")}


def _scope_from_transition(report, tables):
    return {
        "version": 2,
        "full_refresh": False,
        "windows": [],
        "effective_intervals": [],
        "covered_tables": {
            name: {key: ref[key] for key in ("table_id", "delta_version")}
            for name, ref in tables.items()
        },
        "economics_cutover": report,
    }


def _transition_scope(plan, source, tables, binding, *, cutover=None):
    report = {
        "schema_version": 1,
        "source": _snapshot_identity(source),
        "cutover": _snapshot_identity(cutover) if cutover is not None else None,
        "target_tables_sha256": _hash(tables),
        "intent_id": plan["intent_id"],
        "binding_sha256": binding["binding_sha256"],
        "row_counts": {
            "parameters": binding["canonical"]["row_count"],
            "raw": binding["raw"]["row_count"],
            "economics": plan["sources"]["economics"]["row_count"],
        },
        "differences": 0,
        "runtime_provenance": runtime_provenance(),
    }
    report["report_sha256"] = _hash(report)
    return _scope_from_transition(report, tables)


def _validate_repair_envelope(candidate, issued, scope):
    ignored = {"snapshot_id", "created_at_utc", "manifest_sha256"}
    expected = {key: value for key, value in issued.items() if key not in ignored}
    expected.update(
        run_id=issued["run_id"] + ":research-scope",
        code_version="economics-scope-transition.v1",
        parents=[issued["snapshot_id"]],
        quality_checks={**issued["quality_checks"], "economics_scope_transition": "PASS"},
        parameters={**issued["parameters"], "downstream_scope": scope},
    )
    if {key: value for key, value in candidate.items() if key not in ignored} != expected:
        raise ValueError("issued scope repair envelope changed")


def _issued_scope_candidate(plan, directory, registry):
    path = directory / "research-scope-intent.json"
    candidate = load_snapshot(path) if path.exists() else None
    issued = [
        load_snapshot(path)
        for path in (registry / "manifests").glob("*.json")
        if json.loads(path.read_text(encoding="utf-8")).get("run_id")
        == plan["intent_id"] + ":research-scope"
    ]
    if candidate is None and issued:
        raise ValueError("saved scope repair lost its issued identity intent")
    if candidate is not None and any(item != candidate for item in issued):
        raise ValueError("scope repair has conflicting issued identities")
    return candidate


def validate_economics_scope_transition(manifest, *, root):
    """Discharge only the native-proved zero-effect economics migration edge."""
    from trading_advisor_3000.product_plane.contracts.schema_validation import (
        load_schema,
        validate_schema,
    )

    scope = manifest["parameters"]["downstream_scope"]
    report = scope["economics_cutover"]
    schema = Path(__file__).parents[2] / "contracts/schemas/data_economics_scope_transition.v1.json"
    validate_schema(load_schema(schema), report)
    if report["report_sha256"] != _hash({k: v for k, v in report.items() if k != "report_sha256"}):
        raise ValueError("economics scope report changed")
    if PARAMETERS not in manifest["tables"]:
        raise ValueError("economics scope lacks canonical parameters")
    canonical = Path(manifest["tables"][PARAMETERS]["table_path"]).parent
    plan = _load_accepted_plan(canonical / ".production-cutover/plan.json")
    _, _, directory, registry = _paths(plan)
    if Path(root).resolve() != registry.resolve():
        raise ValueError("economics scope registry differs from its owning root")
    issued = _issued_candidate(plan, directory, registry)
    source = load_snapshot(registry / "manifests" / (plan["previous_snapshot_id"] + ".json"))
    edge = manifest
    if report["cutover"] is not None:
        edge = load_snapshot(registry / "manifests" / (report["cutover"]["snapshot_id"] + ".json"))
        if (
            _snapshot_identity(edge) != report["cutover"]
            or manifest["parents"] != [edge["snapshot_id"]]
            or manifest["tables"] != edge["tables"]
        ):
            raise ValueError("economics scope repair changed its target")
        _validate_repair_envelope(manifest, edge, scope)
    binding = _read(canonical / BINDING)
    binding = load_canonical_binding(
        canonical / BINDING,
        {
            "prepared_input_table_id": plan["sources"]["parameters"]["table_id"],
            "prepared_input_version": plan["sources"]["parameters"]["delta_version"],
            "regimes_sha256": plan["policy"]["bounded-parameter-regimes.json"],
            "dated_official_tc_evidence_sha256": plan["policy"]["dated-official-tc-evidence.json"],
        },
    )["canonical_binding"]
    if (
        issued is None
        or edge != issued
        or edge["parents"] != [source["snapshot_id"]]
        or source["manifest_sha256"] != plan["previous_manifest_sha256"]
        or report["source"] != _snapshot_identity(source)
        or report["intent_id"] != plan["intent_id"]
        or report["binding_sha256"] != binding["binding_sha256"]
        or report["target_tables_sha256"] != _hash(manifest["tables"])
        or manifest["tables"] != _transition_tables(plan, source, binding)
        or report["row_counts"]
        != {
            "parameters": binding["canonical"]["row_count"],
            "raw": binding["raw"]["row_count"],
            "economics": plan["sources"]["economics"]["row_count"],
        }
        or scope != _scope_from_transition(report, manifest["tables"])
        or manifest["quality_checks"].get("economics_scope_transition") != "PASS"
        or any(
            value not in {"PASS", "PASS-NOOP"}
            for item in (source, edge, manifest)
            for value in item["quality_checks"].values()
        )
    ):
        raise ValueError("economics scope transition differs from its accepted edge")
    for item in (source, edge):
        for ref in item["tables"].values():
            open_pinned_table(ref)
    return source


def repair_downstream_scope(plan_path, spark, *, expected_data_id, publish=False, on_commit=None):
    """Native audit plus optional immutable metadata child; never rewrite history."""
    from trading_advisor_3000.product_plane.common.data_snapshots import assert_tables_unchanged

    plan = _load_accepted_plan(plan_path)
    _, canonical, directory, registry = _paths(plan)
    with economics_writer_lock(canonical):
        issued = _issued_candidate(plan, directory, registry)
        if issued is None or issued["snapshot_id"] != expected_data_id:
            raise ValueError("scope repair requires its exact initial cutover ID")
        selected = current_snapshot(registry)
        intent_path = directory / "research-scope-intent.json"
        candidate = _issued_scope_candidate(plan, directory, registry)
        if candidate is not None and selected == candidate:
            validate_economics_scope_transition(candidate, root=registry)
            assert_tables_unchanged(candidate["tables"])
            return {"status": "PASS-NOOP", "published": True, "manifest": candidate}
        if selected != issued:
            raise ValueError("scope repair refuses an advanced or unrelated publication")
        assert_tables_unchanged(issued["tables"])
        provenance = runtime_provenance()
        binding = _verify_initial_cutover(plan, issued, spark)
        source = load_snapshot(registry / "manifests" / (plan["previous_snapshot_id"] + ".json"))
        scope = _transition_scope(plan, source, issued["tables"], binding, cutover=issued)
        if candidate is None:
            candidate = create_snapshot(
                tables=issued["tables"],
                required_tables=issued["required_tables"],
                dataset_version=issued["dataset_version"],
                run_id=plan["intent_id"] + ":research-scope",
                code_version="economics-scope-transition.v1",
                quality_checks={**issued["quality_checks"], "economics_scope_transition": "PASS"},
                parents=[issued["snapshot_id"]],
                parameters={**issued["parameters"], "downstream_scope": scope},
                window=issued["window"],
            )
        elif candidate["parameters"]["downstream_scope"] != scope:
            raise ValueError("issued scope repair differs from this audit release")
        validate_economics_scope_transition(candidate, root=registry)
        assert_tables_unchanged(issued["tables"])
        if runtime_provenance() != provenance or current_snapshot(registry) != issued:
            raise ValueError("scope repair code or selected Data changed during audit")
        if publish:
            atomic_json(intent_path, candidate, immutable=True)
            path = save_snapshot(registry, candidate)
            if on_commit:
                on_commit("scope_saved")
            publish_snapshot(registry, path, expected_current_id=expected_data_id)
            if on_commit:
                on_commit("scope_published")
        return {"status": "PASS", "published": publish, "manifest": candidate}


def rollback_downstream_scope(plan_path, *, expected_data_id):
    from trading_advisor_3000.product_plane.common.data_snapshots import assert_tables_unchanged

    plan = _load_accepted_plan(plan_path)
    _, canonical, directory, registry = _paths(plan)
    with economics_writer_lock(canonical):
        candidate = _issued_scope_candidate(plan, directory, registry)
        issued = _issued_candidate(plan, directory, registry)
        if candidate is None or candidate["snapshot_id"] != expected_data_id:
            raise ValueError("scope rollback requires its exact issued repair ID")
        validate_economics_scope_transition(candidate, root=registry)
        assert_tables_unchanged(candidate["tables"])
        current = current_snapshot(registry)
        if current == issued:
            return {"status": "PASS-NOOP", "snapshot_id": issued["snapshot_id"]}
        if current != candidate:
            raise ValueError("scope rollback refuses a later publication")
        path = registry / "manifests" / (issued["snapshot_id"] + ".json")
        publish_snapshot(registry, path, expected_current_id=expected_data_id)
        return {"status": "rolled_back", "snapshot_id": issued["snapshot_id"]}


@_exclusive
def rollback_cutover(plan_path, spark):
    """Abort own partials or undo publication while quiesced, before renewal."""
    plan = _load_plan(plan_path)
    _, canonical, directory, snapshot_root = _paths(plan)
    old_path = snapshot_root / "manifests" / (plan["previous_snapshot_id"] + ".json")
    old = load_snapshot(old_path)
    if old["manifest_sha256"] != plan["previous_manifest_sha256"]:
        raise ValueError("rollback baseline manifest changed")
    current = current_snapshot(snapshot_root)
    intent_path = directory / "candidate-intent.json"
    candidate = load_snapshot(intent_path) if intent_path.exists() else None
    if candidate is None and current["run_id"] == plan["intent_id"]:
        candidate = current
    if current != old and (candidate is None or current != candidate):
        raise ValueError("refuse rollback over unrelated publication")
    if candidate and (
        candidate["run_id"] != plan["intent_id"] or candidate["parents"] != [old["snapshot_id"]]
    ):
        raise ValueError("rollback candidate belongs to another intent")
    for role in ("parameters", "economics"):
        before = plan["sources"][role]
        if capture_table(Path(before["table_path"])) != before:
            raise ValueError("refuse rollback after new producer commits")
    target = canonical / (PARAMETERS + ".delta")
    if (target / "_delta_log").exists():
        table = DeltaTable(str(target))
        owner = {"kind": KIND, "intent_id": plan["intent_id"], "role": "parameters"}
        if table.version() != 0 or _owner(table, 0) != owner:
            raise ValueError("refuse rollback after canonical writer commits")
        if candidate and capture_table(target) != candidate["tables"][PARAMETERS]:
            raise ValueError("rollback canonical target differs from issued candidate")
    elif current != old:
        raise ValueError("selected canonical target disappeared")
    raw_original = plan["sources"]["raw"]
    raw_path = Path(raw_original["table_path"])
    raw_table = DeltaTable(str(raw_path))
    raw_current = capture_table(raw_path)
    rollback_owner = {"kind": KIND, "intent_id": plan["intent_id"], "role": "rollback_raw"}
    raw_owner = {**rollback_owner, "role": "raw"}
    restored = _owner(raw_table, raw_table.version()) == rollback_owner and all(
        raw_current[k] == v for k, v in raw_original.items() if k != "delta_version"
    )
    clean = (
        _owner(raw_table, raw_table.version()) == raw_owner
        and raw_table.version() == raw_original["delta_version"] + 1
    )
    if raw_current != raw_original and not restored and not clean:
        raise ValueError("refuse rollback after another RAW writer commit")
    if clean:
        # Spark RESTORE also restores schema; delta-rs 0.25 retained the newer schema.
        key = "spark.databricks.delta.commitInfo.userMetadata"
        previous_metadata = spark.conf.get(key, "")
        spark.conf.set(key, json.dumps(rollback_owner, sort_keys=True))
        try:
            escaped_path = raw_path.as_posix().replace("`", "``")
            spark.sql(
                f"RESTORE TABLE delta.`{escaped_path}` "
                f"TO VERSION AS OF {raw_original['delta_version']}"
            ).collect()
        finally:
            if previous_metadata:
                spark.conf.set(key, previous_metadata)
            else:
                spark.conf.unset(key)
        restored_ref = capture_table(raw_path)
        if any(restored_ref[k] != v for k, v in raw_original.items() if k != "delta_version"):
            raise ValueError("RAW restore differs from its retained original pin")
    if current != old:
        publish_snapshot(snapshot_root, old_path, expected_current_id=current["snapshot_id"])
    result = {"status": "rolled_back", "snapshot_id": old["snapshot_id"]}
    atomic_json(directory / "status.json", result)
    return result
