"""Closed observed CF family: source admission, native frames and pinned readback."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from trading_advisor_3000.product_plane.common.data_snapshots import (
    SnapshotUnavailableError,
    load_snapshot,
    open_pinned_table,
    read_spark_delta,
    snapshot_read_scope,
)
from trading_advisor_3000.product_plane.common.immutable_files import (
    canonical_bytes,
    child,
    pinned_bytes,
)
from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract
from trading_advisor_3000.product_plane.data_plane.native_observations import physical_schema
from trading_advisor_3000.spark_jobs.continuous_front_job import (
    calendar_active_intervals,
    validate_calendar_sources,
)

from .native_cf_policy import (
    LIMITATIONS,
    LIMITS,
    NEW_ROLES,
    TABLES,
    cf_layouts,
    cf_policy,
    cf_policy_ref,
)
from .native_observations import NativeResearchSnapshot, require, sha


def definition_json(definition):
    return canonical_bytes(definition).decode("utf-8")


def _object(root, reference, what):
    value = json.loads(pinned_bytes(root, reference))
    require(isinstance(value, dict), what + " must be an object")
    return value


def _parent(root, completion_pin, layer, visited=()):
    require(layer in NEW_ROLES, "unsupported CF layer")
    token = (root.resolve().as_posix(), completion_pin["path"], completion_pin["sha256"])
    require(token not in visited and len(visited) < 4, "CF receipt cycle/depth exceeded")
    receipt = _object(root, completion_pin, "CF parent receipt")
    expected = {"CF": "L0", "CF-L1": "CF", "CF-L2": "CF-L1"}[layer]
    version = "native_research_l0_completion.v1" if expected == "L0" else "native_cf_completion.v1"
    require(
        receipt.get("layer") == expected and receipt.get("contract_version") == version,
        "CF parent family/layer mismatch",
    )
    context = _object(root, receipt["context"], "CF parent context")
    require(context.get("layer") == expected, "CF parent context layer mismatch")
    require(
        context.get("contract_version")
        == ("native_research_l0_context.v1" if expected == "L0" else "native_cf_context.v1"),
        "CF parent context family mismatch",
    )
    if expected != "L0":
        expected_profile = (
            "native_cf_composition.v1" if expected == "CF" else "native_cf_prefix_l1.v1"
        )
        require(
            isinstance(context.get("policy"), dict)
            and context["policy"].get("profile_version") == expected_profile,
            "CF parent profile mismatch",
        )
    if expected == "L0":
        reader, graph = NativeResearchSnapshot._verified(root, completion_pin)
    else:
        reader, graph = NativeCfSnapshot._verified(root, completion_pin, _visited=visited)
    return reader, graph


def _source(completion_pin, graph):
    receipt, context = graph["completion"], graph["context"]
    l0 = receipt["layer"] == "L0"
    return {
        "parent_completion": completion_pin,
        "parent_layer": receipt["layer"],
        "parent_contract_version": receipt["contract_version"],
        "parent_profile_version": None if l0 else context["policy"]["profile_version"],
        "l0_completion": completion_pin if l0 else context["source"]["l0_completion"],
        "parent_snapshot": receipt["snapshot"],
        "parent_context": receipt["context"],
        "namespace": context["source"]["namespace"],
    }


def _component_expiration(root, component):
    from trading_advisor_3000.spark_jobs.continuous_front_job import _calendar_source_pair

    source = component["last_trade_source"]
    raw, _ = _calendar_source_pair(root, source["body"], source["receipt"])
    block = json.loads(raw)[source["block"]]
    rows = [dict(zip(block["columns"], row, strict=True)) for row in block["data"]]
    if source["kind"] == "moex_iss_description":
        values = [r["value"] for r in rows if r.get("name") == "LSTDELDATE"]
    else:
        values = [r["LASTDELDATE"] for r in rows if r.get("SECID") == component["source_symbol"]]
    require(
        values == [component["expiration_date"]],
        "CF expiration date differs from original delivery field",
    )


def validate_cf_facts(root, intent, l0_graph):
    """Actual bytes and native source controls, never admission booleans."""
    from decimal import Decimal

    from trading_advisor_3000.spark_jobs.continuous_front_job import _calendar_source_pair

    refs = {p["path"]: p for p in intent["source_fact_pins"]}
    for ref in refs.values():
        pinned_bytes(root, ref)
    packet_pin = {
        "path": "source-facts/temporal/facts.json",
        "sha256": "c018f490796c58c3816152d61db40cdab8d20f09e86c61ce896d2107b090219d",
    }
    packet = _object(root, packet_pin, "CF admitted source facts")
    required = [packet_pin]
    for rule in intent["calendar_roll_schedule"]["governing_rules"]:
        required += [rule["body"], rule["receipt"]]
    for group in intent["calendar_roll_schedule"]["groups"]:
        for component in group["components"]:
            required += [component["last_trade_source"][k] for k in ["body", "receipt"]]
    parent = l0_graph["tables"]["research_bar_views"]
    namespace = l0_graph["context"]["source"]["namespace"]
    for definition in intent["definitions"]:
        basis = definition["numeric_price_basis"]
        require(
            basis["parent_l0_completion"] == intent["parent_completion"],
            "CF numeric basis requires exact original L0 completion",
        )
        required += [basis["source_binding"], basis["dated_specification"]]
        require(
            basis["dated_specification"]["sha256"]
            == packet["direct_facts"]["dated_sbrf_specification"]["sha256"],
            "CF quote basis differs from reviewed dated specification",
        )
        admission = _object(root, basis["source_binding"], "CF source binding")
        require(
            admission.get("schema") == "sbrf_numeric_quote_original_MD_binding.v1",
            "unsupported CF numeric source admission",
        )
        require(
            admission["source_MD_completion"]["sha256"]
            == l0_graph["intent"]["source_completion"]["sha256"],
            "CF numeric basis MD source mismatch",
        )
        require(
            admission["namespace"] == namespace["namespace"], "CF numeric source namespace mismatch"
        )
        require(
            admission["dated_specification"] == basis["dated_specification"],
            "CF dated specification binding mismatch",
        )
        required += [
            admission[k] for k in ["source_MD_completion", "source_manifest", "assignments"]
        ]
        controls = admission["four_daily_original_byte_comparisons"]
        require(len(controls) == 4, "CF requires four independent daily source controls")
        component_ids = {c["instrument"]["instrument_id"] for c in definition["components"]}
        daily_fields = ["instrument_id", "source_symbol", "source_begin", "source_end"]
        daily_rows = (
            parent.filter(
                pc.and_(
                    pc.is_in(parent["instrument_id"], value_set=pa.array(sorted(component_ids))),
                    pc.equal(parent["timeframe"], "1d"),
                )
            )
            .select(daily_fields)
            .to_pylist()
        )
        expected_controls = {tuple(r[k] for k in daily_fields) for r in daily_rows}
        actual_controls = [tuple(c[k] for k in daily_fields) for c in controls]
        reviewed_scope = {
            (symbol, "2026-09-" + day) for symbol in ["SRU6", "SRZ6"] for day in ["14", "15"]
        }
        require(
            len(daily_rows)
            == len(expected_controls)
            == len(actual_controls)
            == len(set(actual_controls))
            == 4
            and {(r["source_symbol"], r["source_begin"][:10]) for r in daily_rows} == reviewed_scope
            and set(actual_controls) == expected_controls,
            "CF daily control coverage differs from trusted parent daily scope",
        )
        for control in controls:
            raw, _ = _calendar_source_pair(
                root, control["original_candle_body"], control["original_candle_receipt"]
            )
            payload = json.loads(raw, parse_float=Decimal)["candles"]
            rows = [dict(zip(payload["columns"], row, strict=True)) for row in payload["data"]]
            values = [r for r in rows if r["begin"] == control["source_begin"]]
            require(len(values) == 1, "missing/duplicate original daily candle control")
            mask = pc.and_(
                pc.equal(parent["instrument_id"], control["instrument_id"]),
                pc.and_(
                    pc.equal(parent["timeframe"], "1d"),
                    pc.equal(parent["source_begin"], control["source_begin"]),
                ),
            )
            actual = parent.filter(mask).to_pylist()
            require(len(actual) == 1, "CF original daily control absent from L0")
            row = actual[0]
            require(
                row["source_end"] == control["source_end"]
                and row["body_sha256"] == control["original_candle_body"]["sha256"]
                and row["receipt_sha256"] == control["original_candle_receipt"]["sha256"],
                "CF daily native lineage mismatch",
            )
            history_name = "iss_" + control["source_symbol"].lower() + "_history_sep14_15"
            history_matches = [
                ref for path, ref in refs.items() if path.endswith("/" + history_name + ".json")
            ]
            history_receipts = [
                ref
                for path, ref in refs.items()
                if path.endswith("/" + history_name + ".receipt.json")
            ]
            require(
                len(history_matches) == len(history_receipts) == 1,
                "missing independent history source control",
            )
            history_raw, history_url = _calendar_source_pair(
                root, history_matches[0], history_receipts[0]
            )
            require(
                history_url.hostname == "iss.moex.com"
                and history_url.path.endswith("/" + control["source_symbol"] + ".json"),
                "foreign history source control",
            )
            block = json.loads(history_raw, parse_float=Decimal)["history"]
            history_rows = [dict(zip(block["columns"], r, strict=True)) for r in block["data"]]
            history = [
                r
                for r in history_rows
                if r["TRADEDATE"] == control["source_begin"][:10]
                and r["SECID"] == control["source_symbol"]
            ]
            require(len(history) == 1, "missing/duplicate independent daily history row")
            for name in ["open", "high", "low", "close", "volume"]:
                expected = Decimal(str(control["OHLCV_exact_native_source_values"][name]))
                require(
                    Decimal(str(values[0][name]))
                    == expected
                    == Decimal(str(row[name]))
                    == Decimal(str(history[0][name.upper()])),
                    "CF original daily OHLCV mismatch: " + name,
                )
        for component in definition["components"]:
            instrument = component["instrument"]
            _component_expiration(root, component)
            require(
                instrument in namespace["instruments"], "CF component differs from parent namespace"
            )
            rows = parent.filter(pc.equal(parent["instrument_id"], instrument["instrument_id"]))
            require(rows.num_rows > 0, "CF component absent from source")
            require(
                set(rows["source_symbol"].to_pylist()) == {component["source_symbol"]},
                "CF component source symbol differs from trusted parent instrument",
            )
            require(
                all(
                    json.loads(value) == component["units"]
                    for value in rows["source_units_json"].to_pylist()
                ),
                "CF raw units differ from source",
            )
        key = definition_json(definition)
        matches = [g for g in intent["calendar_roll_schedule"]["groups"] if g["group_key"] == key]
        require(
            len(matches) == 1
            and matches[0]["roll_offset_trading_days"] == definition["roll"]["offset_sessions"],
            "CF definition/calendar mismatch",
        )
        group = matches[0]
        eligible = parent.filter(
            pc.is_in(
                parent["instrument_id"],
                value_set=pa.array(
                    [c["instrument"]["instrument_id"] for c in definition["components"]]
                ),
            )
        )
        require(
            all(
                group["coverage_from"] <= label[:10] < group["coverage_to"]
                for label in eligible["source_begin"].to_pylist()
            ),
            "CF parent source date lacks admitted calendar coverage",
        )
        require(
            [
                (
                    c["component_key"],
                    c["source_symbol"],
                    c["last_trade_date"],
                    c["last_trade_source"],
                )
                for c in sorted(matches[0]["components"], key=lambda c: c["source_order"])
            ]
            == [
                (
                    c["instrument"]["instrument_id"],
                    c["source_symbol"],
                    c["last_trade_date"],
                    c["last_trade_source"],
                )
                for c in definition["components"]
            ],
            "CF source order/component facts mismatch",
        )
    require(
        len(intent["calendar_roll_schedule"]["groups"]) == len(intent["definitions"]),
        "undeclared CF calendar group",
    )
    require(all(refs.get(p["path"]) == p for p in required), "CF required source fact not declared")
    validate_calendar_sources(root, intent["calendar_roll_schedule"])


def cf_numerical_identity(layer):
    from .native_feature_materialization import _check_worker_engines, core_worker_engines
    from .semantic_fingerprint import SymbolGraph

    engines = core_worker_engines()
    _check_worker_engines(engines)
    prefix = "trading_advisor_3000."
    roots = {
        "selector": prefix + "spark_jobs.continuous_front_job.calendar_selected_frame",
        "adapter": prefix + "product_plane.research.native_cf._cf_native_frames",
        "indicators": prefix
        + "product_plane.research.indicators.materialize._zero_anchor_price_frame",
        "derived": prefix
        + "product_plane.research.derived_indicators.materialize._with_continuous_front_projection",
    }
    result = {
        "version": "native_cf_numerical.v1",
        "layer": layer,
        "basis": "observed_cf_composed_numeric_sequence.v1",
        "policy_sha256": sha(cf_policy(layer)),
        "kernels": {
            k: SymbolGraph(Path(__file__).resolve().parents[2]).fingerprint(
                (
                    symbol,
                    prefix + "product_plane.research.native_cf._cf_input_metrics",
                    prefix + "spark_jobs.continuous_front_job.roll_gap_expression",
                )
                if k == "adapter"
                else (symbol,)
            )
            for k, symbol in roots.items()
        },
        "engines": engines,
    }
    return {**result, "digest": sha(result)}


def build_cf_command(
    source_reader, *, command_id, layer="CF", definitions, calendar_roll_schedule, source_fact_pins
):
    require(layer == "CF", "CF sidecar producer not implemented")
    graph = source_reader.verify()
    require(graph["completion"]["layer"] == "L0", "CF requires completed native L0")
    root = source_reader.root
    completion_pin = json.loads(source_reader._completion_json)
    intent = {
        "schema_version": 1,
        "contract_version": "native_cf_intent.v1",
        "input_mode": "native_cf_observation.v1",
        "layer": layer,
        "command_id": command_id,
        "source_root": "/research-trial",
        "parent_completion": completion_pin,
        "definitions": definitions,
        "calendar_roll_schedule": calendar_roll_schedule,
        "source_fact_pins": source_fact_pins,
        "policy": cf_policy_ref(layer),
        "policy_sha256": sha(cf_policy(layer)),
        "numerical_identity": cf_numerical_identity(layer),
    }
    from .native_observation_materialization import producer_code_sha256

    intent["code_sha256"] = producer_code_sha256()
    validate_identity_contract(
        "native_cf_intent.v1.json", intent, graph["context"]["source"]["namespace"]
    )
    validate_cf_facts(root, intent, graph)
    return intent


def _cf_input_metrics(pdf):
    from .indicators.materialize import _zero_anchor_price_frame

    pdf = pdf.sort_values("source_begin").reset_index(drop=True)
    gaps = {
        int(r["roll_epoch"]): float(r["_gap"])
        for r in pdf.to_dict("records")
        if r["is_first_bar_after_roll"]
    }
    adjusted = _zero_anchor_price_frame(pdf, gap_by_sequence=gaps)
    result = pdf[["cf_definition_json", "timeframe", "source_begin"]].copy()
    for name in ["true_range", "log_ret_1"]:
        result[name] = adjusted[name].astype("Float64").to_numpy(dtype="float64", na_value=np.nan)
        result.loc[~np.isfinite(result[name]), name] = np.nan
    return result


def _arrow_equal_values(actual, expected):
    require(
        actual.schema.equals(expected.schema, check_metadata=True),
        "CF expected physical schema mismatch",
    )
    for field in actual.schema:
        a, b = actual[field.name], expected[field.name]
        if pa.types.is_floating(field.type):
            require(a.is_null().equals(b.is_null()), "CF metric NULL mask mismatch")
            require(
                np.allclose(
                    a.drop_null().to_numpy(), b.drop_null().to_numpy(), rtol=1e-12, atol=1e-10
                ),
                "CF numeric input mismatch",
            )
        else:
            require(a.equals(b), "CF selected source buffer/lineage mismatch: " + field.name)


class NativeCfSnapshot:
    """Explicit closed CF receipt; fresh graph on every public call."""

    def __init__(self, root, completion_pin):
        self.root = Path(root).resolve()
        self._completion_json = canonical_bytes(completion_pin).decode("utf-8")
        self.verify()

    @classmethod
    def _verified(cls, root, completion_pin, *, _visited=()):
        reader = cls.__new__(cls)
        reader.root = Path(root).resolve()
        reader._completion_json = canonical_bytes(completion_pin).decode("utf-8")
        return reader, reader._verify(_visited)

    def verify(self):
        return self._verify(())

    def _verify(self, visited):
        try:
            completion_pin = json.loads(self._completion_json)
            token = (self.root.as_posix(), completion_pin["path"], completion_pin["sha256"])
            require(token not in visited and len(visited) < 4, "CF receipt cycle/depth exceeded")
            completion = _object(self.root, completion_pin, "CF completion")
            require(
                completion.get("contract_version") == "native_cf_completion.v1"
                and completion.get("layer") in NEW_ROLES,
                "invalid CF completion family/layer",
            )
            graph = validate_cf_graph(
                self.root,
                snapshot_pin=completion["snapshot"],
                context_pin=completion["context"],
                intent_pin=completion["intent"],
                input_sha256=completion["input_sha256"],
                visited=(*visited, token),
            )
            context, intent, manifest = graph["context"], graph["intent"], graph["manifest"]
            validate_identity_contract(
                "native_cf_completion.v1.json", completion, context["source"]["namespace"]
            )
            require(
                all(
                    completion[k] == intent[k]
                    for k in ["layer", "command_id", "parent_completion", "source_root"]
                ),
                "CF completion/intent mismatch",
            )
            require(
                completion["layer"] == context["layer"]
                and completion["snapshot"]["snapshot_id"] == manifest["snapshot_id"]
                and completion["attempt_id"] == manifest["run_id"],
                "CF completion/graph mismatch",
            )
            return {
                **graph,
                "completion": completion,
                "completion_pin": completion_pin,
                "root": self.root.as_posix(),
            }
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SnapshotUnavailableError("invalid unpublished CF candidate: " + str(exc)) from exc

    def _require_graph(self, graph):
        require(
            graph["root"] == self.root.as_posix()
            and graph["completion_pin"] == json.loads(self._completion_json),
            "foreign CF graph root/completion",
        )

    def _read_role_verified(self, graph, role):
        self._require_graph(graph)
        require(role in graph["context"]["layouts"], "unknown CF table role")
        return graph["tables"][TABLES[role]]

    def read_role(self, role):
        return self._read_role_verified(self.verify(), role)

    def _compute_metadata(self, graph):
        self._require_graph(graph)
        context = graph["context"]
        result = {
            "schema_version": 1,
            "contract_version": "native_cf_compute.v1",
            "input_mode": "native_cf_observation.v1",
            "layer": context["layer"],
            "root": "/research-trial",
            "completion": json.loads(self._completion_json),
            "context": graph["completion"]["context"],
            "snapshot": graph["completion"]["snapshot"],
            "parent_completion": context["source"]["parent_completion"],
            "l0_completion": context["source"]["l0_completion"],
            "namespace": context["source"]["namespace"],
            "context_payload": context,
            "tables": [
                {
                    "role": role,
                    "table_name": TABLES[role],
                    "reference": graph["manifest"]["tables"][TABLES[role]],
                    "layout": layout,
                }
                for role, layout in context["layouts"].items()
            ],
            "limits": context["limits"],
            "limitations": context["limitations"],
        }
        validate_identity_contract("native_cf_compute.v1.json", result, result["namespace"])
        return result

    def compute_metadata(self):
        return self._compute_metadata(self.verify())

    def read_spark(self, spark):
        graph = self.verify()
        metadata = self._compute_metadata(graph)
        role = {"CF": "bars", "CF-L1": "indicators", "CF-L2": "derived"}[metadata["layer"]]
        reference = graph["manifest"]["tables"][TABLES[role]]
        with snapshot_read_scope(graph["manifest"]):
            frame = read_spark_delta(
                spark, reference["table_path"], version=reference["delta_version"]
            )
        require(frame.count() == reference["row_count"], "CF Spark row count mismatch")
        require(
            [(f.name, f.dataType.simpleString()) for f in frame.schema]
            == [(f["name"], f["sql_type"]) for f in graph["context"]["layouts"][role]["fields"]],
            "CF Spark physical schema mismatch",
        )
        return frame, metadata


def expected_cf_base(parent, intent, layouts, context_id):
    """Bounded independent read-side source selection and reference reconstruction."""
    import pandas as pd

    native_names = [f["name"] for f in layouts["bars"]["fields"]]
    native_names = native_names[: native_names.index("cf_definition_json")]
    source_rows = parent["tables"]["research_bar_views"].to_pylist()
    intervals = calendar_active_intervals(intent["calendar_roll_schedule"])
    bars, events, qcs, sequences, used, capabilities = [], [], [], [], [], []
    for definition in intent["definitions"]:
        key = definition_json(definition)
        members = {c["instrument"]["instrument_id"] for c in definition["components"]}
        eligible = [r for r in source_rows if r["instrument_id"] in members]
        for timeframe in sorted({r["timeframe"] for r in eligible}):
            all_rows = [r for r in source_rows if r["timeframe"] == timeframe]
            component_rows = [r for r in eligible if r["timeframe"] == timeframe]
            selected = []
            for row in component_rows:
                matches = [
                    i
                    for i in intervals
                    if i["group_key"] == key
                    and i["component_key"] == row["instrument_id"]
                    and i["active_from"] <= row["source_begin"][:10] < i["active_to"]
                ]
                if matches:
                    require(len(matches) == 1, "ambiguous CF active component")
                    selected.append((row, matches[0]))
            selected.sort(key=lambda pair: pair[0]["source_begin"])
            require(
                selected and len({r["source_begin"] for r, _ in selected}) == len(selected),
                "empty/duplicate CF selected source key",
            )
            seqrows = []
            seqevents = []
            epoch = 0
            embargo = ""
            previous = None
            for row, planned in selected:
                is_roll = previous is not None and previous["instrument_id"] != row["instrument_id"]
                gap = 0.0
                if is_roll:
                    epoch += 1
                    event = {
                        "cf_definition_json": key,
                        "timeframe": timeframe,
                        "cf_context_id": context_id,
                        "roll_ordinal": epoch,
                        "planned_roll_ordinal": planned["planned_roll_ordinal"],
                        "switch_source_date": planned["active_from"],
                        "observed_switch_source_begin": row["source_begin"],
                        "old_instrument_id": previous["instrument_id"],
                        "new_instrument_id": row["instrument_id"],
                    }
                    for side, ref in [("old", previous), ("new", row)]:
                        for field in [
                            "source_begin",
                            "source_end",
                            "body_sha256",
                            "receipt_sha256",
                            "row_ordinal",
                        ]:
                            event[side + "_" + field] = ref[field]
                        event[side + "_reference_close"] = ref["close"]
                        used.append(
                            {
                                "definition_json": key,
                                "timeframe": timeframe,
                                "roll_ordinal": epoch,
                                "side": side,
                                "instrument_id": ref["instrument_id"],
                                **{
                                    field: ref[field]
                                    for field in [
                                        "source_begin",
                                        "source_end",
                                        "body_sha256",
                                        "receipt_sha256",
                                        "row_ordinal",
                                    ]
                                },
                            }
                        )
                    gap = float(row["close"]) - float(previous["close"])
                    embargo = max(embargo, row["source_end"], previous["source_end"])
                    event.update(
                        additive_gap=gap, reference_calculation_embargo_source_label=embargo
                    )
                    seqevents.append(event)
                item = {
                    name: row["instrument_id" if name == "source_instrument_id" else name]
                    for name in native_names
                }
                item.update(
                    cf_definition_json=key,
                    cf_context_id=context_id,
                    cf_recipe_version="native_cf_composition.v1",
                    instrument_id=None,
                    active_instrument_id=row["instrument_id"],
                    planned_roll_ordinal=planned["planned_roll_ordinal"],
                    roll_epoch=epoch,
                    is_first_bar_after_roll=is_roll,
                    reference_calculation_embargo_source_label=max(embargo, row["source_end"]),
                    _gap=gap,
                    _analysis=row["slice_role"] == "analysis",
                )
                seqrows.append(item)
                previous = row
            metrics = _cf_input_metrics(pd.DataFrame(seqrows))
            analysis = sum(r["_analysis"] for r in seqrows)
            require(analysis > 0, "CF sequence has no analysis observation")
            for index, row in enumerate(seqrows):
                for name in ["true_range", "log_ret_1"]:
                    value = metrics.iloc[index][name]
                    row[name] = None if not np.isfinite(value) else float(value)
            sequences.append(
                {
                    "definition": definition,
                    "timeframe": timeframe,
                    "selected_rows": len(seqrows),
                    "analysis_rows": analysis,
                    "first_source_begin": seqrows[0]["source_begin"],
                    "last_source_begin": seqrows[-1]["source_begin"],
                    "last_source_end": seqrows[-1]["source_end"],
                    "planned_roll_ordinals": sorted({r["planned_roll_ordinal"] for r in seqrows}),
                    "observed_events": len(seqevents),
                }
            )
            for name, needed in [("true_range", 1), ("log_ret_1", 2)]:
                finite = [i for i, r in enumerate(seqrows) if r[name] is not None]
                afinite = sum(seqrows[i]["_analysis"] for i in finite)
                state = (
                    "ready"
                    if afinite
                    else "insufficient_history"
                    if len(seqrows) < needed
                    else "undefined_arithmetic"
                )
                capabilities.append(
                    {
                        "sequence": {"definition": definition, "timeframe": timeframe},
                        "feature": name,
                        "materialized": True,
                        "analysis_rows": analysis,
                        "state": state,
                        "reason": "native_cf_composition.v1/" + name + "/" + state,
                        "required_observations": needed,
                        "finite_count": len(finite),
                        "analysis_finite_count": afinite,
                        "first_finite_bar_index": finite[0] if finite else None,
                        "first_finite_source_begin": seqrows[finite[0]]["source_begin"]
                        if finite
                        else None,
                    }
                )
            qcs.append(
                {
                    "cf_definition_json": key,
                    "timeframe": timeframe,
                    "cf_context_id": context_id,
                    "parent_inspected_rows": len(all_rows),
                    "eligible_component_rows": len(component_rows),
                    "selected_rows": len(seqrows),
                    "analysis_rows": analysis,
                    "observed_events": len(seqevents),
                    "ladder_rows": len(seqevents),
                    "missing_reference_count": 0,
                    "status": "PASS",
                }
            )
            bars.extend(seqrows)
            events.extend(seqevents)
    selection = {
        name: sum(q[name] for q in qcs)
        for name in [
            "parent_inspected_rows",
            "eligible_component_rows",
            "selected_rows",
            "analysis_rows",
            "observed_events",
            "ladder_rows",
        ]
    }
    selection["qc_rows"] = len(qcs)
    require(
        selection["selected_rows"] <= LIMITS["rows"] and len(sequences) <= LIMITS["sequences"],
        "CF selected rows/sequence budget exceeded",
    )
    tables = {
        role: pa.Table.from_pylist(rows, schema=physical_schema(layouts[role])).sort_by(
            [(k, "ascending") for k in layouts[role]["grain"]]
        )
        for role, rows in [("bars", bars), ("events", events), ("ladder", events), ("qc", qcs)]
    }
    return tables, {
        "selection": selection,
        "sequences": sequences,
        "used_references": used,
        "capabilities": capabilities,
    }


def build_cf_frames(root, intent, source_reader, parent, spark):
    """Native Spark composition; the existing writer persists this frame set."""

    policy = cf_policy("CF")
    layouts = policy["layouts"]
    validate_cf_facts(root, intent, parent)
    expected, counts = expected_cf_base(parent, intent, layouts, "")
    source = _source(intent["parent_completion"], parent)
    context = {
        "schema_version": 1,
        "contract_version": "native_cf_context.v1",
        "input_mode": "native_cf_observation.v1",
        "layer": "CF",
        "source": source,
        "definitions": intent["definitions"],
        "calendar_roll_schedule": intent["calendar_roll_schedule"],
        "source_fact_pins": intent["source_fact_pins"],
        "policy": intent["policy"],
        "policy_sha256": intent["policy_sha256"],
        "policy_payload": policy,
        "layouts": layouts,
        "numerical_identity": intent["numerical_identity"],
        "runtime_code_sha256": intent["code_sha256"],
        "quality": {
            "version": "native_cf_comparison.v1",
            "parent_fields": "exact",
            "masks": "exact",
            "discrete": "exact",
            "nonfinite": "forbidden",
            "new_feature_values": {"rtol": 1e-12, "atol": 1e-10},
        },
        **counts,
        "limits": dict(LIMITS),
        "limitations": dict(LIMITATIONS),
    }
    context["cf_context_id"] = "CFCTX:" + sha(context)
    validate_identity_contract("native_cf_context.v1.json", context, source["namespace"])
    frame, _ = source_reader.read_spark(spark)

    def guarded_metrics(pdf):
        from .native_feature_materialization import _check_worker_engines

        _check_worker_engines(intent["numerical_identity"]["engines"])
        return _cf_input_metrics(pdf)

    frames = _cf_native_frames(frame, intent, context, expected, spark, guarded_metrics)
    require(
        frames["bars"].count() == context["selection"]["selected_rows"],
        "CF Spark selection cardinality mismatch",
    )
    return frames, layouts, context, parent["manifest"]


def _cf_native_frames(frame, intent, context, expected, spark, guarded_metrics):
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from trading_advisor_3000.spark_jobs.continuous_front_job import (
        calendar_selected_frame,
        roll_gap_expression,
    )

    layouts = context["layouts"]
    eligible = None
    for definition in intent["definitions"]:
        ids = [c["instrument"]["instrument_id"] for c in definition["components"]]
        part = frame.where(F.col("instrument_id").isin(ids)).withColumn(
            "cf_definition_json", F.lit(definition_json(definition))
        )
        eligible = part if eligible is None else eligible.unionByName(part)
    selected = calendar_selected_frame(
        eligible,
        intent["calendar_roll_schedule"],
        group_column="cf_definition_json",
        component_column="instrument_id",
        source_date=F.substring("source_begin", 1, 10),
    ).drop("true_range", "log_ret_1")
    order = Window.partitionBy("cf_definition_json", "timeframe").orderBy("source_begin")
    prior = F.lag(
        F.struct(
            *[
                F.col(n)
                for n in [
                    "instrument_id",
                    "source_begin",
                    "source_end",
                    "body_sha256",
                    "receipt_sha256",
                    "row_ordinal",
                    "close",
                ]
            ]
        )
    ).over(order)
    selected = selected.withColumn("_previous", prior).withColumn(
        "is_first_bar_after_roll",
        F.col("_previous").isNotNull()
        & (F.col("_previous.instrument_id") != F.col("instrument_id")),
    )
    selected = selected.withColumn(
        "roll_epoch",
        F.sum(F.col("is_first_bar_after_roll").cast("long")).over(
            order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    ).withColumn(
        "_gap",
        F.when(
            F.col("is_first_bar_after_roll"), roll_gap_expression("close", "_previous.close")
        ).otherwise(F.lit(0.0)),
    )
    refend = F.when(
        F.col("is_first_bar_after_roll"),
        F.greatest(F.col("source_end"), F.col("_previous.source_end")),
    )
    selected = selected.withColumn(
        "reference_calculation_embargo_source_label",
        F.greatest(
            F.col("source_end"),
            F.max(refend).over(order.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        ),
    )
    metric_schema = T.StructType(
        [selected.schema[n] for n in ["cf_definition_json", "timeframe", "source_begin"]]
        + [T.StructField(n, T.DoubleType(), True) for n in ["true_range", "log_ret_1"]]
    )
    metrics = selected.groupBy("cf_definition_json", "timeframe").applyInPandas(
        guarded_metrics,
        schema=metric_schema,
    )
    bars = (
        selected.join(metrics, ["cf_definition_json", "timeframe", "source_begin"], "inner")
        .withColumn("source_instrument_id", F.col("instrument_id"))
        .withColumn("active_instrument_id", F.col("instrument_id"))
        .withColumn("instrument_id", F.lit(None).cast("string"))
        .withColumn("cf_recipe_version", F.lit(context["policy"]["profile_version"]))
    )
    switch = F.lit(None).cast("string")
    for interval in calendar_active_intervals(intent["calendar_roll_schedule"]):
        switch = F.when(
            (F.col("cf_definition_json") == interval["group_key"])
            & (F.col("instrument_id") == interval["component_key"]),
            F.lit(interval["active_from"]),
        ).otherwise(switch)
    event = selected.where("is_first_bar_after_roll").withColumn("switch_source_date", switch)
    expressions = [
        F.col("cf_definition_json"),
        F.col("timeframe"),
        F.col("roll_epoch").alias("roll_ordinal"),
        F.col("planned_roll_ordinal"),
        F.col("switch_source_date"),
        F.col("source_begin").alias("observed_switch_source_begin"),
        F.col("_previous.instrument_id").alias("old_instrument_id"),
        F.col("instrument_id").alias("new_instrument_id"),
    ]
    for side, prefix in [("old", "_previous."), ("new", "")]:
        expressions.extend(
            F.col(prefix + n).alias(side + "_" + n)
            for n in ["source_begin", "source_end", "body_sha256", "receipt_sha256", "row_ordinal"]
        )
        expressions.append(F.col(prefix + "close").alias(side + "_reference_close"))
    expressions.extend(
        [F.col("_gap").alias("additive_gap"), F.col("reference_calculation_embargo_source_label")]
    )
    event = event.select(*expressions)
    qc_rows = expected["qc"].to_pylist()
    qc = spark.createDataFrame(
        qc_rows,
        schema=T.StructType(
            [
                T.StructField(
                    f["name"],
                    T.LongType() if f["sql_type"] in {"bigint", "long"} else T.StringType(),
                    f["nullable"],
                )
                for f in layouts["qc"]["fields"]
            ]
        ),
    )
    frames = {"bars": bars, "events": event, "ladder": event, "qc": qc}
    for role in frames:
        frames[role] = (
            frames[role]
            .withColumn("cf_context_id", F.lit(context["cf_context_id"]))
            .select(
                *[
                    F.col(f["name"]).cast(f["sql_type"]).alias(f["name"])
                    for f in layouts[role]["fields"]
                ]
            )
        )
    return frames


def validate_cf_graph(root, *, snapshot_pin, context_pin, intent_pin, input_sha256, visited=()):
    context = _object(root, context_pin, "CF context")
    require(
        context.get("contract_version") == "native_cf_context.v1"
        and context.get("layer") in NEW_ROLES,
        "invalid CF context family/layer",
    )
    source = context["source"]
    namespace = source["namespace"]
    validate_identity_contract("native_cf_context.v1.json", context, namespace)
    # Versioned physical family shape; recorded numerical descriptors stay historical.
    require(context["layouts"] == cf_layouts("CF"), "CF fixed family role layout mismatch")
    intent = _object(root, intent_pin, "CF intent")
    validate_identity_contract("native_cf_intent.v1.json", intent, namespace)
    require(sha(intent) == input_sha256, "CF canonical input hash mismatch")
    for key in [
        "layer",
        "definitions",
        "calendar_roll_schedule",
        "source_fact_pins",
        "policy",
        "policy_sha256",
        "numerical_identity",
    ]:
        require(context[key] == intent[key], "CF intent/context mismatch: " + key)
    require(
        context["runtime_code_sha256"] == intent["code_sha256"]
        and source["parent_completion"] == intent["parent_completion"],
        "CF runtime/parent binding mismatch",
    )
    require(
        context["cf_context_id"]
        == "CFCTX:" + sha({k: v for k, v in context.items() if k != "cf_context_id"}),
        "CF context digest mismatch",
    )
    require(
        sha(context["policy_payload"]) == context["policy_sha256"], "CF stored policy hash mismatch"
    )
    require(
        context["policy"] == {k: context["policy_payload"][k] for k in context["policy"]},
        "CF stored policy reference mismatch",
    )
    numerical = context["numerical_identity"]
    require(
        numerical["digest"] == sha({k: v for k, v in numerical.items() if k != "digest"}),
        "CF numerical digest mismatch",
    )
    runtime = context["policy_payload"]["runtime_profile"]
    require(
        numerical["engines"]["pandas_ta_classic"] == "0.4.47"
        and numerical["engines"]["talib_absent"]
        and all(numerical["engines"][k] == runtime[k] for k in ["numba", "scipy"]),
        "unadmitted CF recorded backend",
    )
    _, parent = _parent(root, intent["parent_completion"], context["layer"], visited)
    require(
        source == _source(intent["parent_completion"], parent),
        "CF source metadata differs from exact parent",
    )
    require(context["layer"] == "CF", "CF sidecar reader not implemented")
    validate_cf_facts(root, intent, parent)
    pinned_bytes(root, snapshot_pin)
    manifest = load_snapshot(child(root, snapshot_pin["path"]))
    require(
        manifest["dataset_version"] == context["policy_payload"]["version"]
        and manifest["code_version"] == intent["code_sha256"]
        and manifest["parents"] == [source["parent_snapshot"]["snapshot_id"]],
        "CF manifest recipe/parent mismatch",
    )
    require(
        manifest["parameters"]
        == {
            "input_sha256": input_sha256,
            "context": context_pin,
            "intent": intent_pin,
            "source_root": "/research-trial",
            "parent_completion": intent["parent_completion"],
        },
        "CF manifest crosslinks mismatch",
    )
    require(
        manifest["quality_checks"]
        == {k: "PASS" for k in ["parent", "schema", "grain", "capabilities"]},
        "CF manifest quality not accepted",
    )
    require(manifest["run_id"] == Path(snapshot_pin["path"]).parts[-3], "CF attempt mismatch")
    old = parent["manifest"]["tables"]
    require(
        set(manifest["tables"])
        == set(manifest["required_tables"])
        == set(old) | {TABLES[r] for r in NEW_ROLES[context["layer"]]},
        "incomplete CF role graph",
    )
    require(
        all(manifest["tables"][n] == r for n, r in old.items()),
        "CF changed inherited table reference",
    )
    tables = dict(parent["tables"])
    for role in NEW_ROLES[context["layer"]]:
        reference = manifest["tables"][TABLES[role]]
        layout = context["layouts"][role]
        require(
            Path(reference["table_path"]).resolve().is_relative_to(root.resolve()),
            "CF table escapes root",
        )
        require(
            0 <= reference["row_count"] <= LIMITS["rows"]
            and 0 <= reference["file_count"] <= LIMITS["files"],
            "CF physical count budget exceeded",
        )
        delta = open_pinned_table(reference)
        require(
            sum(delta.get_add_actions().column("size_bytes").to_pylist())
            <= LIMITS["physical_bytes"],
            "CF physical byte budget exceeded",
        )
        require(
            not delta.metadata().partition_columns
            and delta.to_pyarrow_dataset().schema.equals(
                physical_schema(layout), check_metadata=True
            ),
            "CF physical layout mismatch",
        )
        data = (
            delta.to_pyarrow_table()
            .combine_chunks()
            .sort_by([(k, "ascending") for k in layout["grain"]])
        )
        require(
            data.num_rows == reference["row_count"]
            and data.select(layout["grain"]).group_by(layout["grain"]).aggregate([]).num_rows
            == data.num_rows,
            "CF physical rows/duplicate grain mismatch",
        )
        require(
            all(f["nullable"] or data[f["name"]].null_count == 0 for f in layout["fields"]),
            "NULL required CF field",
        )
        tables[TABLES[role]] = data
    expected, counts = expected_cf_base(
        parent, intent, context["layouts"], context["cf_context_id"]
    )
    for role, data in expected.items():
        _arrow_equal_values(tables[TABLES[role]], data)
    require(
        all(context[k] == v for k, v in counts.items()),
        "CF physical selection/capability/reference matrix mismatch",
    )
    return {"context": context, "intent": intent, "manifest": manifest, "tables": tables}
