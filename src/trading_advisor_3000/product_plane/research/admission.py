"""Versioned execution admission for the existing Research materialization route."""

from __future__ import annotations

import functools
import hashlib
import inspect
import json
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

from trading_advisor_3000.product_plane.common.data_snapshots import (
    assert_tables_unchanged,
    capture_table,
    current_snapshot,
    load_snapshot,
)

_ACTIVE: ContextVar[dict | None] = ContextVar("research_refresh_admission", default=None)
_OWNER: ContextVar[bool] = ContextVar("research_refresh_owner", default=False)
_EXECUTION_KEYS = {
    "refresh_admission",
    "operation",
    "changed_windows",
    "refresh_scope_version",
    "force_full_refresh",
    "hash_compatibility",
    "data_snapshot_manifest_path",
}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _domain(config, *, inputs=None):
    from .data_snapshots import _parameters

    result = {
        key: value for key, value in _parameters(config).items() if key not in _EXECUTION_KEYS
    }
    # These are the existing owning adapters' defaults, including Dagster's
    # absent-to-empty normalization; they do not authorize another input root.
    if not result.get("data_snapshot_raw_output_dir"):
        result.pop("data_snapshot_raw_output_dir", None)
    if not result.get("canonical_contract_economics_path"):
        # The existing adapter may resolve the shared canonical economics root.
        # Bind that exact admitted input instead of inventing a baseline-local path.
        pinned = (inputs or {}).get("canonical_contract_economics", {})
        result["canonical_contract_economics_path"] = pinned.get("table_path") or (
            (Path(config["canonical_output_dir"]) / "canonical_contract_economics.delta")
            .resolve()
            .as_posix()
        )
    return result


def _parent(root, manifest=None):
    manifest = manifest or current_snapshot(root)
    if manifest is None:
        return None
    return {
        "registry_root": Path(root).resolve().as_posix(),
        "manifest_path": (Path(root) / "manifests" / f"{manifest['snapshot_id']}.json")
        .resolve()
        .as_posix(),
        "snapshot_id": manifest["snapshot_id"],
        "manifest_sha256": manifest["manifest_sha256"],
    }


def _seal(payload):
    payload = json.loads(_json(payload))
    return {**payload, "admission_id": _digest(payload)}


def issue_admission(config, *, operation, completed_outputs=()):
    """Issue daily admission from authoritative manifests; rebuild is an explicit selection."""
    from .data_snapshots import _input_tables, _registry
    from .refresh_scope import resolve_refresh_scope

    if operation not in {"daily", "rebuild"}:
        raise ValueError("refresh admission requires an explicit daily or rebuild operation")
    config = dict(config)
    data_root = Path(config["canonical_output_dir"]) / ".data_snapshots"
    path = str(config.get("data_snapshot_manifest_path") or "")
    data = load_snapshot(Path(path)) if path else current_snapshot(data_root)
    research_root = _registry(config)
    research = current_snapshot(research_root)
    if operation == "daily":
        if not path:
            result = {"outcome": "BLOCKED", "reason": "missing_data_manifest", "windows": []}
        elif config.get("force_full_refresh"):
            result = {"outcome": "BLOCKED", "reason": "daily_force_full_refresh", "windows": []}
        else:
            result = resolve_refresh_scope(
                data_manifest_path=Path(path),
                materialized_output_dir=Path(config["materialized_output_dir"]),
                dataset_version=str(config["dataset_version"]),
                timeframes=tuple(config["timeframes"]),
                configuration=config,
                _completed_outputs=completed_outputs,
            )
        inputs = dict(data["tables"]) if data else {}
        volume_path = str(config.get("volume_profile_raw_1m_table_path") or "")
        if result["outcome"] == "SCOPED" and volume_path and Path(volume_path).stem not in inputs:
            inputs[Path(volume_path).stem] = capture_table(Path(volume_path))
    else:
        result = {"outcome": "SCOPED", "windows": []}
        inputs = _input_tables(config, data)
    original = list(
        (data or {}).get("parameters", {}).get("downstream_scope", {}).get("windows", [])
    )
    return _seal(
        {
            "schema_version": 1,
            "operation": operation,
            "outcome": result["outcome"],
            "data_parent": _parent(data_root, data),
            "research_parent": _parent(research_root, research),
            "inputs": inputs,
            "domain": _domain(config, inputs=inputs),
            "original_windows": result.get("original_windows", original),
            "effective_windows": list(result["windows"]),
            "reason": result.get("reason", ""),
            "hash_compatibility": result.get("hash_compatibility", {}),
        }
    )


def configure_admission(config, *, operation=None, refresh_admission=None):
    config = dict(config)
    if refresh_admission is None:
        if operation != "rebuild":
            raise ValueError(
                "refresh admission required; explicitly select rebuild for a new full calculation"
            )
        if config.get("changed_windows"):
            raise ValueError("explicit rebuild declares its domain separately from daily windows")
        config.update(operation="rebuild", refresh_scope_version=1, force_full_refresh=True)
        refresh_admission = issue_admission(config, operation="rebuild")
    else:
        if operation is not None and operation != refresh_admission.get("operation"):
            raise ValueError("refresh admission operation changed")
        config["operation"] = refresh_admission["operation"]
    config["refresh_admission"] = dict(refresh_admission)
    validate_admission(refresh_admission, config=config, resolve=False)
    return config


def validate_admission(
    value, *, config=None, completed_outputs=(), resolve=True, defer_research_current=False
):
    """Reject missing/legacy/tampered evidence, drift, and implicit broadening."""
    if not isinstance(value, Mapping):
        raise ValueError("refresh admission required before materialization")
    value = json.loads(_json(dict(value)))
    payload = {key: item for key, item in value.items() if key != "admission_id"}
    from trading_advisor_3000.product_plane.contracts.schema_validation import (
        SchemaValidationError,
        load_schema,
        validate_schema,
    )

    schema_path = Path(__file__).parents[1] / "contracts/schemas/research_refresh_admission.v1.json"
    try:
        validate_schema(load_schema(schema_path), value)
    except SchemaValidationError as exc:
        raise ValueError(f"refresh admission schema invalid: {exc}") from exc
    if value["admission_id"] != _digest(payload):
        raise ValueError("refresh admission content changed")
    if value["outcome"] == "BLOCKED":
        raise ValueError(f"refresh admission BLOCKED: {value['reason']}")
    if value["outcome"] == "NOOP":
        raise ValueError("refresh admission NOOP cannot compute or publish")
    if not isinstance(value["inputs"], dict) or not isinstance(value["domain"], dict):
        raise ValueError("refresh admission requires native references and declared domain")
    for role, parent in (("data", value["data_parent"]), ("research", value["research_parent"])):
        if parent is None:
            continue
        manifest = load_snapshot(Path(parent["manifest_path"]))
        current = current_snapshot(Path(parent["registry_root"]))
        if not current or any(
            manifest[key] != parent[key]
            or (not (role == "research" and defer_research_current) and current[key] != parent[key])
            for key in ("snapshot_id", "manifest_sha256")
        ):
            raise ValueError("refresh admission parent changed")
    assert_tables_unchanged(value["inputs"])
    if config is not None:
        if _domain(config, inputs=value["inputs"]) != value["domain"]:
            raise ValueError("refresh admission configuration changed")
        if config.get("operation") != value["operation"]:
            raise ValueError("refresh admission operation changed")
    if value["operation"] == "daily":
        if (
            not value["data_parent"]
            or not value["research_parent"]
            or not value["effective_windows"]
        ):
            raise ValueError("daily refresh admission requires parents and nonempty scope")
        from .refresh_scope import _stamp

        for window in value["effective_windows"]:
            if (
                not window.get("instrument_id")
                or not window.get("timeframe")
                or _stamp(window["start_ts"]) > _stamp(window["end_ts"])
            ):
                raise ValueError("daily refresh admission window invalid")
        if config is not None:
            if (
                type(config.get("refresh_scope_version")) is not int
                or config.get("refresh_scope_version") != 1
                or config.get("hash_compatibility", {}) != value["hash_compatibility"]
            ):
                raise ValueError("refresh admission execution version/compatibility changed")
            if not config.get("data_snapshot_enabled") or config.get("force_full_refresh"):
                raise ValueError(
                    "daily refresh admission requires snapshot owner and forbids full refresh"
                )
            if list(config.get("changed_windows") or []) != value["effective_windows"]:
                raise ValueError("refresh admission windows changed")
            if config.get("data_snapshot_manifest_path") != value["data_parent"]["manifest_path"]:
                raise ValueError("refresh admission Data manifest changed")
            if resolve:
                fresh = issue_admission(
                    config, operation="daily", completed_outputs=completed_outputs
                )
                if fresh != value:
                    raise ValueError(
                        "refresh admission proof changed: "
                        f"{fresh.get('reason') or fresh['outcome']}"
                    )
    elif value["effective_windows"]:
        raise ValueError("explicit rebuild uses its declared domain, not daily windows")
    return value


@contextmanager
def _owner_scope(value):
    owner = _OWNER.set(True)
    token = _ACTIVE.set(value)
    try:
        yield value
    finally:
        _ACTIVE.reset(token)
        _OWNER.reset(owner)


def active_admission():
    return _ACTIVE.get() if _OWNER.get() else None


def admitted_pairs():
    value = active_admission()
    if not value or value["operation"] != "daily":
        return None
    return frozenset(
        (str(w["instrument_id"]), str(w["timeframe"])) for w in value["effective_windows"]
    )


def pair_admitted(instrument, timeframe):
    pairs = admitted_pairs()
    return pairs is None or (str(instrument), str(timeframe)) in pairs


def spark_pair_condition():
    pairs = admitted_pairs()
    from pyspark.sql import functions as F

    if pairs is None:
        return F.lit(True)

    condition = F.lit(False)
    for instrument, timeframe in sorted(pairs):
        condition = condition | (
            (F.col("instrument_id") == instrument) & (F.col("timeframe") == timeframe)
        )
    return condition


def filter_spark_pairs(frame):
    return frame if admitted_pairs() is None else frame.filter(spark_pair_condition())


def admitted_asset(function):
    @functools.wraps(function)
    def wrapped(context, *args, **kwargs):
        from .data_snapshots import _STAGE_OUTPUTS, _configuration, _pending_path

        config = _configuration(context)
        completed = []
        if (
            not config.get("refresh_admission")
            and not config.get("data_snapshot_enabled")
            and config.get("reuse_existing_materialization")
            and function.__name__ == "research_datasets"
        ):
            # Existing strategy/backtest projection reads; this path cannot publish.
            return function(context, *args, **kwargs)
        if not config.get("data_snapshot_enabled"):
            value = validate_admission(config.get("refresh_admission"), config=config)
            with _owner_scope(value):
                return function(context, *args, **kwargs)
        path = _pending_path(context, config)
        op_context = getattr(context, "op_execution_context", context)
        if not config.get("refresh_admission"):
            raise ValueError("refresh admission required before asset execution")
        try:
            dagster_run = getattr(op_context, "dagster_run", None)
            parent_run = getattr(dagster_run, "parent_run_id", None)
        except Exception as exc:
            raise ValueError("snapshot-enabled assets require a bound Dagster run") from exc
        if not path.exists() and parent_run:
            path = _pending_path(context, config, parent_run)
        if path.exists():
            pending = json.loads(path.read_text(encoding="utf-8"))
            if pending.get("parameters", {}).get("refresh_admission") != config.get(
                "refresh_admission"
            ):
                raise ValueError("refresh admission changed before resume")
            completed = [
                name
                for stage, status in pending.get("quality_checks", {}).items()
                if status == "PASS"
                for name in _STAGE_OUTPUTS.get(stage, ())
            ]
        instance = getattr(op_context, "instance", None)
        if path.exists() and dagster_run is not None and instance is not None:
            attempt = parent_run or str(context.run_id)
            failed = {
                step.step_key
                for step in instance.get_run_step_stats(attempt)
                if step.status.value == "FAILURE"
            }
            if function.__name__ in failed:
                completed.extend(_STAGE_OUTPUTS.get(function.__name__, ()))
        if not path.exists() and dagster_run is not None and instance is not None:
            tags = getattr(dagster_run, "tags", {}) or {}
            source_id = tags.get("ta3000/upstream_run_id")
            source_run = instance.get_run_by_id(source_id) if source_id else None
            if (
                source_run is not None
                and source_run.job_name == "moex_cf_catch_up_job"
                and source_run.status.value == "SUCCESS"
            ):
                source_path = _pending_path(context, config, source_id)
                if not source_path.exists():
                    raise ValueError("daily refresh upstream CF checkpoint unavailable")
                source = json.loads(source_path.read_text(encoding="utf-8"))
                source_config = (
                    source_run.run_config.get("ops", {})
                    .get("continuous_front_bars", {})
                    .get("config", {})
                )
                success = any(
                    step.step_key == "continuous_front_bars" and step.status.value == "SUCCESS"
                    for step in instance.get_run_step_stats(source_id)
                )
                if (
                    not success
                    or source_config.get("refresh_admission") != config.get("refresh_admission")
                    or source.get("parameters", {}).get("refresh_admission")
                    != config.get("refresh_admission")
                    or source.get("quality_owners", {}).get("continuous_front_bars") != source_id
                    or source.get("quality_checks", {}).get("continuous_front_bars") != "PASS"
                ):
                    raise ValueError("daily refresh upstream CF admission/owner changed")
                assert_tables_unchanged(source["tables"])
                completed.extend(_STAGE_OUTPUTS.get("continuous_front_bars", ()))
        value = validate_admission(
            config.get("refresh_admission"), config=config, completed_outputs=completed
        )
        with _owner_scope(value):
            return function(context, *args, **kwargs)

    return wrapped


def _native_value(value):
    from dataclasses import asdict, is_dataclass
    from enum import Enum

    if isinstance(value, Path):
        return value.resolve().as_posix()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _native_value(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _native_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_native_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise ValueError(f"unbound native contract parameter: {type(value).__name__}")


_VOLATILE_NATIVE = {
    "run_id",
    "spark_master",
    "spark_session_factory",
    "created_at_utc",
    "calculation_app_id",
    "event_log_path",
    "bar_view_count",
    "instrument_tree_count",
    "output_paths",
}


def _native_parameters(options, excluded):
    return {
        key: _native_value(value)
        for key, value in options.items()
        if key not in excluded and key not in _VOLATILE_NATIVE
    }


def _native_domain_checks(function, options, value, *, sources, input_roots):
    domain = value["domain"]
    for key, _names in input_roots:
        expected = domain.get("input_roots", {}).get(key, domain["materialized_output_dir"])
        if Path(options[key]).resolve().as_posix() != expected:
            raise ValueError(f"refresh admission native input root changed: {key}")
    for key in sources:
        path = options.get(key)
        declared = domain.get("input_paths")
        if declared is not None and key in declared:
            expected = declared[key]
        elif key == "volume_profile_raw_1m_table_path":
            expected = domain.get("volume_profile_raw_1m_table_path")
        elif key == "canonical_contract_economics_path":
            expected = domain.get("canonical_contract_economics_path")
        elif path is not None:
            reference = value["inputs"].get(Path(path).stem)
            if reference is None:
                matches = [
                    ref
                    for ref in value["inputs"].values()
                    if Path(ref["table_path"]).stem == Path(path).stem
                ]
                if len(matches) == 1:
                    reference = matches[0]
            if reference:
                expected = reference["table_path"]
            else:
                expected = str(Path(domain["materialized_output_dir"]) / Path(path).name)
        else:
            expected = None
        if key in {"volume_profile_raw_1m_table_path", "canonical_contract_economics_path"}:
            pinned = expected and any(
                Path(ref["table_path"]).resolve() == Path(expected).resolve()
                for ref in value["inputs"].values()
            )
            exists = expected and (Path(expected) / "_delta_log").is_dir()
            if exists and not pinned:
                raise ValueError(f"refresh admission native input appeared after admission: {key}")
            # The existing rebuild adapter omits its unavailable optional VP
            # default. Never omit a pinned input or grant this daily exception.
            if (
                key == "volume_profile_raw_1m_table_path"
                and path is None
                and value["operation"] == "rebuild"
                and not pinned
                and not exists
            ):
                expected = None
        if (Path(path).resolve().as_posix() if path is not None else None) != (
            Path(expected).resolve().as_posix() if expected else None
        ):
            raise ValueError(f"refresh admission native input path changed: {key}")
    aliases = {
        "instrument_ids": "dataset_instrument_ids",
        "contract_ids": "dataset_contract_ids",
        "derived_set_version": "derived_indicator_set_version",
        "derived_profile_version": "derived_indicator_profile_version",
        "split_params": "validation_plan",
        "qc_mode": "continuous_front_indicator_qc_mode",
        "sidecar_materialization_mode": "continuous_front_indicator_sidecar_materialization_mode",
    }
    for key in (
        "timeframes",
        "instrument_ids",
        "dataset_instrument_ids",
        "contract_ids",
        "dataset_name",
        "universe_id",
        "start_ts",
        "end_ts",
        "warmup_bars",
        "split_method",
        "split_params",
        "indicator_set_version",
        "derived_set_version",
        "derived_indicator_set_version",
        "derived_profile_version",
        "qc_mode",
        "sidecar_materialization_mode",
        "volume_profile_tick_size_by_instrument",
    ):
        target = aliases.get(key, key)
        if key not in options or target not in domain:
            continue
        actual, expected = _native_value(options[key]), _native_value(domain[target])
        if key in {"timeframes", "instrument_ids", "dataset_instrument_ids", "contract_ids"}:
            actual, expected = sorted(actual or []), sorted(expected or [])
            if function.__name__ == "run_research_derived_source_frames_spark_job":
                windows = (
                    value["effective_windows"]
                    or domain.get("native_parameters", {}).get("refresh_windows", [])
                    or []
                )
                field = "timeframe" if key == "timeframes" else "instrument_id"
                projected = sorted({str(window[field]) for window in windows})
                if projected and actual == projected:
                    expected = projected
        if key == "volume_profile_tick_size_by_instrument":
            actual, expected = actual or {}, expected or {}
        if key in {"start_ts", "end_ts"}:
            actual, expected = actual or "", expected or ""
        if actual != expected:
            raise ValueError(f"refresh admission native parameter changed: {key}")
    if "manifest_seed" in options:
        seed = _native_value(options["manifest_seed"])
        for key in (
            "dataset_name",
            "universe_id",
            "timeframes",
            "base_timeframe",
            "start_ts",
            "end_ts",
            "series_mode",
            "split_method",
            "warmup_bars",
        ):
            # L0 resolves omitted date bounds from its pinned source.  The
            # owning native job's input/selectors were already checked above.
            if key in {"start_ts", "end_ts"} and not domain.get(key):
                continue
            expected = domain.get(key)
            if key == "series_mode":
                expected = (
                    "continuous_front"
                    if seed.get("contour_id") == "pit_active_front"
                    else "contract"
                )
            elif key == "split_method" and not domain.get("validation_plan"):
                expected = "full"
            if key in domain and seed.get(key) != expected:
                raise ValueError(f"refresh admission dataset manifest changed: {key}")
    if value["operation"] == "daily" and function.__name__ in {
        "materialize_cf_indicator_input_frame",
        "run_continuous_front_base_indicator_sidecar_job",
    }:
        raise ValueError("daily refresh requires the complete native sidecar owner")
    if value["operation"] == "daily" and (
        options.get("profile") is not None or options.get("volume_profile_source_rows") is not None
    ):
        raise ValueError("daily refresh admission forbids unbound profile/source overrides")
    # Direct rebuild receipts freeze all original native parameters. Config
    # receipts instead bind native projections below, for either operation.
    if domain.get("native_entry"):
        return
    profile_key = (
        "derived_indicator_profile_version"
        if "derived" in function.__name__
        else "indicator_profile_version"
    )
    if (
        "profile_version" in options
        and profile_key in domain
        and (options["profile_version"] or "core_v1") != domain.get(profile_key)
    ):
        raise ValueError("refresh admission native profile changed")
    if "policy" in options:
        from .datasets import ContinuousFrontPolicy

        expected = ContinuousFrontPolicy.from_config(domain.get("continuous_front_policy", {}))
        if (options["policy"] or ContinuousFrontPolicy()) != expected:
            raise ValueError("refresh admission native CF policy changed")
    if "execution_economics_required" in options and not options["execution_economics_required"]:
        raise ValueError("daily refresh admission requires execution economics")
    if "contour_id" in options:
        expected = (
            "pit_active_front"
            if domain.get("series_mode") == "continuous_front"
            else "native_tradable"
        )
        if options["contour_id"] != expected:
            raise ValueError("refresh admission native contour changed")
    if "rule_set_version" in options:
        from .continuous_front_indicators.rules import DEFAULT_RULE_SET_VERSION

        if options["rule_set_version"] != DEFAULT_RULE_SET_VERSION:
            raise ValueError("refresh admission native roll rules changed")
    if function.__name__ == "run_research_derived_source_frames_spark_job":
        from .derived_indicators.materialize import _source_indicator_columns_for_profile
        from .derived_indicators.registry import build_derived_indicator_profile_registry

        profile = build_derived_indicator_profile_registry().get(
            domain["derived_indicator_profile_version"]
        )
        if tuple(options["source_indicator_columns"]) != _source_indicator_columns_for_profile(
            profile
        ):
            raise ValueError("refresh admission native source projection changed")
    if function.__name__ == "run_continuous_front_indicator_sidecar_spark_job":
        from .continuous_front_indicators.pandas_job import (
            _load_dataset_manifest,
            _max_strict_roll_window_bars,
            _policy_versions,
        )
        from .continuous_front_indicators.rules import (
            adapter_bundle_hash,
            default_indicator_roll_rules,
        )
        from .derived_indicators.registry import current_derived_indicator_profile
        from .indicators.registry import default_indicator_profile

        base, derived = default_indicator_profile(), current_derived_indicator_profile()
        rules = default_indicator_roll_rules(
            indicator_profile=base,
            derived_profile=derived,
            rule_set_version=options["rule_set_version"],
        )
        manifest = _load_dataset_manifest(
            materialized_output_dir=Path(domain["materialized_output_dir"]),
            dataset_version=domain["dataset_version"],
            contour_id="pit_active_front",
        )
        roll, adjustment = _policy_versions(manifest)
        expected = {
            "source_canonical_version": str(manifest.get("source_table") or ""),
            "roll_policy_version": roll,
            "adjustment_policy_version": adjustment,
            "adapter_hash": adapter_bundle_hash(rules),
            "include_derived": True,
            "indicator_value_columns": base.expected_output_columns(),
            "derived_value_columns": derived.output_columns,
            "rule_count": len(rules),
            "max_base_cross_contract_window_bars": _max_strict_roll_window_bars(
                rules, output_family="base"
            ),
            "max_derived_cross_contract_window_bars": _max_strict_roll_window_bars(
                rules, output_family="derived"
            ),
        }
        if any(
            _native_value(options[key]) != _native_value(item) for key, item in expected.items()
        ):
            raise ValueError("refresh admission native sidecar contract changed")


def admitted_compute(*, output, sources=(), input_roots=(), windows=None):
    """Direct calls select rebuild; daily execution belongs to the validated Dagster owner."""

    def decorate(function):
        signature = inspect.signature(function)

        @functools.wraps(function)
        def wrapped(*args, **kwargs):
            explicit = kwargs.pop("refresh_admission", None)
            operation = kwargs.pop("operation", None)
            inherited = active_admission()
            value = explicit if explicit is not None else inherited
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            options = bound.arguments
            out = Path(options[output] or options.get("materialized_output_dir"))
            dataset = str(
                options.get("dataset_version") or options["manifest_seed"].dataset_version
            )
            excluded = {output, *sources, *(key for key, _ in input_roots)}
            if value is None:
                if operation != "rebuild":
                    raise ValueError(
                        "refresh admission required; select explicit rebuild separately"
                    )
                paths = [Path(options[key]) for key in sources if options.get(key) is not None]
                for key, names in input_roots:
                    paths.extend(Path(options[key]) / f"{name}.delta" for name in names)
                inputs = {
                    path.resolve().as_posix(): capture_table(path)
                    for path in paths
                    if (path / "_delta_log").is_dir()
                }
                native = _native_parameters(options, excluded)
                value = _seal(
                    {
                        "schema_version": 1,
                        "operation": "rebuild",
                        "outcome": "SCOPED",
                        "data_parent": None,
                        "research_parent": None,
                        "inputs": inputs,
                        "domain": {
                            "materialized_output_dir": out.resolve().as_posix(),
                            "dataset_version": dataset,
                            "timeframes": list(
                                options.get(
                                    "timeframes",
                                    getattr(options.get("manifest_seed"), "timeframes", ()),
                                )
                            ),
                            "dataset_instrument_ids": list(
                                options.get(
                                    "dataset_instrument_ids", options.get("instrument_ids", ())
                                )
                            ),
                            "input_paths": {
                                key: Path(options[key]).resolve().as_posix()
                                for key in sources
                                if options.get(key) is not None
                            },
                            "input_roots": {
                                key: Path(options[key]).resolve().as_posix()
                                for key, _ in input_roots
                            },
                            "native_entry": function.__name__,
                            "native_parameters": native,
                        },
                        "original_windows": [],
                        "effective_windows": [],
                        "reason": "",
                        "hash_compatibility": {},
                    }
                )
            if inherited is not None and value != inherited:
                raise ValueError("refresh admission changed across native handoff")
            value = validate_admission(value, resolve=False)
            if operation is not None and operation != value["operation"]:
                raise ValueError("refresh admission operation changed")
            domain = value["domain"]
            if (
                domain["materialized_output_dir"] != out.resolve().as_posix()
                or domain["dataset_version"] != dataset
            ):
                raise ValueError("refresh admission output/dataset changed")
            if value["operation"] == "daily" and inherited is None:
                raise ValueError(
                    "daily refresh requires the validated Dagster snapshot owner; "
                    "direct native daily execution is unavailable"
                )
            if domain.get("native_entry") == function.__name__ and _native_parameters(
                options, excluded
            ) != domain.get("native_parameters"):
                raise ValueError("refresh admission declared native parameters changed")
            _native_domain_checks(
                function, options, value, sources=sources, input_roots=input_roots
            )
            if value["operation"] == "daily":
                if options.get("force_recompute"):
                    raise ValueError("daily refresh admission forbids force_recompute")
                if (
                    windows is not None
                    and list(options.get(windows) or []) != value["effective_windows"]
                ):
                    raise ValueError("refresh admission native windows changed or missing")
            with _owner_scope(value):
                result = function(*args, **kwargs)
            validate_admission(value, resolve=False)
            return result

        return wrapped

    return decorate
