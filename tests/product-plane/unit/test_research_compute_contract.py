"""Persisted numerical compatibility, independent of release packaging."""

import ast
import copy
import shutil
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from deltalake import write_deltalake
from test_research_hash_compatibility import accepted_chain as accepted_chain

from trading_advisor_3000.product_plane.research import compute_contract as c
from trading_advisor_3000.product_plane.research import semantic_fingerprint as f


def _module(root, text):
    root.mkdir(exist_ok=True)
    (root / "fixture.py").write_text(text, encoding="utf-8")
    return lambda: f.SymbolGraph(root).fingerprint(("trading_advisor_3000.fixture.kernel",))


def test_symbol_closure_tracks_helpers_defaults_models_and_constants(tmp_path):
    text = """from dataclasses import dataclass
K = 2
@dataclass
class Spec:
    value: int = 3
    def apply(self, x):
        return x * self.value
def helper(x, bias=1):
    return x * K + bias
def kernel(spec: "Spec", x):
    return helper(spec.apply(x))
"""
    before = _module(tmp_path, text)()
    for old, new in [
        ("K = 2", "K = 4"),
        ("bias=1", "bias=2"),
        ("value: int = 3", "value: int = 4"),
        ("x * self.value", "x + self.value"),
    ]:
        assert _module(tmp_path, text.replace(old, new))() != before
    reordered = text.replace("K = 2\n", "") + "\nK = 2\n"
    assert _module(tmp_path, "# comment\n" + reordered)() == before


@pytest.mark.parametrize(
    "body",
    [
        "return missing_formula(x)",
        'return globals()["helper"](x)',
        "return callback(x)",
        (
            "import importlib\n    return "
            'importlib.import_module("trading_advisor_3000.other").formula(x)'
        ),
    ],
)
def test_unknown_dynamic_or_callback_edges_fail_closed(tmp_path, body):
    probe = _module(tmp_path, "def kernel(x, callback=None):\n    " + body + "\n")
    with pytest.raises(f.ContractError):
        probe()


@pytest.mark.parametrize(
    "prefix",
    [
        "K=[1]\nK.append(2)\n",
        "K=1\nK=2\n",
        "K=1\nif True:\n    K=2\n",
        "from math import pi as K\nK=2\n",
    ],
)
def test_ambiguous_and_mutated_module_bindings_fail_closed(tmp_path, prefix):
    with pytest.raises(f.ContractError):
        _module(tmp_path, prefix + "def kernel():\n    return K\n")()


def test_local_project_import_and_reexport_resolve_actual_helper(tmp_path):
    (tmp_path / "other.py").write_text("from .third import formula\n")
    path = tmp_path / "third.py"
    path.write_text("def formula(x):\n    return x + 1\n")
    probe = _module(
        tmp_path,
        "def kernel(x):\n    import trading_advisor_3000.other as p\n    return p.formula(x)\n",
    )
    before = probe()
    path.write_text("def formula(x):\n    return x + 2\n")
    assert probe() != before


@pytest.mark.parametrize(
    "statement",
    [
        "diagnostics['x'] = result['close'] = diagnostics.get('x', 0) + 1",
        "diagnostics[mutate_numeric(frame)] = 1",
        "result['close'] = diagnostics.get('x', 0)",
        "if diagnostics.get('x'): return frame * 2",
    ],
)
def test_observation_exclusion_cannot_erase_numeric_effects(statement):
    node = ast.parse(
        "def _compute_cached_profile_frame(frame, diagnostics):\n    result=frame.copy()\n    "
        + statement
        + "\n    return result\n"
    ).body[0]
    with pytest.raises(f.ContractError):
        f._observation_only(node)


@pytest.fixture
def package(tmp_path):
    target = tmp_path / "package"
    shutil.copytree(
        Path(c.__file__).resolve().parents[2], target, ignore=shutil.ignore_patterns("__pycache__")
    )
    return target


def test_two_future_releases_keep_numerical_identity(package, published_contract, monkeypatch):
    before, accepted, config, pending, instance = published_contract
    from trading_advisor_3000.product_plane.research import data_snapshots as owner
    from trading_advisor_3000.product_plane.research import hash_compatibility as bridge
    from trading_advisor_3000.product_plane.research import refresh_scope

    def consume(contract, source):
        current = {
            **pending["runtime_provenance"],
            "source_sha256": source,
            "git_commit": source,
            "contracts_sha256": source,
        }
        receipt = bridge.compatibility_receipt(accepted, current, contract)
        assert receipt["version"] == 2 and "source_sha256" not in receipt
        config["hash_compatibility"] = receipt
        pending.update(runtime_provenance=current, semantic_compute_contract=contract)
        monkeypatch.setattr(refresh_scope, "runtime_provenance", lambda: current)
        monkeypatch.setattr(
            bridge, "reviewed_current_source", lambda: pytest.fail("modern receipt used bootstrap")
        )
        scope = refresh_scope.resolve_refresh_scope(
            data_manifest_path=Path(config["data_snapshot_manifest_path"]),
            materialized_output_dir=Path(config["materialized_output_dir"]),
            dataset_version=config["dataset_version"],
            timeframes=config["timeframes"],
            configuration=config,
        )
        assert scope["force_full_refresh"] is False and scope["windows"] == []
        assert owner._validate_hash_compatibility(config, pending, instance) == accepted

    assert before == c.semantic_compute_contract(package=package)

    def raw():
        return f.digest(
            {
                p.relative_to(package).as_posix(): p.read_text(encoding="utf-8-sig")
                for p in package.rglob("*.py")
            }
        )

    previous = raw()
    writer = package / "product_plane/research/derived_indicators/store.py"
    old = writer.read_text()
    assert "DEFAULT_DERIVED_INDICATOR_WRITE_BATCH_ROWS = 100_000" in old
    writer.write_text(
        old.replace(
            "DEFAULT_DERIVED_INDICATOR_WRITE_BATCH_ROWS = 100_000",
            "DEFAULT_DERIVED_INDICATOR_WRITE_BATCH_ROWS = 50_000",
        )
    )
    telemetry = package / "product_plane/common/stage_timings.py"
    telemetry.write_text(telemetry.read_text().replace('"stage_started"', '"native_stage_started"'))
    first = c.semantic_compute_contract(package=package)
    assert raw() != previous and c.compare_contracts(before, first)
    consume(first, raw())
    previous = raw()
    clock = package / "spark_jobs/continuous_front_job.py"
    clock.write_text(
        clock.read_text().replace("replace(microsecond=0)", "replace(microsecond=123000)")
    )
    kernel = package / "product_plane/research/indicators/materialize.py"
    text = kernel.read_text()
    start, end = text.index("def _indicator_value("), text.index("def _merge_indicator_values(")
    block = text[start:end]
    kernel.write_text("# second arbitrary release\n" + text[:start] + text[end:] + "\n" + block)
    second = c.semantic_compute_contract(package=package)
    assert raw() != previous and c.compare_contracts(before, second)
    consume(second, raw())


@pytest.mark.parametrize(
    "path,old,new,component",
    [
        ("indicators/materialize.py", "return float(value)\n", "return float(value) * 2\n", "l1"),
        (
            "indicators/volume_profile.py",
            "VP_QUALITY_NO_SOURCE = 1",
            "VP_QUALITY_NO_SOURCE = 2",
            "l1",
        ),
        (
            "derived_indicators/materialize.py",
            "return int(value)\n",
            "return int(value) + 1\n",
            "l2",
        ),
    ],
)
def test_real_numeric_adapter_and_helper_mutations_invalidate(package, path, old, new, component):
    before = c.kernel_contract(package=package)
    target = package / "product_plane/research" / path
    text = target.read_text()
    assert old in text
    target.write_text(text.replace(old, new, 1))
    assert c.kernel_contract(package=package)[component] != before[component]


def test_quality_and_engine_changes_are_not_numeric_full_requests():
    original = c.semantic_compute_contract()
    for field, message in [("quality", "revalidation"), ("engine", "authority")]:
        changed = {**original, field: "a" * 64}
        changed["digest"] = f.digest({k: v for k, v in changed.items() if k != "digest"})
        with pytest.raises(f.ContractError, match=message):
            c.compare_contracts(original, changed)
    damaged = copy.deepcopy(original)
    damaged["kernels"]["l1"] = "forged"
    with pytest.raises(f.ContractError):
        c.compare_contracts(damaged, original)


@pytest.fixture
def published_contract(tmp_path, monkeypatch):
    from trading_advisor_3000.product_plane.common import data_snapshots as storage
    from trading_advisor_3000.product_plane.research import data_snapshots as owner

    canonical, output = tmp_path / "canonical", tmp_path / "research"
    canonical.mkdir()
    output.mkdir()
    tables = {}
    for name in (
        "canonical_bars",
        "canonical_session_calendar",
        "canonical_roll_map",
        "raw_fixture",
    ):
        path = canonical / (name + ".delta")
        write_deltalake(str(path), pa.table({"value": [1.0]}))
        tables[name] = storage.capture_table(path)
    data = storage.create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="fixture",
        run_id="data",
        code_version="test",
        quality_checks={"data": "PASS"},
        parameters={"downstream_scope": {"version": 1, "full_refresh": False}},
    )
    root = canonical / ".data_snapshots"
    data_path = storage.save_snapshot(root, data)
    storage.publish_snapshot(root, data_path, expected_current_id=None)
    config = dict(
        data_snapshot_enabled=True,
        canonical_output_dir=str(canonical),
        materialized_output_dir=str(output),
        dataset_version="fixture",
        code_version="test",
        series_mode="continuous_front",
        timeframes=["15m"],
        refresh_scope_version=1,
        force_full_refresh=False,
        changed_windows=[],
        data_snapshot_manifest_path=str(data_path),
    )
    status = SimpleNamespace(value="SUCCESS")
    instance = SimpleNamespace(
        get_run_by_id=lambda _: SimpleNamespace(status=status),
        get_run_step_stats=lambda _: [
            SimpleNamespace(step_key=name, status=status) for name in owner._STAGE_OUTPUTS
        ],
    )
    context = SimpleNamespace(run_id="producer", op_config=config, instance=instance)
    # The new producer is the checkpoint/manifest owner; native numerical coverage is
    # exercised separately. No semantic object is hand-assigned into this checkpoint.
    pending = owner._initial(context, config)
    for name in (
        "research_datasets",
        "research_instrument_tree",
        "research_bar_views",
        "continuous_front_bars",
        "continuous_front_roll_events",
        "continuous_front_adjustment_ladder",
        "continuous_front_qc_report",
        "continuous_front_indicator_acceptance_report",
        "cf_indicator_input_frame",
        "indicator_roll_rules",
        "continuous_front_indicator_frames",
        "continuous_front_derived_indicator_frames",
        "continuous_front_indicator_qc_observations",
        "continuous_front_indicator_run_manifest",
    ):
        path = output / (name + ".delta")
        write_deltalake(str(path), pa.table({"value": [1.0]}))
        pending["tables"][name] = storage.capture_table(path)
    # Actual native values and writers participate in the persisted proof.
    from dataclasses import replace

    from deltalake import DeltaTable
    from test_research_derived_indicator_layer import _view

    from trading_advisor_3000.product_plane.research.derived_indicators import materialize as l2
    from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
        current_derived_indicator_profile,
    )
    from trading_advisor_3000.product_plane.research.derived_indicators.store import (
        write_derived_indicator_frame_batches,
    )
    from trading_advisor_3000.product_plane.research.indicators import materialize as l1
    from trading_advisor_3000.product_plane.research.indicators.registry import (
        default_indicator_profile,
    )
    from trading_advisor_3000.product_plane.research.indicators.store import (
        write_indicator_frame_partition_batches,
    )

    bars = [
        replace(_view(ts_index=i, close=100 + i * 0.2), dataset_version="fixture")
        for i in range(240)
    ]
    p1 = default_indicator_profile()
    rows1 = l1._build_partition_rows(
        dataset_version="fixture", indicator_set_version="indicators-v1", profile=p1, series=bars
    )
    key1 = rows1[0].partition_key(series_mode="contract")
    write_indicator_frame_partition_batches(
        output_dir=output, partition_row_batches=((key1, rows1),), profile=p1
    )
    p2 = current_derived_indicator_profile()
    rows2 = l2._build_prepared_partition_rows(
        dataset_version="fixture",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        series_mode="contract",
        profile=p2,
        local_series=bars,
        local_indicator_rows=rows1,
    )
    write_derived_indicator_frame_batches(output_dir=output, row_batches=[rows2], profile=p2)
    for name in ("research_indicator_frames", "research_derived_indicator_frames"):
        path = output / (name + ".delta")
        table = DeltaTable(str(path)).to_pyarrow_table()
        assert table.num_rows == len(bars) == 240
        assert len(set(table["ts"].to_pylist())) == 240
        pending["tables"][name] = storage.capture_table(path)
    assert rows1[-1].values["sma_20"] == pytest.approx(sum(bar.close for bar in bars[-20:]) / 20)
    assert rows2[-1].values["rolling_high_20"] == pytest.approx(max(bar.high for bar in bars[-20:]))
    stages = (
        "continuous_front_bars",
        "research_datasets",
        "research_indicator_frames",
        "research_derived_indicator_frames",
        "continuous_front_indicator_acceptance_report",
    )
    pending["quality_checks"] = {name: "PASS" for name in stages}
    pending["quality_owners"] = {name: "producer" for name in stages}
    storage.atomic_json(owner._pending_path(context, config), pending)
    # Metadata fixture tables intentionally do not implement the market schemas.
    monkeypatch.setattr(owner, "preflight", lambda _: None)
    acceptance = {"sidecar_result": {"status": "PASS"}}
    result = owner.publish_research_snapshot(context, acceptance=acceptance)
    accepted = storage.load_snapshot(Path(result["snapshot_manifest_path"]))
    produced = accepted["parameters"]["semantic_compute_contract"]
    assert c.compare_contracts(produced, c.semantic_compute_contract(config))
    assert produced == pending["semantic_compute_contract"]
    before = {key: ref["delta_version"] for key, ref in accepted["tables"].items()}
    assert owner.publish_research_snapshot(context, acceptance=acceptance) == result
    assert before == {
        key: ref["delta_version"]
        for key, ref in storage.current_snapshot(owner._registry(config))["tables"].items()
    }
    native = owner.runtime_provenance
    with monkeypatch.context() as drift:
        drift.setattr(
            owner, "runtime_provenance", lambda: {**native(), "source_sha256": "different-release"}
        )
        with pytest.raises(ValueError, match="same parameters and code"):
            owner._initial(context, config)
    # Market/QC inputs are explicit fixtures; this proves the compute-contract boundary.
    instance.get_run_step_stats = lambda _: [
        SimpleNamespace(step_key=name, status=status) for name in stages
    ]
    return produced, accepted, config, pending, instance


def test_real_pending_and_publisher_emit_consumable_contract(published_contract):
    contract = published_contract[0]
    assert c.validate_contract(contract) == contract


def test_class_method_reorder_preserves_fields_and_rejects_shadowing(tmp_path):
    prefix = "from dataclasses import dataclass\n@dataclass\nclass Spec:\n    value: int = 2\n"
    first = "    def apply(self,x):\n        return x*self.value\n"
    second = "    def offset(self):\n        return self.value+1\n"
    suffix = "def kernel(spec: Spec,x):\n    return spec.apply(x)+spec.offset()\n"
    before = _module(tmp_path, prefix + first + second + suffix)()
    assert _module(tmp_path, prefix + second + first + suffix)() == before
    for class_body in (first + "    apply=1\n", "    apply=1\n" + first):
        with pytest.raises(f.ContractError):
            _module(tmp_path, prefix + class_body + suffix)()


def test_local_relative_import_inside_package_uses_package_parent(tmp_path):
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "__init__.py").write_text(
        "def kernel(x):\n    from .helper import formula\n    return formula(x)\n"
    )
    (nested / "helper.py").write_text("def formula(x):\n    return x+1\n")
    (tmp_path / "helper.py").write_text("def formula(x):\n    return x+999\n")
    graph = f.SymbolGraph(tmp_path)
    graph.fingerprint(("trading_advisor_3000.nested.kernel",))
    assert "trading_advisor_3000.nested.helper.formula" in graph.selected
    assert "trading_advisor_3000.helper.formula" not in graph.selected


def test_real_proof_codec_mutation_requires_adaptation_not_recompute(package):
    before = c.semantic_compute_contract(package=package)
    path = package / "spark_jobs/continuous_front_indicator_sidecar_job.py"
    text = path.read_text()
    tree = ast.parse(text)
    constant = next(
        node
        for node in tree.body
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(
            isinstance(item, ast.Name) and item.id == "ROW_HASH_VERSION" for item in ast.walk(node)
        )
    )
    lines = text.splitlines(True)
    lines[constant.lineno - 1 : constant.end_lineno] = ['ROW_HASH_VERSION = "proof-codec-next"\n']
    path.write_text("".join(lines))
    after = c.semantic_compute_contract(package=package)
    assert before["kernels"] == after["kernels"]
    assert before["proof"] != after["proof"]
    with pytest.raises(f.ContractError, match="adaptation"):
        c.compare_contracts(before, after)


def test_native_scope_reuses_identity_per_operation_and_rejects_drift(monkeypatch):
    identities = [c._current_numeric_identity()]
    calls = []

    def inspect():
        calls.append(1)
        return identities[-1]

    monkeypatch.setattr(c, "_current_numeric_identity", inspect)

    @c.native_compute_scope
    def operation(change=False):
        for _ in range(100):
            assert c.numerical_identity() == identities[0]
        if change:
            identities.append({**identities[0], "engine": "changed"})

    operation()
    assert len(calls) == 2
    with pytest.raises(f.ContractError, match="changed during native operation"):
        operation(True)
    assert c._active_numeric_identity.get() is None


def test_external_alias_and_external_reexport_are_formula_dependencies(tmp_path):
    text = "from math import sqrt as operation\ndef kernel(x):\n    return operation(x)\n"
    before = _module(tmp_path, text)()
    assert _module(tmp_path, text.replace("sqrt", "exp"))() != before
    (tmp_path / "other.py").write_text("from math import sqrt as formula\n")
    probe = _module(tmp_path, "from .other import formula\ndef kernel(x):\n    return formula(x)\n")
    before = probe()
    (tmp_path / "other.py").write_text("from math import exp as formula\n")
    assert probe() != before
    with pytest.raises(f.ContractError, match="external numerical provider"):
        _module(
            tmp_path,
            'from os import getenv\ndef kernel(x):\n    return float(getenv("FORMULA", x))\n',
        )()


def test_external_provider_policy_is_part_of_authority(tmp_path, monkeypatch):
    path = tmp_path / "engine.py"
    source = Path(f.__file__).read_text()
    before = f.engine_identity()
    path.write_text(source.replace('"zoneinfo",', '"zoneinfo", "os",'))
    monkeypatch.setattr(f, "__file__", str(path))
    assert f.engine_identity() != before
