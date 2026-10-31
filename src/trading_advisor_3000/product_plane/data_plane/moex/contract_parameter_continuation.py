"""Admission of reviewed bounded parameter bundles; no acquisition or writes."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from deltalake import DeltaTable

from .economics import MoexContractParameterRegime, validate_contract_parameter_regimes


@dataclass(frozen=True)
class MoexContractParameterEvidence:
    bundle_id: str
    regimes_path: Path
    dated_official_tc_path: Path


@dataclass(frozen=True)
class _AdmittedBundle:
    regimes_sha256: str
    evidence_sha256: str
    prepared_table_id: str
    prepared_version: int = 0


_BUNDLE_ID = "bounded-tail-20261005-v1"
_ADMITTED_BUNDLE = _AdmittedBundle(
    "9eef9156c8d83ebede9ceaa9f7824a71543239d9e0215b9d5cebb8b2990ac590",
    "666a0a618b4b1f337b1c874623e14da72a08eda6c35dfccff83057e770b08f63",
    "d550b9e4-e3d6-40dc-93f0-fc0e420e3b43",
)
_FRESH_BUNDLE_ID = "bounded-tail-20261010-v1"
_FRESH_ADMITTED_BUNDLE = _AdmittedBundle(
    "6c6df0f55d0c61f229115d30089102ae70cc7dddde04248020bb10007376b812",
    "e962938f4ac5b466a74d53225373b2526d650ab2a06b72a493458924dfd961f8",
    "d550b9e4-e3d6-40dc-93f0-fc0e420e3b43",
    prepared_version=1,
)
_MAX_METADATA_BYTES = 64 * 1024
_ASSETS = frozenset(
    {"BR", "GOLD", "SILV", "PLD", "PLT", "NG", "SPYF", "NASD", "RTS", "MIX", "MXI", "RGBI", "WHEAT"}
)
_BOUNDS = ("2026-07-29", "2026-10-05")
_FRESH_BOUNDS = ("2026-07-29", "2026-10-10")


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate evidence JSON key: {key}")
        value[key] = item
    return value


def _reject_constant(value):
    raise ValueError(f"nonfinite evidence JSON constant: {value}")


def _read_verified_json(path: Path, expected_sha: str):
    if not isinstance(path, Path):
        raise TypeError("parameter evidence paths must be Path")
    with path.open("rb") as source:
        payload = source.read(_MAX_METADATA_BYTES + 1)
    if len(payload) > _MAX_METADATA_BYTES:
        raise ValueError("parameter evidence exceeds 64KiB")
    measured = hashlib.sha256(payload).hexdigest()
    if measured != expected_sha:
        raise ValueError("parameter evidence SHA256 does not match admitted bundle")
    return json.loads(
        payload.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_constant
    ), measured


def validate_contract_parameter_evidence(
    additional: tuple[MoexContractParameterRegime, ...],
    evidence: MoexContractParameterEvidence | None,
) -> dict[str, object] | None:
    """Validate tuple and receipt bytes before touching any Delta source."""
    validate_contract_parameter_regimes(additional)
    if type(evidence) is _OwnedRenewalEvidence:
        return _validate_owned_renewal(additional, evidence)
    if not additional:
        if evidence is not None:
            raise ValueError("empty additional tuple requires no parameter evidence")
        return None
    if type(evidence) is not MoexContractParameterEvidence:
        raise TypeError("additional regimes require MoexContractParameterEvidence")
    if evidence.bundle_id == _BUNDLE_ID:
        admitted_bundle, bounds = _ADMITTED_BUNDLE, _BOUNDS
    elif evidence.bundle_id == _FRESH_BUNDLE_ID:
        admitted_bundle, bounds = _FRESH_ADMITTED_BUNDLE, _FRESH_BOUNDS
    else:
        raise ValueError("parameter evidence bundle is not admitted")
    rules, rules_sha = _read_verified_json(evidence.regimes_path, admitted_bundle.regimes_sha256)
    dated, dated_sha = _read_verified_json(
        evidence.dated_official_tc_path, admitted_bundle.evidence_sha256
    )
    if type(rules) is not list or rules != [asdict(rule) for rule in additional]:
        raise ValueError("parameter evidence tuple differs from supplied regimes")
    if len(additional) != 13 or {r.assetcode for r in additional} != _ASSETS:
        raise ValueError("parameter continuation requires the admitted thirteen assets")
    for rule in additional:
        if (
            rule.rule_id != f"{rule.assetcode}-{evidence.bundle_id}"
            or (rule.effective_from, rule.effective_to) != bounds
            or rule.validation_method != "dated_official_tc"
        ):
            raise ValueError("parameter continuation differs from admitted bounded interval")
        prefix, separator, digest = rule.source_document_id.rpartition(";sha256:")
        if not separator or not prefix.strip() or ";sha256:" in prefix or digest != dated_sha:
            raise ValueError("parameter source_document_id does not bind measured evidence SHA")
    if (
        type(dated) is not dict
        or dated.get("status") != "PASS"
        or dated.get("validation_method") != "dated_official_tc"
        or type(dated.get("snapshot_version")) is not int
        or dated["snapshot_version"] != 134
        or dated.get("extrapolation_scope") != list(bounds)
        or dated.get("inference_not_daily_official") is not True
        or dated.get("turnover_proxy_rejected_due_to_nonidentical_trade_baskets") is not True
        or type(dated.get("dated_official_rows")) is not int
        or dated["dated_official_rows"] != 674
    ):
        raise ValueError("dated official receipt does not describe admitted inference")
    profiles = dated.get("profiles")
    if type(profiles) is not list or len(profiles) != 13:
        raise ValueError("dated official receipt requires thirteen unique profiles")
    assets, samples = set(), 0
    for profile in profiles:
        if (
            type(profile) is not dict
            or profile.get("assetcode") not in _ASSETS
            or profile["assetcode"] in assets
        ):
            raise ValueError("invalid or duplicate dated official profile")
        assets.add(profile["assetcode"])
        rows, p95, maximum = (profile.get(key) for key in ("rows", "p95", "maximum"))
        if type(rows) is not int or rows <= 0:
            raise ValueError("dated official profile requires positive sample count")
        if (
            any(type(v) not in (int, float) or not math.isfinite(v) for v in (p95, maximum))
            or not 0 <= p95 <= maximum < 0.001
        ):
            raise ValueError("invalid admitted dated official profile error")
        samples += rows
    if samples != 674:
        raise ValueError("dated official profile sample sum differs from receipt")
    return {
        "bundle_id": evidence.bundle_id,
        "regimes_sha256": rules_sha,
        "dated_official_tc_evidence_sha256": dated_sha,
        "prepared_input_table_id": admitted_bundle.prepared_table_id,
        "prepared_input_version": admitted_bundle.prepared_version,
        "receipt_dated_official_rows": samples,
        "inference_not_daily_official": True,
    }


def bind_contract_parameter_input(table_path: Path, verified: dict[str, object]) -> int:
    """Native metadata only; the owning Spark reader must read this exact version."""
    if not isinstance(table_path, Path) or not table_path.is_absolute():
        raise ValueError("parameter prepared input path must be absolute")
    version = int(verified["prepared_input_version"])
    snapshot = DeltaTable(str(table_path), version=version)
    if (
        snapshot.metadata().id != verified["prepared_input_table_id"]
        or snapshot.version() != version
    ):
        raise ValueError("parameter prepared input Delta identity/version mismatch")
    return version


@dataclass(frozen=True)
class _OwnedRenewalEvidence:
    renewal: object


def renew_contract_parameter_evidence(renewal):
    """Only the owning native acquisition result can renew reviewed constant units."""
    evidence = _OwnedRenewalEvidence(renewal)
    rules = _renewed_rules(renewal)
    _validate_owned_renewal(rules, evidence)
    return rules, evidence


def _renewed_rules(renewal):
    from .native_history_refresh import _RENEWAL_OWNER, NativeHistoryRenewal

    if type(renewal) is not NativeHistoryRenewal or renewal._owner is not _RENEWAL_OWNER:
        raise TypeError("parameter renewal requires the actual owning history result")
    return tuple(
        replace(
            rule,
            effective_to=renewal.observed_date_end,
            rule_id=f"{rule.assetcode}-native-history-{renewal.observed_date_end}",
        )
        for rule in renewal.base_rules
    )


def _validate_owned_renewal(additional, evidence):
    from datetime import date

    import pyarrow.dataset as ds

    from .native_history_refresh import (
        HISTORY_SOURCE,
        PREPARED_SOURCE,
        _current_crosscheck_hash,
        _hash,
    )

    renewal = evidence.renewal
    if additional != _renewed_rules(renewal):
        raise ValueError("renewed native parameter tuple differs from owning result")
    base = validate_contract_parameter_evidence(renewal.base_rules, renewal.base_evidence)
    expected_proof = _hash(
        dict(
            tables=renewal.tables,
            crosscheck=renewal.current_crosscheck,
            source_hash=renewal.source_hash,
            observed_dates=[renewal.observed_date_start, renewal.observed_date_end],
            admitted_date_end=renewal.admitted_date_end,
        )
    )
    if expected_proof != renewal.proof_sha256:
        raise ValueError("owning native renewal result was altered")
    start, end, admitted = (
        date.fromisoformat(value)
        for value in (
            renewal.observed_date_start,
            renewal.observed_date_end,
            renewal.admitted_date_end,
        )
    )
    if not start <= admitted <= end or (end - start).days > 31:
        raise ValueError("native renewal bounds lack actual dated coverage")
    identities = {
        "raw": "ae9e49f9-9e8f-405d-9e07-0a2079eee691",
        "prepared": base["prepared_input_table_id"],
        "oi": "026176bf-9d3b-4192-adcf-559cd33f7c93",
    }
    tables = {}
    for name, identity in identities.items():
        ref = renewal.tables[name]
        path, version = Path(ref["table_path"]), ref["delta_version"]
        if not path.is_absolute() or type(version) is not int or version < 0:
            raise ValueError("native renewal table ref requires absolute path/exact version")
        table = DeltaTable(str(path), version=version)
        if table.metadata().id != identity or ref["table_id"] != identity:
            raise ValueError("native renewal owning table identity mismatch")
        tables[name] = table
    predicate = (ds.field("trade_date") >= start) & (ds.field("trade_date") <= end)
    raw = (
        tables["raw"]
        .to_pyarrow_dataset()
        .scanner(
            columns=["source_document_hash"],
            filter=predicate & (ds.field("source_id") == HISTORY_SOURCE),
        )
        .head(10001)
        .to_pylist()
    )
    if (
        len(raw) > 10000
        or _hash(sorted(row["source_document_hash"] for row in raw)) != renewal.source_hash
    ):
        raise ValueError("native renewal history hashes differ from pinned source")
    prepared = (
        tables["prepared"]
        .to_pyarrow_dataset()
        .scanner(
            columns=["assetcode", "min_step", "lot_volume", "enrichment_provenance_json"],
            filter=predicate & (ds.field("source_id") == PREPARED_SOURCE),
        )
        .head(10001)
        .to_pylist()
    )
    native = {rule.assetcode: rule for rule in renewal.base_rules}
    if not prepared or len(prepared) > 10000 or {row["assetcode"] for row in prepared} != _ASSETS:
        raise ValueError("native prepared scope lacks reviewed thirteen assets")
    for row in prepared:
        rule = native[row["assetcode"]]
        provenance = json.loads(row["enrichment_provenance_json"])
        if (
            row["min_step"] != rule.min_step
            or row["lot_volume"] != rule.lot_volume
            or provenance.get("parameter_validation_evidence_sha256")
            != base["dated_official_tc_evidence_sha256"]
            or provenance.get("current_snapshot_crosscheck_sha256")
            != _current_crosscheck_hash(renewal.current_crosscheck)
            or provenance.get("current_snapshot_not_daily_official") is not True
        ):
            raise ValueError("native prepared row differs from reviewed renewal provenance")
    return {
        **base,
        "bundle_id": f"native-history-{renewal.observed_date_end}",
        "regimes_sha256": _hash([asdict(rule) for rule in additional]),
        "prepared_input_version": renewal.tables["prepared"]["delta_version"],
        "renewal_proof_sha256": renewal.proof_sha256,
        "renewal_admitted_date_end": renewal.admitted_date_end,
        "current_snapshot_mc_crosscheck": renewal.current_crosscheck,
    }
