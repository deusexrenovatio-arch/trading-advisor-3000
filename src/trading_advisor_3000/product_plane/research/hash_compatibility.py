"""Semantic accepted-snapshot admission and one-time presemantic bootstrap.

Raw runtime provenance remains the strict resume/publication contract. This
adapter admits an accepted snapshot, never arbitrary caller permission.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar

# Exact post-226 production79257fa3, reconstructed from its CRLF Git checkout.
NATIVE_LEGACY_SOURCE = "8a4432db9fc8e5da29cc3b6668b61972ded6bc270072252e000d4414a6827fbb"
LEGACY_NATIVE_L2_IMPLEMENTATION = (
    "3183b255524a646acbf791cad4a12b31bd5017d410a7c8ff444ef7c95a40fb15",
    "03e55fd245842af90f697ad9a30cdd5005e88038060bf56e44bf62685dcc3f9c",
    "78630475a3a79745d9116a6e00b888e1bdc258240a7f839fb280c495c29a0ee1",
    "b017e27e3ed51f21b8b0799bbfa06ebf252306c895707b07ed00c29205204796",
    "2f4efdbb0a62db05f963b92daad11bcc2d7a1e83ef847fb8537706280ecec376",
    "f003b68ccafe64d2bd063dd9ea94d9eebdf2b5462619b46d697f4c3301ba059f",
)
LEGACY_SOURCES = {
    # Exact deployed raw file hashes; both changes leave numerical formulas intact.
    "6c41c2dfc0dca91d67eeb63065ccb23e6368895e511aab4c816868bfc3c4acec": (
        "0d697e2ac44572fa2bb62bf9b153b515bc556a4bf822b4b4700bf991fd6521f9"
    ),
    "6c266cccbd56afd3682146aba0a98a9aa7be9385d1f24408724b0ffdce30b188": (
        "5a297d32aeadf2e80a989396dc42be83cbb27a6e67a3f39bf86c88eb63929e49"
    ),
    NATIVE_LEGACY_SOURCE: LEGACY_NATIVE_L2_IMPLEMENTATION[0],
}
CANONICAL_L2_IMPLEMENTATION_SHA256 = next(iter(LEGACY_SOURCES.values()))
SCOPE_PARAMETER_KEYS = frozenset(
    {
        "changed_windows",
        "refresh_scope_version",
        "force_full_refresh",
        "data_snapshot_manifest_path",
        "hash_compatibility",
    }
)
REQUIRED_QUALITY = frozenset(
    {
        "continuous_front_bars",
        "research_datasets",
        "research_indicator_frames",
        "research_derived_indicator_frames",
        "continuous_front_indicator_acceptance_report",
        "continuous_front_acceptance",
    }
)
_source: ContextVar[str | None] = ContextVar("reviewed_hash_migration", default=None)


# Fixed ONCE for the audited old792 -> v1 numerical baseline migration.
# Ordinary future releases compare their persisted semantic contracts directly.
BOOTSTRAP_KERNELS = {
    "cf": "9c1d7d5784c79cf748078cffe96ad9d4a1e196a74e3bc874e805d279933d721d",
    "l0": "9bd5c855759d7bedfbf095eaddb21d569b5ffe58ec85ef3a77f213bb4e75638e",
    "l1": "fb2aaa135c70444bb31355cea88981968f09c97def44074440d03f6a15fc4466",
    "l2": "2d5463704336a5676d1a7c1d6bf74dbf7210929dd9897f2b5d3379f7cb982080",
    "sidecar": "b52597f218b6783378e96cd0235050384a1c94f5a467ad67637c15851cc1b275",
    "proof": "c1ea5683400d6401f3b9b9bc51da41fe1f511c267ce3a2a8acb016f79aa8930d",
    "quality": "4e4f4b3f0aaf9af01f3c9cfa85112c0caa19ee4adfa96c5827ffff0dc1c21186",
}
BOOTSTRAP_ENGINE = "c00f66d72676fc94012c3f063a56b14a5fe875d7d6ecdc4331d5aa01ccfb9be2"


BOOTSTRAP_CONTRACT_DIGEST = "2aea7705130ee86a42f77b15312550e03ce9d829c10e5c4941688a2771060ede"


def reviewed_current_source() -> bool:
    from .compute_contract import semantic_compute_contract

    return semantic_compute_contract()["digest"] == BOOTSTRAP_CONTRACT_DIGEST


def compatible_provenance(
    old: dict, current: dict, old_contract=None, current_contract=None
) -> bool:
    if old_contract is not None:
        from .compute_contract import compare_contracts

        return compare_contracts(old_contract, current_contract)
    return (
        old.get("source_sha256") == NATIVE_LEGACY_SOURCE
        and {k: v for k, v in old.items() if k != "source_sha256"}
        == {k: v for k, v in current.items() if k != "source_sha256"}
        and reviewed_current_source()
        and (
            current_contract is None or current_contract.get("digest") == BOOTSTRAP_CONTRACT_DIGEST
        )
    )


def compatibility_receipt(accepted: dict, current: dict, contract: dict) -> dict | None:
    parameters = accepted["parameters"]
    if not compatible_provenance(
        parameters.get("runtime_provenance", {}),
        current,
        parameters.get("semantic_compute_contract"),
        contract,
    ):
        return None
    base = {"snapshot_id": accepted["snapshot_id"], "manifest_sha256": accepted["manifest_sha256"]}
    if parameters.get("semantic_compute_contract") is not None:
        return {"version": 2, **base, "compute_digest": contract["digest"]}
    return {"version": 1, **base, "source_sha256": NATIVE_LEGACY_SOURCE}


def migration_source() -> str | None:
    source = _source.get()
    if source and not reviewed_current_source():
        raise ValueError("reviewed migration source changed")
    return source


@contextmanager
def _verified_migration_scope(source: str):
    # Only the snapshot owner calls this after validating accepted state/QC.
    if source != NATIVE_LEGACY_SOURCE or not reviewed_current_source():
        raise ValueError("unreviewed hash migration")
    token = _source.set(source)
    try:
        yield
        if not reviewed_current_source():
            raise ValueError("reviewed migration source changed")
    finally:
        _source.reset(token)
