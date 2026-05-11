from __future__ import annotations

from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    MOEX_HISTORICAL_DATA_ROOT_ENV,
    MOEX_PRODUCT_RUNTIME_STAGING_ROOT_ENV,
    MOEX_VERIFICATION_STAGING_ROOT_ENV,
    configured_moex_runtime_staging_roots,
)


def _external_root(name: str) -> Path:
    return Path.cwd().resolve().parent / ".ta3000-moex-staging-roots-unit" / name


def test_runtime_staging_roots_default_to_separate_external_subdirs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _external_root("ta3000-data-default")
    monkeypatch.setenv(MOEX_HISTORICAL_DATA_ROOT_ENV, data_root.as_posix())
    monkeypatch.delenv(MOEX_PRODUCT_RUNTIME_STAGING_ROOT_ENV, raising=False)
    monkeypatch.delenv(MOEX_VERIFICATION_STAGING_ROOT_ENV, raising=False)

    roots = configured_moex_runtime_staging_roots(repo_root=Path.cwd())

    assert roots.product_runtime_root == data_root / "staging" / "product-runtime"
    assert roots.verification_root == data_root / "staging" / "verification"


def test_runtime_staging_roots_accept_explicit_external_roots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _external_root("ta3000-data-explicit")
    product_root = _external_root("ta3000-product-runtime-staging")
    verification_root = _external_root("ta3000-verification-staging")
    monkeypatch.setenv(MOEX_HISTORICAL_DATA_ROOT_ENV, data_root.as_posix())
    monkeypatch.setenv(MOEX_PRODUCT_RUNTIME_STAGING_ROOT_ENV, product_root.as_posix())
    monkeypatch.setenv(MOEX_VERIFICATION_STAGING_ROOT_ENV, verification_root.as_posix())

    roots = configured_moex_runtime_staging_roots(repo_root=Path.cwd())

    assert roots.product_runtime_root == product_root
    assert roots.verification_root == verification_root


def test_runtime_staging_roots_reject_overlap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _external_root("ta3000-data-overlap")
    staging_root = _external_root("shared-staging")
    monkeypatch.setenv(MOEX_HISTORICAL_DATA_ROOT_ENV, data_root.as_posix())
    monkeypatch.setenv(MOEX_PRODUCT_RUNTIME_STAGING_ROOT_ENV, staging_root.as_posix())
    monkeypatch.setenv(
        MOEX_VERIFICATION_STAGING_ROOT_ENV, (staging_root / "verification").as_posix()
    )

    with pytest.raises(RuntimeError, match="must not overlap"):
        configured_moex_runtime_staging_roots(repo_root=Path.cwd())
