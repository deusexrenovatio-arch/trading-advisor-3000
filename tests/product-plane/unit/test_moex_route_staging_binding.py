from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.route_staging_binding import (
    build_route_staging_binding_report,
)


def _artifact_paths(tmp_path: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for mode in ("nightly_1", "nightly_2", "repair", "backfill", "recovery"):
        path = tmp_path / "input-artifacts" / f"{mode}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"mode": mode, "status": "PASS"}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        paths[mode] = path
    return paths


def _run_ids() -> dict[str, str]:
    return {
        "nightly_1": "phase03-staging-nightly-1",
        "nightly_2": "phase03-staging-nightly-2",
        "repair": "phase03-staging-repair-1",
        "backfill": "phase03-staging-backfill-1",
        "recovery": "phase03-staging-recovery-1",
    }


def test_build_route_staging_binding_report_copies_artifacts_and_emits_report(tmp_path: Path) -> None:
    artifact_paths = _artifact_paths(tmp_path)
    run_ids = _run_ids()

    def fake_fetcher(run_id: str) -> dict[str, object]:
        return {
            "run_id": run_id,
            "status": "SUCCESS",
            "job_name": "moex_historical_cutover_job",
            "start_time": 1.0,
            "end_time": 2.0,
            "update_time": 3.0,
            "tags": {"dagster/mode": "test"},
            "graphql_url": "https://dagster-staging.example.internal/graphql",
        }

    result = build_route_staging_binding_report(
        dagster_url="https://dagster-staging.example.internal",
        output_dir=tmp_path / "bundle",
        run_ids=run_ids,
        artifact_paths_by_mode=artifact_paths,
        extra_real_bindings=["delta://baseline/moex"],
        fetch_run_summary=fake_fetcher,
    )

    assert result["status"] == "PASS"
    report_path = Path(str(result["staging_binding_report_path"]))
    verification_path = Path(str(result["dagster_run_verification_path"]))
    assert report_path.exists()
    assert verification_path.exists()

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["proof_class"] == "staging-real"
    assert report["environment"] == "staging-real"
    assert report["run_ids"] == run_ids
    assert "dagster://staging/moex-historical-cutover" in report["real_bindings"]
    assert "delta-ledger-cas://technical-route-run-ledger" in report["real_bindings"]
    assert "delta://baseline/moex" in report["real_bindings"]

    copied_paths = [Path(path) for path in report["artifact_paths"]]
    assert len(copied_paths) == 5
    assert all(path.exists() for path in copied_paths)
    assert all(str(path).startswith(str((tmp_path / "bundle").resolve())) for path in copied_paths)

    verification = json.loads(verification_path.read_text(encoding="utf-8"))
    assert verification["status"] == "PASS"
    assert verification["job_name"] == "moex_historical_cutover_job"
    assert verification["run_summaries"]["nightly_1"]["status"] == "SUCCESS"


def test_build_route_staging_binding_report_fails_closed_when_run_is_not_success(tmp_path: Path) -> None:
    artifact_paths = _artifact_paths(tmp_path)

    def fake_fetcher(run_id: str) -> dict[str, object]:
        return {
            "run_id": run_id,
            "status": "FAILURE",
            "job_name": "moex_historical_cutover_job",
            "start_time": 1.0,
            "end_time": 2.0,
            "update_time": 3.0,
            "tags": {},
            "graphql_url": "https://dagster-staging.example.internal/graphql",
        }

    with pytest.raises(ValueError, match="must be SUCCESS"):
        build_route_staging_binding_report(
            dagster_url="https://dagster-staging.example.internal",
            output_dir=tmp_path / "bundle",
            run_ids=_run_ids(),
            artifact_paths_by_mode=artifact_paths,
            fetch_run_summary=fake_fetcher,
        )


def test_build_route_staging_binding_report_rejects_localhost_dagster_url(tmp_path: Path) -> None:
    artifact_paths = _artifact_paths(tmp_path)

    def fake_fetcher(run_id: str) -> dict[str, object]:
        raise AssertionError(f"collector must fail before querying run `{run_id}`")

    with pytest.raises(ValueError, match="loopback or unspecified host|external staging Dagster host"):
        build_route_staging_binding_report(
            dagster_url="http://127.0.0.1:3011",
            output_dir=tmp_path / "bundle",
            run_ids=_run_ids(),
            artifact_paths_by_mode=artifact_paths,
            fetch_run_summary=fake_fetcher,
        )
