from __future__ import annotations

from datetime import UTC, datetime
import ipaddress
import json
from pathlib import Path
import shutil
from typing import Callable
from urllib import error, request
from urllib.parse import urlparse


STAGING_RUN_ID_KEYS = ("nightly_1", "nightly_2", "repair", "backfill", "recovery")
DEFAULT_DAGSTER_BINDING = "dagster://staging/moex-historical-cutover"
DEFAULT_REAL_BINDINGS = ("delta-ledger-cas://technical-route-run-ledger",)
_LOCAL_DAGSTER_HOSTS = {"localhost", "host.docker.internal"}
RUN_SUMMARY_QUERY = """
query RouteRunSummary($runId: ID!) {
  runOrError(runId: $runId) {
    __typename
    ... on Run {
      runId
      status
      jobName
      startTime
      endTime
      updateTime
      tags {
        key
        value
      }
    }
    ... on RunNotFoundError {
      message
    }
    ... on PythonError {
      message
      stack
    }
  }
}
"""


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_graphql_url(dagster_url: str) -> str:
    normalized = str(dagster_url).strip().rstrip("/")
    if not normalized:
        raise ValueError("`dagster_url` must be non-empty")
    if normalized.endswith("/graphql"):
        return normalized
    return normalized + "/graphql"


def validate_external_dagster_url(dagster_url: str) -> str:
    normalized = str(dagster_url).strip().rstrip("/")
    if not normalized:
        raise ValueError("`dagster_url` must be non-empty")

    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("`dagster_url` must be absolute http/https URL for staging-real evidence")

    host = parsed.hostname.strip().lower()
    if host in _LOCAL_DAGSTER_HOSTS:
        raise ValueError("`dagster_url` must reference external staging Dagster host, not local host")

    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return normalized

    if ip.is_loopback or ip.is_unspecified:
        raise ValueError("`dagster_url` must not use loopback or unspecified host for staging-real evidence")
    return normalized


def _normalize_binding_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: set[str] = set()
    for item in values or []:
        text = str(item).strip()
        if text:
            normalized.add(text)
    return sorted(normalized)


def _normalize_run_ids(run_ids: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key in STAGING_RUN_ID_KEYS:
        value = str(run_ids.get(key, "")).strip()
        if not value:
            raise ValueError(f"missing non-empty staging Dagster run id `{key}`")
        normalized[key] = value
    return normalized


def _normalize_artifact_paths(artifact_paths_by_mode: dict[str, Path | str]) -> dict[str, Path]:
    normalized: dict[str, Path] = {}
    for key in STAGING_RUN_ID_KEYS:
        raw = artifact_paths_by_mode.get(key)
        if raw is None:
            raise ValueError(f"missing artifact path for `{key}`")
        path = Path(str(raw)).resolve()
        if not path.exists():
            raise FileNotFoundError(f"artifact path does not exist for `{key}`: {path.as_posix()}")
        normalized[key] = path
    return normalized


def _copy_artifact(source_path: Path, *, destination_root: Path, mode: str) -> str:
    destination = destination_root / mode / source_path.name
    if source_path.is_dir():
        shutil.copytree(source_path, destination, dirs_exist_ok=True)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination)
    return destination.as_posix()


def _graphql_headers(*, token: str | None) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_dagster_run_summary(
    *,
    dagster_url: str,
    run_id: str,
    request_timeout_sec: float = 30.0,
    token: str | None = None,
) -> dict[str, object]:
    graphql_url = _normalize_graphql_url(dagster_url)
    payload = {
        "query": RUN_SUMMARY_QUERY,
        "variables": {"runId": run_id},
    }
    req = request.Request(
        graphql_url,
        data=json.dumps(payload).encode("utf-8"),
        headers=_graphql_headers(token=token),
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=request_timeout_sec) as response:
            body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ValueError(
            f"Dagster GraphQL request failed for run `{run_id}` with HTTP {exc.code}: {detail}"
        ) from exc
    except error.URLError as exc:
        raise ValueError(
            f"Dagster GraphQL request failed for run `{run_id}`: {exc.reason}"
        ) from exc

    document = json.loads(body)
    errors_payload = document.get("errors")
    if isinstance(errors_payload, list) and errors_payload:
        raise ValueError(f"Dagster GraphQL returned errors for run `{run_id}`: {errors_payload}")

    data = document.get("data")
    if not isinstance(data, dict):
        raise ValueError(f"Dagster GraphQL response is missing `data` for run `{run_id}`")
    run_or_error = data.get("runOrError")
    if not isinstance(run_or_error, dict):
        raise ValueError(f"Dagster GraphQL response is missing `runOrError` for run `{run_id}`")

    typename = str(run_or_error.get("__typename", "")).strip()
    if typename != "Run":
        message = str(run_or_error.get("message", "")).strip()
        raise ValueError(
            f"Dagster did not return a run payload for `{run_id}`; "
            f"got `{typename or 'EMPTY'}` ({message or 'no message'})"
        )

    tags_payload = run_or_error.get("tags")
    tags: dict[str, str] = {}
    if isinstance(tags_payload, list):
        for item in tags_payload:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key", "")).strip()
            value = str(item.get("value", "")).strip()
            if key:
                tags[key] = value

    return {
        "run_id": str(run_or_error.get("runId", "")).strip() or run_id,
        "status": str(run_or_error.get("status", "")).strip().upper(),
        "job_name": str(run_or_error.get("jobName", "")).strip(),
        "start_time": run_or_error.get("startTime"),
        "end_time": run_or_error.get("endTime"),
        "update_time": run_or_error.get("updateTime"),
        "tags": tags,
        "graphql_url": graphql_url,
    }


def build_route_staging_binding_report(
    *,
    dagster_url: str,
    output_dir: Path,
    run_ids: dict[str, str],
    artifact_paths_by_mode: dict[str, Path | str],
    expected_job_name: str = "moex_historical_cutover_job",
    dagster_binding: str = DEFAULT_DAGSTER_BINDING,
    extra_real_bindings: list[str] | tuple[str, ...] | None = None,
    orchestrator: str = "dagster-daemon",
    request_timeout_sec: float = 30.0,
    token: str | None = None,
    fetch_run_summary: Callable[[str], dict[str, object]] | None = None,
) -> dict[str, object]:
    dagster_url_normalized = validate_external_dagster_url(dagster_url)

    normalized_run_ids = _normalize_run_ids(run_ids)
    normalized_artifacts = _normalize_artifact_paths(artifact_paths_by_mode)
    normalized_job_name = str(expected_job_name).strip()
    if not normalized_job_name:
        raise ValueError("`expected_job_name` must be non-empty")
    normalized_orchestrator = str(orchestrator).strip().lower()
    if not normalized_orchestrator.startswith("dagster"):
        raise ValueError("`orchestrator` must confirm Dagster-owned execution")

    binding_value = str(dagster_binding).strip() or DEFAULT_DAGSTER_BINDING
    real_bindings = _normalize_binding_list(
        [binding_value, *DEFAULT_REAL_BINDINGS, *(extra_real_bindings or [])]
    )
    if not any(item.startswith("dagster://") for item in real_bindings):
        raise ValueError("`real_bindings` must include at least one `dagster://...` binding")

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    copied_artifact_root = output_dir / "staging-artifacts"

    run_fetcher = fetch_run_summary
    if run_fetcher is None:
        run_fetcher = lambda current_run_id: fetch_dagster_run_summary(
            dagster_url=dagster_url_normalized,
            run_id=current_run_id,
            request_timeout_sec=request_timeout_sec,
            token=token,
        )

    run_summaries: dict[str, dict[str, object]] = {}
    for mode in STAGING_RUN_ID_KEYS:
        run_id = normalized_run_ids[mode]
        summary = dict(run_fetcher(run_id))
        actual_job_name = str(summary.get("job_name", "")).strip()
        actual_status = str(summary.get("status", "")).strip().upper()
        if actual_job_name != normalized_job_name:
            raise ValueError(
                f"Dagster run `{run_id}` belongs to unexpected job `{actual_job_name or 'EMPTY'}`; "
                f"expected `{normalized_job_name}`"
            )
        if actual_status != "SUCCESS":
            raise ValueError(
                f"Dagster run `{run_id}` must be SUCCESS for staging-real evidence; "
                f"got `{actual_status or 'EMPTY'}`"
            )
        run_summaries[mode] = summary

    copied_artifact_paths: dict[str, str] = {}
    artifact_paths: list[str] = []
    for mode in STAGING_RUN_ID_KEYS:
        copied_path = _copy_artifact(
            normalized_artifacts[mode],
            destination_root=copied_artifact_root,
            mode=mode,
        )
        copied_artifact_paths[mode] = copied_path
        artifact_paths.append(copied_path)

    verification_payload = {
        "status": "PASS",
        "environment": "staging-real",
        "orchestrator": normalized_orchestrator,
        "dagster_url": dagster_url_normalized,
        "job_name": normalized_job_name,
        "run_summaries": run_summaries,
        "source_artifact_paths": {
            mode: normalized_artifacts[mode].as_posix() for mode in STAGING_RUN_ID_KEYS
        },
        "copied_artifact_paths": copied_artifact_paths,
        "real_bindings": real_bindings,
        "generated_at_utc": _utc_now_iso(),
    }
    verification_path = output_dir / "dagster-run-verification.json"
    verification_path.write_text(
        json.dumps(verification_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    report_payload = {
        "proof_class": "staging-real",
        "environment": "staging-real",
        "orchestrator": normalized_orchestrator,
        "dagster_url": dagster_url_normalized,
        "job_name": normalized_job_name,
        "run_ids": normalized_run_ids,
        "artifact_paths": artifact_paths,
        "real_bindings": real_bindings,
        "verification_artifact_path": verification_path.as_posix(),
        "generated_at_utc": _utc_now_iso(),
    }
    report_path = output_dir / "staging-binding-report.json"
    report_path.write_text(
        json.dumps(report_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return {
        "status": "PASS",
        "output_dir": output_dir.as_posix(),
        "staging_binding_report_path": report_path.as_posix(),
        "dagster_run_verification_path": verification_path.as_posix(),
        "report": report_payload,
        "verification": verification_payload,
    }
