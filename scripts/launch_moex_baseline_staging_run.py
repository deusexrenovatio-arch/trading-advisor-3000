from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.moex.runtime_instances import (  # noqa: E402
    PRODUCT_RUNTIME_ROLE,
    VERIFICATION_RUNTIME_ROLE,
    build_moex_baseline_run_config_for_instance,
    load_moex_runtime_instances_registry,
)


def _default_run_id() -> str:
    return "codex-manual-" + datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _safe_run_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.=-]+", "-", value.strip()).strip(".-")
    if not cleaned:
        raise ValueError("run id must contain at least one safe character")
    return cleaned


def _default_ingest_till_utc() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _validate_graphql_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Dagster GraphQL URL must use http/https and include a host")
    return urllib.parse.urlunsplit(parsed)


def _apply_graphql_port_env(url: str, *, port_env: str) -> str:
    env_name = port_env.strip()
    raw_port = os.environ.get(env_name, "").strip() if env_name else ""
    if not raw_port:
        return url
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise ValueError(f"{env_name} must be an integer port") from exc
    if port <= 0 or port > 65535:
        raise ValueError(f"{env_name} must be a valid TCP port")
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    if not host:
        return url
    host_part = f"[{host}]" if ":" in host and not host.startswith("[") else host
    return urllib.parse.urlunsplit(parsed._replace(netloc=f"{host_part}:{port}"))


def _resolve_graphql_url(raw_url: str, *, dagster: dict[str, object], override: bool) -> str:
    url = raw_url.strip()
    if not override:
        url = _apply_graphql_port_env(
            url,
            port_env=str(dagster.get("graphql_port_env", "") or ""),
        )
    return _validate_graphql_url(url)


def _graphql_json_response(body: str, *, source: str) -> dict[str, object]:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return {"errors": [{"message": f"{source} returned non-JSON response"}]}
    if not isinstance(payload, dict):
        return {"errors": [{"message": f"{source} response must be a JSON object"}]}
    return payload


def _post_graphql(
    *, url: str, query: str, variables: dict[str, object], timeout_sec: int
) -> dict[str, object]:
    graphql_url = _validate_graphql_url(url)
    request = urllib.request.Request(
        graphql_url,
        data=json.dumps({"query": query, "variables": variables}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            body = response.read().decode(errors="replace")
            return _graphql_json_response(body, source="Dagster GraphQL")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        return _graphql_json_response(body, source="Dagster GraphQL error")
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        return {"errors": [{"message": f"Dagster GraphQL request failed: {reason}"}]}


def _launch_mutation() -> str:
    return """
mutation Launch($executionParams: ExecutionParams!) {
  launchRun(executionParams: $executionParams) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on RunConfigValidationInvalid { errors { message reason } }
    ... on PipelineNotFoundError { message }
    ... on PythonError { message stack }
    ... on UnauthorizedError { message }
  }
}
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Launch a MOEX baseline Dagster run against a registered staging instance."
    )
    parser.add_argument(
        "--instance-id", default="", help="Defaults to the product-runtime staging instance."
    )
    parser.add_argument("--run-id", default="", help="Logical run id.")
    parser.add_argument("--ingest-till-utc", default="", help="Defaults to current UTC timestamp.")
    parser.add_argument(
        "--registry-path", default="", help="Optional runtime instance registry path."
    )
    parser.add_argument(
        "--graphql-url", default="", help="Override Dagster GraphQL URL from registry."
    )
    parser.add_argument("--timeout-sec", type=_positive_int, default=60)
    parser.add_argument(
        "--allow-product-runtime-write",
        action="store_true",
        help="Required when launching the product-runtime staging instance.",
    )
    parser.add_argument(
        "--allow-unseeded-verification",
        action="store_true",
        help="Required when launching a verification instance without a separate seed proof.",
    )
    args = parser.parse_args()

    registry = load_moex_runtime_instances_registry(
        Path(args.registry_path).resolve() if str(args.registry_path).strip() else None,
        repo_root=ROOT,
    )
    instance = (
        registry.instance(str(args.instance_id))
        if str(args.instance_id).strip()
        else registry.default_product_runtime()
    )
    if instance.role == PRODUCT_RUNTIME_ROLE and instance.mutation_policy.get(
        "require_explicit_product_write", False
    ):
        if not args.allow_product_runtime_write:
            raise SystemExit(
                "product runtime staging launch requires --allow-product-runtime-write"
            )
    if instance.role == VERIFICATION_RUNTIME_ROLE:
        require_seed = bool(instance.mutation_policy.get("require_seed", False))
        if require_seed and not args.allow_unseeded_verification:
            raise SystemExit(
                "verification staging launch requires seed proof or --allow-unseeded-verification"
            )

    dagster_owner = registry.dagster_owner(instance)
    dagster = dagster_owner.dagster
    graphql_url = str(args.graphql_url).strip() or str(dagster.get("graphql_url", "")).strip()
    if not graphql_url:
        raise SystemExit(
            f"runtime instance `{dagster_owner.instance_id}` does not declare dagster.graphql_url"
        )
    graphql_url = _resolve_graphql_url(
        graphql_url,
        dagster=dagster,
        override=bool(str(args.graphql_url).strip()),
    )
    run_id = _safe_run_id(str(args.run_id).strip() or _default_run_id())
    ingest_till_utc = str(args.ingest_till_utc).strip() or _default_ingest_till_utc()
    run_config = build_moex_baseline_run_config_for_instance(
        instance,
        run_id=run_id,
        ingest_till_utc=ingest_till_utc,
    )
    variables = {
        "executionParams": {
            "selector": {
                "repositoryLocationName": str(dagster["repository_location"]),
                "repositoryName": str(dagster["repository"]),
                "jobName": str(dagster["job"]),
            },
            "runConfigData": run_config,
            "mode": "default",
            "executionMetadata": {
                "tags": [
                    {"key": "ta3000/runtime_instance_id", "value": instance.instance_id},
                    {"key": "ta3000/runtime_instance_role", "value": instance.role},
                    {"key": "ta3000/logical_run_id", "value": run_id},
                    {"key": "ta3000/trigger", "value": "codex-registered-staging-launch"},
                ]
            },
        }
    }
    payload = _post_graphql(
        url=graphql_url,
        query=_launch_mutation(),
        variables=variables,
        timeout_sec=int(args.timeout_sec),
    )
    print(
        json.dumps(
            {
                "instance_id": instance.instance_id,
                "dagster_owner_instance_id": dagster_owner.instance_id,
                "logical_run_id": run_id,
                "ingest_till_utc": ingest_till_utc,
                "graphql_url": graphql_url,
                "payload": payload,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    launch = dict(dict(payload.get("data", {}) or {}).get("launchRun", {}) or {})
    if launch.get("__typename") != "LaunchRunSuccess":
        raise SystemExit("Dagster launch did not return LaunchRunSuccess")


if __name__ == "__main__":
    main()
