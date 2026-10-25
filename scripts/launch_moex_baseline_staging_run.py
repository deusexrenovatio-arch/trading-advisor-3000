from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.moex.runtime_instances import (  # noqa: E402
    BRANCH_RUNTIME_ROLE,
    PRODUCT_RUNTIME_ROLE,
    STAGING_RUNTIME_ROLES,
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


def _endpoint_binding(url: str) -> tuple:
    parsed = urllib.parse.urlsplit(_validate_graphql_url(url))
    host = parsed.hostname or ""
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("guarded staging endpoint cannot contain credentials/query/fragment")
    if host == "localhost":
        host = "loopback"
    else:
        try:
            address = ipaddress.ip_address(host)
            if address.is_loopback or (
                getattr(address, "ipv4_mapped", None) and address.ipv4_mapped.is_loopback
            ):
                host = "loopback"
        except ValueError:
            pass
    return (
        parsed.scheme,
        host,
        parsed.port or (443 if parsed.scheme == "https" else 80),
        parsed.path.rstrip("/"),
    )


def _guarded_launch_plan(registry, instance, *, graphql_url, bypass_flags=False):
    """Client identity checks complement the server's immutable plan/admission."""
    from trading_advisor_3000.product_plane.common.staging_contracts import load_plan
    from trading_advisor_3000.product_plane.common.staging_execution import (
        IMMUTABLE_PLAN_PATH,
        PLAN_ENV,
    )

    expected_registry = ROOT / "deployment/runtime-instances/moex-runtime-instances.v1.yaml"
    if registry.source_path.resolve() != expected_registry.resolve() or bypass_flags:
        raise ValueError(
            "guarded staging requires the repository-owned registry and protected plan"
        )
    expected = str(instance.dagster["graphql_url"])
    if _endpoint_binding(graphql_url) != _endpoint_binding(expected):
        raise ValueError("guarded staging GraphQL endpoint override forbidden")
    product = registry.dagster_owner(registry.default_product_runtime())
    product_url = str(product.dagster["graphql_url"])
    production = {
        _endpoint_binding(product_url),
        _endpoint_binding(
            _resolve_graphql_url(
                product_url,
                dagster=product.dagster,
                override=False,
            )
        ),
    }
    if _endpoint_binding(expected) in production:
        raise ValueError("guarded staging cannot launch through a production endpoint")
    path = (
        IMMUTABLE_PLAN_PATH if IMMUTABLE_PLAN_PATH.is_file() else Path(os.environ.get(PLAN_ENV, ""))
    )
    configured_plan = os.environ.get(PLAN_ENV, "")
    if (
        IMMUTABLE_PLAN_PATH.is_file()
        and configured_plan
        and Path(configured_plan).resolve() != path.resolve()
    ):
        raise ValueError("immutable guarded plan cannot be overridden")
    if not path.is_absolute() or path.resolve().is_relative_to(ROOT.resolve()):
        raise ValueError("guarded staging requires a protected plan outside the candidate checkout")
    plan = load_plan(path)
    stage = "branch" if instance.role == BRANCH_RUNTIME_ROLE else "acceptance"
    if (
        plan["stage"] != stage
        or plan["data_root"].replace("\\", "/").rstrip("/")
        != str(instance.paths["data_root"]).rstrip("/")
        or plan["root_job"] != instance.dagster["job"]
    ):
        raise ValueError("guarded staging plan stage/root/job differs from its registered instance")
    if datetime.fromisoformat(plan["deadline_utc"].replace("Z", "+00:00")) <= datetime.now(UTC):
        raise ValueError("guarded staging plan has expired")
    if plan["require_scheduled"] or plan.get("mode") == "reference":
        raise ValueError("guarded acceptance/reference must start through its actual schedule")
    _guarded_native_run_config(plan)
    return plan


def _guarded_native_run_config(plan, *, run_id=None, ingest_till_utc=""):
    from trading_advisor_3000.product_plane.common.staging_contracts import digest

    source = plan.get("source", {}).get("native_schedule", {}).get("run_config")
    if (
        not isinstance(source, dict)
        or set(source) - {"ops", "execution", "resources", "loggers"}
        or not isinstance(source.get("ops"), dict)
        or set(source["ops"]) != {"moex_baseline_update"}
        or not isinstance(source["ops"]["moex_baseline_update"], dict)
        or set(source["ops"]["moex_baseline_update"]) != {"config"}
        or not isinstance(source["ops"]["moex_baseline_update"]["config"], dict)
    ):
        raise ValueError("guarded branch requires the captured native root RunConfig")
    result = json.loads(json.dumps(source, allow_nan=False))
    config = result["ops"]["moex_baseline_update"]["config"]
    parameters = plan.get("producer_parameters", {}).get("run_moex_baseline_update")
    if not isinstance(parameters, dict) or not parameters:
        raise ValueError("captured native RunConfig requires protected producer parameters")
    root = PurePosixPath(plan["data_root"])
    paths = (
        "raw_table_path",
        "canonical_bars_path",
        "canonical_provenance_path",
        "canonical_session_intervals_path",
        "canonical_session_calendar_path",
        "canonical_roll_map_path",
        "raw_economics_root",
        "canonical_economics_root",
        "evidence_root",
    )
    for key in (*paths, "daily_open_interest_path", "canonical_closed_session_dates_path"):
        if key not in paths and not config.get(key):
            continue
        value = config.get(key)
        path = PurePosixPath(value) if isinstance(value, str) else PurePosixPath()
        parameter_key = "evidence_dir" if key == "evidence_root" else key
        if (
            not root.is_absolute()
            or ".." in root.parts
            or "\\" in str(root)
            or not path.is_absolute()
            or not path.is_relative_to(root)
            or ".." in path.parts
            or "\\" in str(path)
            or str(path) != value
            or parameter_key not in parameters
        ):
            raise ValueError("captured native RunConfig path escapes its protected POSIX data root")
    for key, value in config.items():
        parameter_key = "evidence_dir" if key == "evidence_root" else key
        if parameter_key not in parameters:
            continue
        if key in {"timeframes", "cf_catch_up_timeframes"} and isinstance(value, str):
            value = sorted({item.strip() for item in value.split(",") if item.strip()})
        if digest(value) != digest(parameters[parameter_key]):
            raise ValueError("captured native RunConfig differs from protected producer parameters")
    cutoff = config.get("ingest_till_utc")
    if (
        not isinstance(cutoff, str)
        or not isinstance(config.get("run_id"), str)
        or not config["run_id"]
    ):
        raise ValueError("captured native RunConfig requires its fixed cutoff and logical run id")
    instant = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
    start = datetime.fromisoformat(plan["scope"]["start_utc"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(plan["scope"]["end_utc"].replace("Z", "+00:00"))
    if instant.tzinfo is None or not start < instant <= end:
        raise ValueError("captured native cutoff exceeds the protected scope")
    if ingest_till_utc and ingest_till_utc != cutoff:
        raise ValueError("guarded branch ingest cutoff override differs from its protected value")
    if run_id is not None:
        config["run_id"] = run_id
    return result


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
    guarded_plan = None
    if instance.role in STAGING_RUNTIME_ROLES:
        guarded_plan = _guarded_launch_plan(
            registry,
            instance,
            graphql_url=graphql_url,
            bypass_flags=args.allow_product_runtime_write or args.allow_unseeded_verification,
        )
        run_config = _guarded_native_run_config(
            guarded_plan, run_id=run_id, ingest_till_utc=str(args.ingest_till_utc).strip()
        )
        ingest_till_utc = run_config["ops"]["moex_baseline_update"]["config"]["ingest_till_utc"]
    else:
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
    if guarded_plan is not None:
        from trading_advisor_3000.product_plane.common.staging_contracts import digest

        variables["executionParams"]["executionMetadata"]["tags"].extend(
            [
                {"key": "ta3000/staging_plan_sha256", "value": digest(guarded_plan)},
                {
                    "key": "ta3000/candidate_head_sha",
                    "value": guarded_plan["candidate"]["head_sha"],
                },
            ]
        )
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
