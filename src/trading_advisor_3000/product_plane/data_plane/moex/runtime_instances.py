from __future__ import annotations

# ruff: noqa: E501
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

MOEX_RUNTIME_INSTANCES_REGISTRY_ENV = "TA3000_MOEX_RUNTIME_INSTANCES_REGISTRY"
RUNTIME_INSTANCES_REGISTRY_RELATIVE_PATH = (
    Path("deployment") / "runtime-instances" / "moex-runtime-instances.v1.yaml"
)
PRODUCT_RUNTIME_ROLE = "product_runtime_staging"
VERIFICATION_RUNTIME_ROLE = "disposable_verification_staging"
_SAFE_RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.=-]+$")


@dataclass(frozen=True)
class MoexRuntimeInstance:
    instance_id: str
    role: str
    status: str
    description: str
    docker: dict[str, object]
    dagster: dict[str, object]
    paths: dict[str, object]
    launch_defaults: dict[str, object]
    mutation_policy: dict[str, object]
    lifecycle: dict[str, object]


@dataclass(frozen=True)
class MoexRuntimeInstancesRegistry:
    version: str
    default_product_runtime_instance: str
    default_verification_instance: str
    instances: dict[str, MoexRuntimeInstance]
    source_path: Path

    def instance(self, instance_id: str) -> MoexRuntimeInstance:
        resolved_id = instance_id.strip()
        if not resolved_id:
            raise ValueError("runtime instance id must not be empty")
        try:
            return self.instances[resolved_id]
        except KeyError as exc:
            known = ", ".join(sorted(self.instances))
            raise KeyError(
                f"unknown MOEX runtime instance `{resolved_id}`; known: {known}"
            ) from exc

    def default_product_runtime(self) -> MoexRuntimeInstance:
        return self.instance(self.default_product_runtime_instance)

    def default_verification_runtime(self) -> MoexRuntimeInstance:
        return self.instance(self.default_verification_instance)

    def dagster_owner(self, instance: MoexRuntimeInstance) -> MoexRuntimeInstance:
        owner_id = str(instance.dagster.get("use_instance", "")).strip()
        if owner_id:
            return self.instance(owner_id)
        return instance


def default_moex_runtime_instances_registry_path(*, repo_root: Path) -> Path:
    raw = os.environ.get(MOEX_RUNTIME_INSTANCES_REGISTRY_ENV, "").strip()
    if raw:
        path = Path(raw).expanduser()
        return path if path.is_absolute() else (repo_root / path).resolve()
    return (repo_root / RUNTIME_INSTANCES_REGISTRY_RELATIVE_PATH).resolve()


def _require_mapping(value: object, *, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"`{name}` must be a mapping")
    return {str(key): item for key, item in value.items()}


def _require_text(value: object, *, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"`{name}` must not be empty")
    return text


def validate_moex_runtime_run_id(value: object, *, name: str = "run_id") -> str:
    text = _require_text(value, name=name)
    if text in {".", ".."} or not _SAFE_RUN_ID_RE.fullmatch(text):
        raise ValueError(f"`{name}` must be a single safe path segment")
    return text


def _load_instance(instance_id: str, payload: object) -> MoexRuntimeInstance:
    data = _require_mapping(payload, name=f"instances.{instance_id}")
    return MoexRuntimeInstance(
        instance_id=instance_id,
        role=_require_text(data.get("role"), name=f"instances.{instance_id}.role"),
        status=_require_text(data.get("status"), name=f"instances.{instance_id}.status"),
        description=str(data.get("description", "")).strip(),
        docker=_require_mapping(data.get("docker", {}), name=f"instances.{instance_id}.docker"),
        dagster=_require_mapping(data.get("dagster", {}), name=f"instances.{instance_id}.dagster"),
        paths=_require_mapping(data.get("paths", {}), name=f"instances.{instance_id}.paths"),
        launch_defaults=_require_mapping(
            data.get("launch_defaults", {}),
            name=f"instances.{instance_id}.launch_defaults",
        ),
        mutation_policy=_require_mapping(
            data.get("mutation_policy", {}),
            name=f"instances.{instance_id}.mutation_policy",
        ),
        lifecycle=_require_mapping(
            data.get("lifecycle", {}), name=f"instances.{instance_id}.lifecycle"
        ),
    )


def load_moex_runtime_instances_registry(
    path: Path | None = None,
    *,
    repo_root: Path | None = None,
) -> MoexRuntimeInstancesRegistry:
    root = repo_root or Path(__file__).resolve().parents[5]
    registry_path = path or default_moex_runtime_instances_registry_path(repo_root=root)
    payload = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    data = _require_mapping(payload, name="runtime_instances_registry")
    instances_payload = _require_mapping(data.get("instances"), name="instances")
    instances = {
        instance_id: _load_instance(instance_id, instance_payload)
        for instance_id, instance_payload in instances_payload.items()
    }
    registry = MoexRuntimeInstancesRegistry(
        version=_require_text(data.get("version"), name="version"),
        default_product_runtime_instance=_require_text(
            data.get("default_product_runtime_instance"),
            name="default_product_runtime_instance",
        ),
        default_verification_instance=_require_text(
            data.get("default_verification_instance"),
            name="default_verification_instance",
        ),
        instances=instances,
        source_path=registry_path,
    )
    _validate_registry(registry)
    return registry


def _validate_registry(registry: MoexRuntimeInstancesRegistry) -> None:
    product = registry.default_product_runtime()
    verification = registry.default_verification_runtime()
    if product.role != PRODUCT_RUNTIME_ROLE:
        raise ValueError("default product runtime instance must have product_runtime_staging role")
    if verification.role != VERIFICATION_RUNTIME_ROLE:
        raise ValueError(
            "default verification instance must have disposable_verification_staging role"
        )
    for instance in registry.instances.values():
        if instance.status != "active":
            continue
        if not bool(instance.mutation_policy.get("manual_launch_allowed", False)):
            raise ValueError(
                f"active runtime instance `{instance.instance_id}` must declare manual launch policy"
            )
        if instance.role == PRODUCT_RUNTIME_ROLE:
            _require_text(
                instance.paths.get("data_root"), name=f"{instance.instance_id}.paths.data_root"
            )
            _require_text(
                instance.dagster.get("graphql_url"),
                name=f"{instance.instance_id}.dagster.graphql_url",
            )
            _require_text(instance.dagster.get("job"), name=f"{instance.instance_id}.dagster.job")
        if instance.role == VERIFICATION_RUNTIME_ROLE:
            _require_text(
                instance.paths.get("run_root_template"),
                name=f"{instance.instance_id}.paths.run_root_template",
            )
            if "{run_id}" not in str(instance.paths["run_root_template"]):
                raise ValueError(
                    f"verification instance `{instance.instance_id}` run_root_template must include {{run_id}}"
                )
            _require_text(
                instance.paths.get("seed_from_instance"),
                name=f"{instance.instance_id}.paths.seed_from_instance",
            )
            seed_instance = registry.instance(str(instance.paths["seed_from_instance"]))
            if seed_instance.role != PRODUCT_RUNTIME_ROLE:
                raise ValueError(
                    f"verification instance `{instance.instance_id}` seed_from_instance "
                    "must reference a product_runtime_staging instance"
                )


def render_moex_runtime_instance_paths(
    instance: MoexRuntimeInstance,
    *,
    run_id: str,
) -> dict[str, str]:
    if instance.role == PRODUCT_RUNTIME_ROLE:
        data_root = _require_text(
            instance.paths.get("data_root"), name=f"{instance.instance_id}.paths.data_root"
        )
    elif instance.role == VERIFICATION_RUNTIME_ROLE:
        resolved_run_id = validate_moex_runtime_run_id(run_id)
        data_root = _require_text(
            instance.paths.get("run_root_template"),
            name=f"{instance.instance_id}.paths.run_root_template",
        ).format(run_id=resolved_run_id)
    else:
        raise ValueError(f"unsupported MOEX runtime instance role: {instance.role}")
    canonical_root = f"{data_root}/canonical/moex/baseline-4y-current"
    return {
        "data_root": data_root,
        "raw_table": f"{data_root}/raw/moex/baseline-4y-current/raw_moex_history.delta",
        "canonical_root": canonical_root,
        "canonical_bars": f"{canonical_root}/canonical_bars.delta",
        "canonical_provenance": f"{canonical_root}/canonical_bar_provenance.delta",
        "canonical_session_calendar": f"{canonical_root}/canonical_session_calendar.delta",
        "canonical_roll_map": f"{canonical_root}/canonical_roll_map.delta",
        "evidence_root": f"{data_root}/moex-baseline-update",
    }


def build_moex_baseline_run_config_for_instance(
    instance: MoexRuntimeInstance,
    *,
    run_id: str,
    ingest_till_utc: str,
) -> dict[str, Any]:
    resolved_run_id = validate_moex_runtime_run_id(run_id)
    paths = render_moex_runtime_instance_paths(instance, run_id=resolved_run_id)
    defaults = instance.launch_defaults
    return {
        "ops": {
            "moex_baseline_update": {
                "config": {
                    "mapping_registry_path": _require_text(
                        defaults.get("mapping_registry_path"),
                        name=f"{instance.instance_id}.launch_defaults.mapping_registry_path",
                    ),
                    "universe_path": _require_text(
                        defaults.get("universe_path"),
                        name=f"{instance.instance_id}.launch_defaults.universe_path",
                    ),
                    "raw_table_path": paths["raw_table"],
                    "canonical_bars_path": paths["canonical_bars"],
                    "canonical_provenance_path": paths["canonical_provenance"],
                    "canonical_session_calendar_path": paths["canonical_session_calendar"],
                    "canonical_roll_map_path": paths["canonical_roll_map"],
                    "evidence_root": paths["evidence_root"],
                    "timeframes": _require_text(defaults.get("timeframes"), name="timeframes"),
                    "refresh_window_days": int(defaults.get("refresh_window_days", 7)),
                    "contract_discovery_lookback_days": int(
                        defaults.get("contract_discovery_lookback_days", 45)
                    ),
                    "contract_discovery_step_days": int(
                        defaults.get("contract_discovery_step_days", 14)
                    ),
                    "refresh_overlap_minutes": int(defaults.get("refresh_overlap_minutes", 180)),
                    "max_changed_window_days": int(defaults.get("max_changed_window_days", 10)),
                    "stability_lag_minutes": int(defaults.get("stability_lag_minutes", 20)),
                    "expand_contract_chain": bool(defaults.get("expand_contract_chain", True)),
                    "ingest_till_utc": _require_text(ingest_till_utc, name="ingest_till_utc"),
                    "run_id": resolved_run_id,
                }
            }
        }
    }
