"""Fail-closed staging execution; Dagster run storage owns durable reservations.

The protected controller owns plans, root markers and quota storage. Candidate
code cannot turn its run tags into an allowance. Unmarked ordinary runtimes are
unchanged; a marked staging root always requires an actual persistent run.
"""

from __future__ import annotations

import functools
import json
import math
import os
import time
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

PLAN_ENV = "TA3000_STAGING_PLAN"
GRANT_ENV = "TA3000_STAGING_GRANT"
ROOT_MARKER = ".ta3000-staging.json"
IMMUTABLE_PLAN_PATH = Path("/staging-control/plan.json")
IMMUTABLE_PERMIT_PATH = Path("/staging-control/execution-permit.json")
IMMUTABLE_HOST_LOCK_PATH = Path("/staging-host-state/heavy-compute.lock")
CONTROL_JOB = "ta3000_staging_control"
RUNTIME_TAG = "dagster/max_runtime_seconds"
_ACTIVE: ContextVar[Any] = ContextVar("staging_execution", default=None)


def digest(value):
    from .staging_contracts import digest as encode

    return encode(value)


def load_plan(path):
    from .staging_contracts import load_plan as load

    try:
        return load(path)
    except OSError as exc:
        raise StagingExecutionError("protected staging plan unavailable") from exc


def validate_document(kind, value):
    from .staging_contracts import validate_document as validate

    return validate(kind, value)


class StagingExecutionError(ValueError):
    """Execution lacks a protected, bounded staging allowance."""


@dataclass
class _Scope:
    plan: dict
    instance: Any
    run: Any
    reservation: Any
    grant_entry: str | None = None
    grant_used: bool = False


def _within(path: Path, root: Path) -> bool:
    return path.resolve().is_relative_to(root.resolve())


def _stamp(value: str) -> float:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise StagingExecutionError("staging scope timestamps require an explicit timezone")
    return parsed.astimezone(UTC).timestamp()


def _deadline(plan: dict, reservation=None) -> None:
    limit = _stamp(plan["deadline_utc"])
    if reservation is not None:
        limit = min(
            limit,
            float(reservation.run_config["started_at"]) + plan["max_runtime_seconds"],
        )
    if time.time() >= limit:
        raise StagingExecutionError("staging execution deadline exceeded")


def _run_started_at(instance, run_id):
    started = instance.get_run_stats(run_id).start_time
    if started is None:
        record = instance.get_run_record_by_id(run_id)
        started = record.start_time if record else None
    if type(started) not in {int, float} or not math.isfinite(started) or started <= 0:
        raise StagingExecutionError("staging compute owner has no actual run-start event")
    return float(started)


def _run_admitted_at(instance, run_id):
    from dagster._core.events import DagsterEventType

    records = instance.get_records_for_run(
        run_id, of_type=DagsterEventType.PIPELINE_STARTING, limit=1, ascending=True
    ).records
    if records:
        return float(records[0].event_log_entry.timestamp)
    return _run_started_at(instance, run_id)


def execution_permitted(plan):
    """A short host-refreshed lease fences dequeue, native work and publication."""
    try:
        permit = json.loads(IMMUTABLE_PERMIT_PATH.read_text(encoding="utf-8"))
        lock = json.loads(IMMUTABLE_HOST_LOCK_PATH.read_text(encoding="utf-8"))
        issued, deadline = _stamp(permit["issued_at_utc"]), _stamp(permit["deadline_utc"])
        nonce = permit["lease_nonce"]
        return (
            permit["schema_version"] == "staging_execution_permit.v1"
            and lock["schema_version"] == "staging_host_lock.v1"
            and permit["plan_sha256"] == lock["plan_sha256"] == digest(plan)
            and isinstance(nonce, str)
            and len(nonce) == 32
            and all(char in "0123456789abcdef" for char in nonce)
            and lock["lease_nonce"] == nonce
            and isinstance(lock["project"], str)
            and bool(lock["project"])
            and type(lock["pid"]) is int
            and lock["pid"] > 0
            and issued <= time.time() < deadline <= _stamp(plan["deadline_utc"])
            and 0 < deadline - issued <= 30
        )
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _require_permit(plan):
    if not execution_permitted(plan):
        raise StagingExecutionError("protected host execution permit missing, expired or changed")


def _marker_for(path: Path) -> Path | None:
    for parent in (path.resolve(), *path.resolve().parents):
        marker = parent / ROOT_MARKER
        if marker.is_file():
            return marker
    return None


def _plan(output_path: Path | str | None = None) -> dict | None:
    env_path = os.environ.get(PLAN_ENV, "").strip()
    if IMMUTABLE_PLAN_PATH.is_file():
        if env_path and Path(env_path).resolve() != IMMUTABLE_PLAN_PATH.resolve():
            raise StagingExecutionError("immutable staging plan cannot be overridden")
        env_path = str(IMMUTABLE_PLAN_PATH)
    marker_path = _marker_for(Path(output_path)) if output_path is not None else None
    marker = json.loads(marker_path.read_text(encoding="utf-8")) if marker_path else None
    if not env_path and marker is None:
        return None
    path = Path(env_path or marker.get("plan_path", ""))
    if not path.is_absolute():
        raise StagingExecutionError("staging requires an absolute protected plan path")
    plan = load_plan(path)
    root = Path(plan["data_root"]).resolve()
    if marker is None:
        marker_path = root / ROOT_MARKER
        if not marker_path.is_file():
            raise StagingExecutionError("protected staging root marker missing")
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    if (
        marker.get("schema_version") != "staging_root.v1"
        or Path(marker.get("plan_path", "")).resolve() != path.resolve()
        or marker.get("plan_digest") != digest(plan)
        or marker_path.parent.resolve() != root
    ):
        raise StagingExecutionError("staging root marker/plan binding changed")
    if output_path is not None and not _within(Path(output_path), root):
        raise StagingExecutionError("staging output escapes protected data root")
    quota = Path(plan["quota_home"]).resolve()
    runtime_home = os.environ.get("DAGSTER_HOME", "")
    if (
        _within(quota, root)
        or _within(root, quota)
        or (runtime_home and _within(quota, Path(runtime_home)))
    ):
        raise StagingExecutionError("quota storage must outlive staging data and runtime HOME")
    if not quota.is_dir():
        raise StagingExecutionError("protected persistent quota home missing")
    _deadline(plan)
    return plan


@contextmanager
def _quota(plan, *, host_quota_home=None):
    root = Path(host_quota_home if host_quota_home is not None else plan["quota_home"])
    if host_quota_home is not None and not root.is_absolute():
        raise StagingExecutionError("host quota transport requires an explicit absolute path")
    config = json.loads((root / "storage.json").read_text(encoding="utf-8"))
    if (
        set(config)
        != {"schema_version", "backend", "postgres_url", "expected_database", "expected_role"}
        or config["schema_version"] != "staging_quota_storage.v1"
        or config["backend"] != "postgres"
    ):
        raise StagingExecutionError(
            "protected quota requires native append-only PostgreSQL storage"
        )
    from dagster_postgres.run_storage import PostgresRunStorage

    class PublicQuotaStorage(PostgresRunStorage):
        @contextmanager
        def connect(self):
            with super().connect() as conn:
                conn.exec_driver_sql("SET search_path TO pg_catalog, public, pg_temp")
                bound = conn.exec_driver_sql(
                    "SELECT 'runs'::regclass::oid='public.runs'::regclass::oid "
                    "AND 'run_tags'::regclass::oid='public.run_tags'::regclass::oid"
                ).scalar()
                if bound is not True:
                    raise StagingExecutionError("quota relations escaped their protected schema")
                yield conn

    storage = PublicQuotaStorage(
        _quota_url(config, host_transport=host_quota_home is not None),
        should_autocreate_tables=False,
    )
    try:
        with storage.connect() as conn:
            database, role = conn.exec_driver_sql("SELECT current_database(),current_user").one()
            if database != config["expected_database"] or role != config["expected_role"]:
                raise StagingExecutionError("quota database/role binding changed")
            privileged = conn.exec_driver_sql(
                "SELECT r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication "
                "OR r.rolbypassrls "
                "OR has_database_privilege(current_user,current_database(),'CREATE') "
                "OR has_database_privilege(current_user,current_database(),'TEMPORARY') "
                "OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) "
                "OR EXISTS(SELECT 1 FROM pg_database "
                "WHERE datname=current_database() AND datdba=r.oid) "
                "OR EXISTS(SELECT 1 FROM pg_namespace "
                "WHERE left(nspname,3)<>'pg_' AND nspowner=r.oid) "
                "OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='public' AND c.relowner=r.oid) "
                "OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='public' AND CASE WHEN c.relkind='S' "
                "THEN has_sequence_privilege(c.oid,'UPDATE') ELSE false END) "
                "OR EXISTS(SELECT 1 FROM pg_namespace WHERE left(nspname,3)<>'pg_' "
                "AND nspname<>'information_schema' AND has_schema_privilege(oid,'CREATE')) "
                "FROM pg_roles r WHERE r.rolname=current_user"
            ).scalar()
            permitted = conn.exec_driver_sql(
                "SELECT bool_and(has_table_privilege(current_user,t,'SELECT') "
                "AND has_table_privilege(current_user,t,'INSERT') "
                "AND NOT has_table_privilege(current_user,t,"
                "'UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') "
                "AND NOT has_any_column_privilege(current_user,t,'UPDATE,REFERENCES')) "
                "FROM unnest(ARRAY['public.runs','public.run_tags']) AS t"
            ).scalar()
            if privileged is not False or permitted is not True:
                raise StagingExecutionError(
                    "quota database role must be append-only and unprivileged"
                )

        class Store:
            add_run = storage.add_run
            get_runs = storage.get_runs

            @staticmethod
            def get_run_by_id(run_id):
                from dagster import RunsFilter

                runs = storage.get_runs(filters=RunsFilter(run_ids=[run_id]), limit=1)
                return runs[0] if runs else None

        yield Store()
    finally:
        storage.dispose()


def _quota_url(config, *, host_transport=False):
    if not host_transport or os.name != "nt":
        return config["postgres_url"]
    from sqlalchemy.engine import make_url

    url = make_url(config["postgres_url"])
    if (
        url.host != "host.docker.internal"
        or url.username != config["expected_role"]
        or url.database != config["expected_database"]
    ):
        raise StagingExecutionError("host quota transport database/role/endpoint binding changed")
    return url.set(host="127.0.0.1").render_as_string(hide_password=False)


def _key(*parts):
    return digest(list(parts))[:32]


def _add(instance, key, payload, *, tags=None):
    from dagster._core.errors import DagsterRunAlreadyExists
    from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus

    try:
        return instance.add_run(
            DagsterRun(
                job_name=CONTROL_JOB,
                run_id=key,
                status=DagsterRunStatus.SUCCESS,
                run_config=payload,
                tags={"ta3000/staging_control": "reservation", **(tags or {})},
            )
        )
    except DagsterRunAlreadyExists:
        return None
    except Exception:
        # A connection can fail after commit. Never silently reserve another slot.
        record = instance.get_run_by_id(key)
        if record is not None and record.run_config == payload:
            return record
        raise


def _operator_keys(plan_path):
    value = json.loads(
        (Path(plan_path).parent / "operator-keyring.json").read_text(encoding="utf-8")
    )
    if not isinstance(value.get("keys"), dict) or not value["keys"]:
        raise StagingExecutionError("protected operator keyring is unavailable")
    return value["keys"]


def _control_plan_path(plan):
    path = os.environ.get(PLAN_ENV, "")
    if not path:
        marker = Path(plan["data_root"]) / ROOT_MARKER
        path = json.loads(marker.read_text(encoding="utf-8"))["plan_path"]
    return Path(path)


def _increased(previous, limits):
    return (
        set(limits) == {"small", "acceptance", "rebuild"}
        and all(type(limits[key]) is int and limits[key] >= previous[key] for key in previous)
        and any(limits[key] > previous[key] for key in previous)
    )


def _current_policy(instance, plan, *, plan_path=None):
    key = _key("staging-policy-v1", plan["task_id"])
    payload = {"task_id": plan["task_id"], "limits": plan["limits"]}
    _add(instance, key, payload)
    current = dict(instance.get_run_by_id(key).run_config)
    while True:
        approval = instance.get_run_by_id(
            _key("staging-budget-extension-v1", plan["task_id"], digest(current))
        )
        if approval is None:
            return current
        from .staging_contracts import verify_budget_approval

        approved = verify_budget_approval(
            approval.run_config["approval"],
            _operator_keys(plan_path or _control_plan_path(plan)),
        )
        if (
            approved["task_id"] != plan["task_id"]
            or approved["previous_policy_sha256"] != digest(current)
            or not _increased(current["limits"], approved["new_limits"])
        ):
            raise StagingExecutionError("invalid signed quota extension chain")
        current = {"task_id": plan["task_id"], "limits": approved["new_limits"]}


def _policy(instance, plan):
    if _current_policy(instance, plan)["limits"] != plan["limits"]:
        raise StagingExecutionError("task quota policy changed; protected reauthorization required")


def authorize_budget_extension(plan_path, *, authorization_id, reason, new_limits, quota_home=None):
    """Protected host operation; never called automatically by staging execution.

    The host-resolved plan points to the existing quota store. Its sibling
    operator-keyring.json is protected/readonly in candidate containers. The
    operator's raw Ed25519 private key file is never mounted into those containers.
    This operation appends approval records; it changes neither the plan nor spent
    slots. Update the protected plan/marker separately after recording approval.
    """
    from .staging_contracts import sign_budget_approval, verify_budget_approval

    plan_path = Path(plan_path).resolve()
    plan = load_plan(plan_path)
    if not str(authorization_id).strip() or not str(reason).strip():
        raise StagingExecutionError("budget extension requires authorization id and reason")
    keys = _operator_keys(plan_path)
    quota_root = Path(quota_home or plan["quota_home"])
    with _quota(plan, host_quota_home=quota_home) as quota:
        base = quota.get_run_by_id(_key("staging-policy-v1", plan["task_id"]))
        if base is None:
            raise StagingExecutionError("budget extension requires the existing task policy")
        auth_key = _key("staging-budget-authorization-v1", authorization_id)
        existing = quota.get_run_by_id(auth_key)
        if existing is not None:
            envelope = existing.run_config["approval"]
            approval = verify_budget_approval(envelope, keys)
            if (
                approval["task_id"] != plan["task_id"]
                or approval["authorization_id"] != authorization_id
                or approval["new_limits"] != new_limits
                or approval["reason"] != reason
            ):
                raise StagingExecutionError("budget authorization id already used differently")
        else:
            current = _current_policy(quota, plan, plan_path=plan_path)
            if not _increased(current["limits"], new_limits):
                raise StagingExecutionError("quota extension must increase limits monotonically")
            key_path = Path(os.environ.get("TA3000_STAGING_OPERATOR_PRIVATE_KEY", ""))
            key_id = os.environ.get("TA3000_STAGING_OPERATOR_KEY_ID", "")
            if (
                not key_path.is_absolute()
                or _within(key_path, Path(plan["data_root"]))
                or _within(key_path, quota_root)
            ):
                raise StagingExecutionError("host operator private key is unavailable")
            envelope = sign_budget_approval(
                {
                    "schema_version": "staging_budget_approval.v1",
                    "task_id": plan["task_id"],
                    "authorization_id": authorization_id,
                    "reason": reason,
                    "previous_policy_sha256": digest(current),
                    "new_limits": new_limits,
                    "issued_at_utc": datetime.now(UTC).isoformat(),
                },
                key_path.read_bytes(),
                key_id,
            )
            approval = verify_budget_approval(envelope, keys)
            _add(quota, auth_key, {"approval": envelope})
            stored = quota.get_run_by_id(auth_key).run_config["approval"]
            if stored != envelope:
                raise StagingExecutionError("budget authorization id collision")
        chain_key = _key(
            "staging-budget-extension-v1", plan["task_id"], approval["previous_policy_sha256"]
        )
        _add(quota, chain_key, {"approval": envelope})
        if quota.get_run_by_id(chain_key).run_config["approval"] != envelope:
            raise StagingExecutionError("quota extension lost a concurrent policy change")
        return envelope


def _slots(instance, *, prefix, count, payload, identity):
    for slot in range(count):
        key = _key(*prefix, slot)
        existing = instance.get_run_by_id(key)
        if existing is None:
            existing = _add(instance, key, payload)
            if existing is not None:
                return existing
            existing = instance.get_run_by_id(key)
        if identity is not None and existing.run_config.get("identity") == identity:
            if existing.run_config.get("plan_sha256") != payload.get("plan_sha256"):
                raise StagingExecutionError("existing reservation belongs to another plan")
            return existing
    raise StagingExecutionError("staging compute budget exhausted")


def _reference_mode(plan):
    return plan.get("mode", "candidate") == "reference"


def _reference_authority(plan, quota, *, allow_stopped=False, plan_path=None):
    from dagster import RunsFilter

    from .staging_contracts import verify_reference_authorization

    records = quota.get_runs(
        filters=RunsFilter(tags={"ta3000/reference_plan": digest(plan)}), limit=1
    )
    if not records:
        raise StagingExecutionError("reference mode requires protected host activation")
    record = records[0]
    authorization = verify_reference_authorization(
        record.run_config["authorization"], _operator_keys(plan_path or _control_plan_path(plan))
    )
    if (
        authorization["task_id"] != plan["task_id"]
        or authorization["plan_sha256"] != digest(plan)
        or authorization["main_sha"] != plan["reference"]["main_sha"]
        or authorization["receipt_sha256"] != digest(plan["reference"]["receipt"])
        or record.run_id
        != _key("staging-reference-auth-v1", plan["task_id"], authorization["authorization_id"])
    ):
        raise StagingExecutionError("reference activation binding changed")
    if not allow_stopped and quota.get_run_by_id(_key("staging-reference-stop-v1", record.run_id)):
        raise StagingExecutionError("reference halted; protected diagnosis/reactivation required")
    return record


def authorize_reference_mode(
    plan_path,
    *,
    authorization_id,
    reason,
    repository_root,
    runtime_image_digest,
    main_ref="refs/remotes/origin/main",
    quota_home=None,
):
    """Host-only activation of an already accepted, actual main tree and artifact."""
    import subprocess

    from .staging_contracts import (
        sign_reference_authorization,
        verify_reference_authorization,
    )
    from .staging_route_proof import verify_acceptance_route

    path = Path(plan_path).resolve()
    plan = load_plan(path)
    if not _reference_mode(plan) or main_ref not in {"refs/remotes/origin/main", "refs/heads/main"}:
        raise StagingExecutionError("reference activation requires the actual protected main ref")
    _deadline(plan)
    quota_root = Path(quota_home or plan["quota_home"])
    keys = _operator_keys(path)
    with _quota(plan, host_quota_home=quota_home) as quota:
        try:
            accepted = verify_acceptance_route(plan["reference"]["receipt"], keys)
        except ValueError:
            # Reactivating unchanged main reuses its existing signed historical admission.
            _reference_authority(plan, quota, allow_stopped=True, plan_path=path)
            issued = plan["reference"]["receipt"]["payload"]["issued_at_utc"]
            accepted = verify_acceptance_route(
                plan["reference"]["receipt"], keys, now=datetime.fromtimestamp(_stamp(issued), UTC)
            )
        for job, spec in plan["jobs"].items():
            produced = accepted["runs"].get(job, {})
            upstream = accepted["runs"].get(spec["upstream_job"], {})
            if (
                produced.get("status") != "SUCCESS"
                or not produced.get("run_id")
                or (
                    spec["upstream_job"] is not None
                    and produced.get("upstream_run_id") != upstream.get("run_id")
                )
            ):
                raise StagingExecutionError("reference graph differs from the accepted route")

        def git(*args):
            return subprocess.run(
                ["git", *args], cwd=repository_root, check=True, capture_output=True, text=True
            ).stdout.strip()

        main = git("rev-parse", "--verify", "--end-of-options", f"{main_ref}^{{commit}}")
        tree = git("rev-parse", "--verify", "--end-of-options", f"{main}^{{tree}}")
        candidate, original = plan["candidate"], accepted["candidate"]
        if (
            main != plan["reference"]["main_sha"]
            or tree != candidate["tree_sha"]
            or tree != original["tree_sha"]
            or any(
                candidate[field] != original[field]
                for field in ("executable_sha256", "config_sha256", "image_digest")
            )
            or runtime_image_digest != candidate["image_digest"]
            or git("diff", "--name-only", main, "--")
        ):
            raise StagingExecutionError(
                "reference main/tree/deployed artifact differs from acceptance"
            )
        for name in git("ls-files", "--others", "--exclude-standard").splitlines():
            if name.startswith(
                ("src/", "scripts/", "configs/", "deployment/", "requirements")
            ) or name in {
                "pyproject.toml",
                "uv.lock",
                "poetry.lock",
                "setup.py",
                "setup.cfg",
            }:
                raise StagingExecutionError("reference contains undeclared executable files")
        key_path = Path(os.environ.get("TA3000_STAGING_OPERATOR_PRIVATE_KEY", ""))
        if (
            not key_path.is_absolute()
            or _within(key_path, Path(plan["data_root"]))
            or _within(key_path, quota_root)
        ):
            raise StagingExecutionError("host reference operator private key unavailable")
        envelope = sign_reference_authorization(
            {
                "schema_version": "staging_reference_authorization.v1",
                "task_id": plan["task_id"],
                "plan_sha256": digest(plan),
                "main_sha": main,
                "receipt_sha256": digest(plan["reference"]["receipt"]),
                "authorization_id": authorization_id,
                "reason": reason,
                "issued_at_utc": datetime.now(UTC).isoformat(),
            },
            key_path.read_bytes(),
            os.environ.get("TA3000_STAGING_OPERATOR_KEY_ID", ""),
        )
        verify_reference_authorization(envelope, keys)
        key = _key("staging-reference-auth-v1", plan["task_id"], authorization_id)
        existing = quota.get_run_by_id(key)
        if existing is not None:
            previous = verify_reference_authorization(existing.run_config["authorization"], keys)
            if any(
                previous[field] != envelope["payload"][field]
                for field in previous
                if field != "issued_at_utc"
            ):
                raise StagingExecutionError("reference authorization id already used differently")
            return existing.run_config["authorization"]
        _add(quota, key, {"authorization": envelope}, tags={"ta3000/reference_plan": digest(plan)})
        if quota.get_run_by_id(key).run_config["authorization"] != envelope:
            raise StagingExecutionError("reference activation lost concurrent authorization")
        return envelope


def halt_reference_graph(plan_path, *, reason, root_run_id="", quota_home=None):
    plan = load_plan(Path(plan_path))
    with _quota(plan, host_quota_home=quota_home) as quota:
        authority = _reference_authority(plan, quota, allow_stopped=True, plan_path=Path(plan_path))
        _add(
            quota,
            _key("staging-reference-stop-v1", authority.run_id),
            {
                "reason": str(reason),
                "root_run_id": root_run_id,
                "stopped_at": time.time(),
            },
        )


def _scheduled_time(plan, run):
    from dagster._core.storage.tags import SCHEDULE_NAME_TAG, SCHEDULED_EXECUTION_TIME_TAG

    if run.tags.get(SCHEDULE_NAME_TAG) != "moex_baseline_daily_update_schedule" or not run.tags.get(
        SCHEDULED_EXECUTION_TIME_TAG
    ):
        raise StagingExecutionError("acceptance requires the scheduled root route")
    scheduled = _stamp(run.tags[SCHEDULED_EXECUTION_TIME_TAG])
    if scheduled > time.time():
        raise StagingExecutionError("future schedule tags cannot authorize execution")
    expected = plan.get("source", {}).get("native_schedule", {}).get("logical_scheduled_at_utc")
    if expected and not _reference_mode(plan) and scheduled != _stamp(expected):
        raise StagingExecutionError("run belongs to another protected schedule instant")
    return scheduled


def _reference_tick(plan, instance, run):
    from dagster._core.scheduler.instigation import InstigatorType

    scheduled = _scheduled_time(plan, run)
    origin = run.remote_job_origin
    for state in instance.all_instigator_state(instigator_type=InstigatorType.SCHEDULE):
        if (
            state.name != "moex_baseline_daily_update_schedule"
            or origin is None
            or state.repository_origin_id != origin.repository_origin.get_id()
        ):
            continue
        for tick in instance.get_ticks(state.instigator_origin_id, state.selector_id, limit=20):
            if (
                run.run_id in tick.run_ids
                and tick.status.value == "SUCCESS"
                and tick.timestamp == scheduled
            ):
                return tick
    raise StagingExecutionError("root lacks an actual successful persisted schedule tick")


def _reference_root(plan, instance, run, *, reserve):
    from zoneinfo import ZoneInfo

    from dagster import RunsFilter

    with _quota(plan) as quota:
        authority = _reference_authority(plan, quota)
        if run.tags.get("ta3000/reference_authorization", authority.run_id) != authority.run_id:
            raise StagingExecutionError("reference run belongs to another activation")
        instance.add_run_tags(run.run_id, {"ta3000/reference_authorization": authority.run_id})
        failures = instance.get_runs(
            filters=RunsFilter(
                tags={
                    "ta3000/reference_authorization": authority.run_id,
                }
            )
        )
        if any(item.status.value in {"FAILURE", "CANCELED", "CANCELING"} for item in failures):
            _add(
                quota,
                _key("staging-reference-stop-v1", authority.run_id),
                {"reason": "failed/cancelled graph"},
            )
            raise StagingExecutionError("reference halted after a failed/cancelled graph")
        if not reserve:
            return run
        tick = _reference_tick(plan, instance, run)
        day = datetime.fromtimestamp(tick.timestamp, ZoneInfo(plan["reference"]["timezone"])).date()
        record = _slots(
            quota,
            prefix=("staging-reference-day-v1", str(Path(plan["data_root"]).resolve()), str(day)),
            count=1,
            identity=run.run_id,
            payload={
                "identity": run.run_id,
                "root_run_id": run.run_id,
                "plan_sha256": digest(plan),
                "started_at": _run_admitted_at(instance, run.run_id),
                "reference_authorization": authority.run_id,
                "tick_id": str(tick.tick_id),
                "scheduled_utc": tick.timestamp,
                "local_day": str(day),
            },
        )
        if any(
            record.run_config.get(key) != expected
            for key, expected in {
                "root_run_id": run.run_id,
                "started_at": _run_admitted_at(instance, run.run_id),
                "reference_authorization": authority.run_id,
                "tick_id": str(tick.tick_id),
                "scheduled_utc": tick.timestamp,
                "local_day": str(day),
            }.items()
        ):
            raise StagingExecutionError(
                "reference quota reservation differs from its actual tick/run"
            )
        instance.add_run_tags(run.run_id, {"ta3000/reference_authorization": authority.run_id})
        return record


def _root_reservation(plan, run, instance):
    with _quota(plan) as quota:
        _policy(quota, plan)
        started = _run_admitted_at(instance, run.run_id)
        payload = {
            "identity": run.run_id,
            "plan_sha256": digest(plan),
            "task_id": plan["task_id"],
            "started_at": started,
            "root_run_id": run.run_id,
        }
        record = _slots(
            quota,
            prefix=("staging-root-v1", plan["task_id"], plan["attempt_kind"]),
            count=plan["limits"][plan["attempt_kind"]],
            identity=run.run_id,
            payload=payload,
        )
        if record.run_config != payload:
            raise StagingExecutionError("root quota reservation differs from the actual run start")
        return record


def _validate_root_recipe(plan, run_config):
    native = plan.get("source", {}).get("native_schedule", {})
    if _reference_mode(plan) or "run_config" not in native:
        return

    def normalized(value):
        try:
            value = json.loads(json.dumps(value, allow_nan=False))
        except (TypeError, ValueError) as exc:
            raise StagingExecutionError("protected root recipe requires opaque JSON") from exc
        if not isinstance(value, dict):
            raise StagingExecutionError("protected root recipe requires a JSON object")
        ops = value.get("ops")
        if isinstance(ops, dict):
            for op in ops.values():
                if isinstance(op, dict) and isinstance(op.get("config"), dict):
                    op["config"].pop("run_id", None)
        return value

    if digest(normalized(run_config)) != digest(normalized(native["run_config"])):
        raise StagingExecutionError(
            "actual root RunConfig differs from its protected native recipe"
        )


def _bind_run(plan, instance, run, seen=None, *, reserve=True):
    if not instance.is_persistent or run is None:
        raise StagingExecutionError("staging requires an actual persistent Dagster run")
    if run.status.value in {"FAILURE", "CANCELED", "CANCELING"}:
        raise StagingExecutionError("failed/cancelled run cannot execute staging work")
    seen = set(seen or ())
    if run.run_id in seen or len(seen) > len(plan["jobs"]):
        raise StagingExecutionError("invalid staging run ancestry")
    seen.add(run.run_id)
    if run.job_name not in plan["jobs"]:
        raise StagingExecutionError("job outside protected staging graph")
    if run.parent_run_id:
        raise StagingExecutionError("retry/resume requires a new bounded root attempt")
    spec = plan["jobs"][run.job_name]
    upstream = run.tags.get("ta3000/upstream_run_id")
    if run.job_name == plan["root_job"]:
        if upstream or spec["upstream_job"] is not None:
            raise StagingExecutionError("root job cannot borrow upstream admission")
        _validate_root_recipe(plan, run.run_config)
        if plan["require_scheduled"]:
            # Native submit happens before the scheduler commits its SUCCESS tick.
            # Submission is free; dequeue/launch require the completed native link.
            _scheduled_time(plan, run)
            if reserve:
                _reference_tick(plan, instance, run)
        reservation = (
            _reference_root(plan, instance, run, reserve=reserve)
            if _reference_mode(plan)
            else _root_reservation(plan, run, instance)
            if reserve
            else run
        )
    else:
        parent = instance.get_run_by_id(upstream) if upstream else None
        if (
            parent is None
            or parent.job_name != spec["upstream_job"]
            or parent.status.value != "SUCCESS"
        ):
            raise StagingExecutionError("staging child lacks completed allowed upstream run")
        reservation = _bind_run(plan, instance, parent, seen, reserve=reserve)
    if reserve:
        _deadline(plan, reservation)
        with _quota(plan) as quota:
            _slots(
                quota,
                prefix=("staging-job-v1", reservation.run_id, run.job_name),
                count=spec["max_runs"],
                identity=run.run_id,
                payload={"identity": run.run_id, "plan_sha256": digest(plan)},
            )
    bindings = {
        "ta3000/staging_plan_sha256": digest(plan),
        "ta3000/candidate_head_sha": plan["candidate"]["head_sha"],
        "ta3000/staging_root_run_id": reservation.run_config["root_run_id"]
        if reserve
        else reservation.run_id,
    }
    if reserve:
        bindings["ta3000/staging_root_reservation_id"] = reservation.run_id
        if _reference_mode(plan):
            bindings["ta3000/reference_authorization"] = reservation.run_config[
                "reference_authorization"
            ]
    if any(key in run.tags and run.tags[key] != value for key, value in bindings.items()):
        raise StagingExecutionError("actual run carries conflicting staging bindings")
    if reserve:
        cap = _runtime_cap(plan, reservation, instance, run.run_id)
        supplied = int(run.tags.get(RUNTIME_TAG, cap))
        if supplied < 1:
            raise StagingExecutionError("staging runtime duration tag must be positive")
        bindings[RUNTIME_TAG] = str(min(cap, supplied))
    instance.add_run_tags(run.run_id, bindings)
    return reservation


def _context_run(context):
    op = getattr(context, "op_execution_context", context)
    instance = getattr(op, "instance", None)
    run_id = str(getattr(context, "run_id", ""))
    if instance is None:
        raise StagingExecutionError("staging execution requires Dagster context")
    return instance, instance.get_run_by_id(run_id)


def _runtime_cap(plan, reservation, instance, run_id):
    run = instance.get_run_by_id(run_id)
    started = (
        _run_admitted_at(instance, run_id)
        if run.status.value == "STARTING"
        else _run_started_at(instance, run_id)
    )
    remaining = min(
        _stamp(plan["deadline_utc"]),
        float(reservation.run_config["started_at"]) + plan["max_runtime_seconds"],
    ) - float(started)
    cap = min(int(remaining), plan["max_runtime_seconds"])
    if cap < 1:
        raise StagingExecutionError("staging graph deadline cannot authorize another native step")
    return cap


def _check_operation(plan, operation):
    if operation == "rebuild":
        if (
            plan.get("stage") == "branch"
            and plan["attempt_kind"] == "small"
            and plan.get("root_job") == "research_data_prep_job"
            and plan.get("source", {}).get("fixture")
        ):
            from .staging_test_fixture import validate_branch_fixture

            try:
                validate_branch_fixture(plan)
            except (ValueError, OSError, KeyError, TypeError) as exc:
                raise StagingExecutionError(f"bounded test fixture rejected: {exc}") from exc
            return
        if plan["attempt_kind"] != "rebuild" or plan["limits"]["rebuild"] < 1:
            raise StagingExecutionError("full rebuild not authorized by protected plan")
    elif operation != "daily" or plan["attempt_kind"] == "rebuild":
        raise StagingExecutionError("staging operation differs from protected plan")


def _paths(plan, writable_paths):
    for path in writable_paths:
        if not _within(Path(path), Path(plan["data_root"])):
            raise StagingExecutionError("staging write path outside protected data root")


@contextmanager
def staging_execution_scope(context, *, stage, writable_paths, operation="daily"):
    """Bind real run identity before a protected asset starts work."""
    paths = [Path(path) for path in writable_paths]
    plan = _plan(paths[0] if paths else None)
    if plan is None:
        yield None
        return
    _require_permit(plan)
    _paths(plan, paths)
    _check_operation(plan, operation)
    op = getattr(context, "op_execution_context", context)
    if getattr(op, "retry_number", 0):
        raise StagingExecutionError("implicit staging step retry forbidden")
    instance, run = _context_run(context)
    current = _ACTIVE.get()
    if current is not None:
        if current.run.run_id != run.run_id or digest(current.plan) != digest(plan):
            raise StagingExecutionError("staging owner changed across native handoff")
        _deadline(plan, current.reservation)
        yield current
        return
    reservation = _bind_run(plan, instance, run)
    scope = _Scope(plan, instance, run, reservation)
    token = _ACTIVE.set(scope)
    try:
        yield scope
    except BaseException as exc:
        if _reference_mode(plan):
            halt_reference_graph(
                _control_plan_path(plan),
                reason=str(exc),
                root_run_id=reservation.run_config["root_run_id"],
            )
        raise
    finally:
        _ACTIVE.reset(token)


def _claim(scope, native_entry, output_path):
    plan = scope.plan
    if native_entry not in plan["native_steps"]:
        raise StagingExecutionError(f"native compute entry outside plan: {native_entry}")
    _deadline(plan, scope.reservation)
    _paths(plan, [output_path])
    with _quota(plan) as quota:
        claim = _slots(
            quota,
            prefix=("staging-native-v1", scope.reservation.run_id, native_entry),
            count=plan["native_steps"][native_entry],
            identity=None,
            payload={
                "identity": uuid4().hex,
                "plan_sha256": digest(plan),
                "run_id": scope.run.run_id,
                "native_entry": native_entry,
                "output_path": str(Path(output_path).resolve()),
            },
        )
    return claim.run_id


def _live_run(scope):
    if scope.instance is not None:
        owner = nullcontext(scope.instance)
    else:
        from dagster import DagsterInstance

        home = os.environ.get("DAGSTER_HOME", "")
        if not home:
            raise StagingExecutionError("native subprocess lost its persistent runtime binding")
        owner = DagsterInstance.from_config(home)
    with owner as instance:
        actual = instance.get_run_by_id(scope.run.run_id)
        if actual is None or actual.status.value != "STARTED":
            raise StagingExecutionError("staging run disappeared, stopped or changed bindings")
        cap = _runtime_cap(scope.plan, scope.reservation, instance, actual.run_id)
        if (
            actual.tags.get("ta3000/staging_plan_sha256") != digest(scope.plan)
            or actual.tags.get("ta3000/staging_root_reservation_id") != scope.reservation.run_id
            or not 0 < int(actual.tags.get(RUNTIME_TAG, "0")) <= cap
        ):
            raise StagingExecutionError("staging run disappeared, stopped or changed bindings")
        canonical = _bind_run(scope.plan, instance, actual)
        if (
            canonical.run_id != scope.reservation.run_id
            or canonical.run_config != scope.reservation.run_config
        ):
            raise StagingExecutionError("native grant lacks its canonical root reservation")
        from dagster._core.events import DagsterEventType

        if instance.get_records_for_run(
            actual.run_id, of_type=DagsterEventType.PIPELINE_STARTING, limit=1
        ).records:
            with _quota(scope.plan) as quota:
                launch = quota.get_run_by_id(
                    _key("staging-launch-v1", canonical.run_id, actual.run_id)
                )
                if launch is None or launch.run_config != {
                    "root_reservation_id": canonical.run_id,
                    "run_id": actual.run_id,
                }:
                    raise StagingExecutionError("native compute lacks its canonical launch claim")


def _check_windows(plan, windows):
    bounds = plan["scope"]
    for window in windows or ():
        instrument = window.get("instrument_id", window.get("internal_id"))
        timeframe = window.get("timeframe", window.get("source_timeframe"))
        start = window.get("start_ts", window.get("window_start_utc"))
        end = window.get("end_ts", window.get("window_end_utc"))
        if not isinstance(start, str) or not isinstance(end, str):
            raise StagingExecutionError("native changed window lacks bounded UTC scope")
        first, last = _stamp(start), _stamp(end)
        if (
            instrument not in bounds["instrument_ids"]
            or timeframe not in bounds["timeframes"]
            or first < _stamp(bounds["start_utc"])
            or last > _stamp(bounds["end_utc"])
            or last < first
            or last - first > bounds["max_changed_window_days"] * 86400
        ):
            raise StagingExecutionError("native changed window exceeds protected staging scope")


def _consume_grant(plan, native_entry, output_path):
    from types import SimpleNamespace

    raw = os.environ.get(GRANT_ENV, "")
    if not raw:
        raise StagingExecutionError("direct staging compute requires a bound Dagster owner")
    grant = json.loads(raw)
    validate_document("grant", grant)
    if (
        grant["plan_sha256"] != digest(plan)
        or grant["native_entry"] != native_entry
        or Path(grant["output_path"]).resolve() != Path(output_path).resolve()
        or time.time() >= _stamp(grant["deadline_utc"])
    ):
        raise StagingExecutionError("native subprocess grant binding changed/expired")
    with _quota(plan) as quota:
        issued = quota.get_run_by_id(_key("staging-grant-v1", grant["nonce"]))
        root = quota.get_run_by_id(grant["root_reservation_id"])
        if issued is None or issued.run_config.get("grant") != grant or root is None:
            raise StagingExecutionError("native grant was not issued by protected owner")
        scope = _Scope(
            plan, None, SimpleNamespace(run_id=grant["run_id"]), root, grant_entry=native_entry
        )
        _live_run(scope)
        claim = quota.get_run_by_id(issued.run_config["claim_id"])
        if (
            claim is None
            or claim.run_id
            not in {
                _key("staging-native-v1", root.run_id, native_entry, slot)
                for slot in range(plan["native_steps"].get(native_entry, 0))
            }
            or claim.run_config.get("run_id") != grant["run_id"]
            or claim.run_config.get("plan_sha256") != digest(plan)
            or claim.run_config.get("native_entry") != native_entry
            or claim.run_config.get("output_path") != str(Path(output_path).resolve())
            or root.run_config.get("plan_sha256") != digest(plan)
        ):
            raise StagingExecutionError("native grant root/claim binding changed")
        if _add(quota, _key("staging-grant-consumed-v1", grant["nonce"]), {"grant": grant}) is None:
            raise StagingExecutionError("native subprocess grant already consumed")
        if _add(quota, _key("staging-claim-consumed-v1", claim.run_id), {"grant": grant}) is None:
            raise StagingExecutionError("native subprocess claim already consumed")
    return scope


def require_staging_compute(*, stage, output_path, operation="daily", windows=None):
    """Reserve actual native work; never turn a missing owner into a rebuild."""
    plan = _plan(output_path)
    if plan is None:
        return
    _require_permit(plan)
    _check_operation(plan, operation)
    _check_windows(plan, windows)
    scope = _ACTIVE.get()
    if scope is None:
        scope = _consume_grant(plan, stage, output_path)
        _ACTIVE.set(scope)
    if digest(scope.plan) != digest(plan):
        raise StagingExecutionError("staging plan changed during compute")
    _live_run(scope)
    if scope.grant_entry is not None:
        if scope.grant_entry != stage or scope.grant_used:
            raise StagingExecutionError("native subprocess grant cannot authorize another compute")
        _deadline(plan, scope.reservation)
        scope.grant_used = True
        return
    claim = _claim(scope, stage, output_path)
    with _quota(plan) as quota:
        if (
            _add(quota, _key("staging-claim-consumed-v1", claim), {"run_id": scope.run.run_id})
            is None
        ):
            raise StagingExecutionError("native compute claim already consumed")


def protected_subprocess_env(env, *, native_entry, output_path):
    """Give one native subprocess an output/entry/run-bound, single-use grant."""
    result = dict(env)
    result.pop("TA3000_STAGING_OPERATOR_PRIVATE_KEY", None)
    result.pop("TA3000_STAGING_OPERATOR_KEY_ID", None)
    scope = _ACTIVE.get()
    plan = _plan(output_path)
    if plan is None:
        return result
    _require_permit(plan)
    if scope is None or scope.grant_entry is not None or digest(scope.plan) != digest(plan):
        raise StagingExecutionError("only the actual protected run can grant a native subprocess")
    claim = _claim(scope, native_entry, output_path)
    deadline = min(
        _stamp(plan["deadline_utc"]),
        float(scope.reservation.run_config["started_at"]) + plan["max_runtime_seconds"],
    )
    grant = {
        "schema_version": "staging_grant.v1",
        "plan_sha256": digest(plan),
        "root_reservation_id": scope.reservation.run_id,
        "run_id": scope.run.run_id,
        "native_entry": native_entry,
        "output_path": str(Path(output_path).resolve()),
        "nonce": uuid4().hex,
        "deadline_utc": datetime.fromtimestamp(deadline, UTC).isoformat().replace("+00:00", "Z"),
    }
    validate_document("grant", grant)
    with _quota(plan) as quota:
        if (
            _add(
                quota, _key("staging-grant-v1", grant["nonce"]), {"grant": grant, "claim_id": claim}
            )
            is None
        ):
            raise StagingExecutionError("native grant collision")
    result[GRANT_ENV] = json.dumps(grant, separators=(",", ":"))
    return result


def assert_staging_publication(output_path):
    plan = _plan(output_path)
    if plan is None:
        return
    _require_permit(plan)
    scope = _ACTIVE.get()
    if scope is None or digest(scope.plan) != digest(plan):
        raise StagingExecutionError("staging publication requires the bound execution owner")
    _paths(plan, [output_path])
    _deadline(plan, scope.reservation)
    _live_run(scope)


def staging_asset(function=None, *, operation="daily"):
    """Wrap existing Dagster asset ownership without changing its refresh receipt."""
    if function is None:
        return lambda target: staging_asset(target, operation=operation)

    @functools.wraps(function)
    def wrapped(context, *args, **kwargs):
        op = getattr(context, "op_execution_context", context)
        config = dict(getattr(op, "op_config", {}) or {})
        keys = (
            "materialized_output_dir",
            "output_dir",
            "results_output_dir",
            "raw_table_path",
            "canonical_bars_path",
            "staging_output_dir",
            "canonical_output_dir",
            "evidence_root",
        )
        paths = [Path(config[key]) for key in keys if config.get(key)]
        bound = _plan(paths[0] if paths else None)
        if bound is not None and bound.get("source", {}).get("fixture"):
            from .staging_test_fixture import validate_branch_fixture

            fixture = validate_branch_fixture(bound)
            readonly = {
                "canonical_output_dir": Path(fixture["canonical_dir"]),
                "canonical_bars_path": Path(fixture["canonical_dir"]) / "canonical_bars.delta",
                "raw_table_path": Path(fixture["raw_dir"]) / "raw_moex_history.delta",
            }
            paths = [
                Path(config[key])
                for key in keys
                if config.get(key)
                and (key not in readonly or Path(config[key]).resolve() != readonly[key].resolve())
            ]
            if not paths:
                paths = [Path(bound["data_root"])]
        if not paths and os.environ.get(PLAN_ENV):
            plan = load_plan(Path(os.environ[PLAN_ENV]))
            paths = [Path(plan["data_root"])]
        try:
            with staging_execution_scope(
                context, stage=function.__name__, writable_paths=paths, operation=operation
            ):
                return function(context, *args, **kwargs)
        except Exception as exc:
            plan = _plan(paths[0] if paths else None)
            if plan is not None and _reference_mode(plan):
                halt_reference_graph(_control_plan_path(plan), reason=str(exc))
            raise

    return wrapped


def __getattr__(name):
    # Default production/test imports do not initialize Dagster or its plugins.
    if name == "StagingDirectRunCoordinator":
        from dagster._core.run_coordinator.default_run_coordinator import DefaultRunCoordinator

        class StagingDirectRunCoordinator(DefaultRunCoordinator):
            def submit_run(self, context):
                plan = _plan()
                if plan is None or plan["stage"] != "branch" or plan["require_scheduled"]:
                    raise StagingExecutionError("direct staging coordinator requires a branch plan")
                _require_permit(plan)
                _bind_run(plan, self._instance, context.dagster_run, reserve=False)
                return super().submit_run(context)

        StagingDirectRunCoordinator.__module__ = __name__
        StagingDirectRunCoordinator.__qualname__ = name
        globals()[name] = StagingDirectRunCoordinator
        return StagingDirectRunCoordinator
    if name == "StagingRunLauncher":
        from dagster._core.launcher.default_run_launcher import DefaultRunLauncher

        class StagingRunLauncher(DefaultRunLauncher):
            def launch_run(self, context):
                plan = _plan()
                if plan is None:
                    raise StagingExecutionError("staging launcher requires its protected plan")
                _require_permit(plan)
                run = self._instance.get_run_by_id(context.dagster_run.run_id)
                if run is None or run.status.value != "STARTING":
                    raise StagingExecutionError("staging launch requires actual STARTING run")
                reservation = _bind_run(plan, self._instance, run)
                with _quota(plan) as quota:
                    claim = _add(
                        quota,
                        _key("staging-launch-v1", reservation.run_id, run.run_id),
                        {"root_reservation_id": reservation.run_id, "run_id": run.run_id},
                    )
                    if claim is None:
                        raise StagingExecutionError("staging run launch already spent")
                # Candidate code may lack every asset decorator: the attempt is already spent.
                super().launch_run(context)

        StagingRunLauncher.__module__ = __name__
        StagingRunLauncher.__qualname__ = name
        globals()[name] = StagingRunLauncher
        return StagingRunLauncher
    if name != "StagingRunCoordinator":
        raise AttributeError(name)
    from dagster._core.run_coordinator import QueuedRunCoordinator

    class StagingRunCoordinator(QueuedRunCoordinator):
        def __init__(self, **kwargs):
            if kwargs.get("max_concurrent_runs") not in {None, 1}:
                raise StagingExecutionError("staging coordinator requires one concurrent run")
            kwargs["max_concurrent_runs"] = 1
            kwargs["max_user_code_failure_retries"] = 0
            super().__init__(**kwargs)

        def submit_run(self, context):
            plan = _plan()
            if plan is None:
                raise StagingExecutionError("staging coordinator requires its protected plan")
            _bind_run(plan, self._instance, context.dagster_run, reserve=False)
            return super().submit_run(context)

        def get_run_queue_config(self):
            config = super().get_run_queue_config()
            try:
                plan = _plan()
                allowed = plan is not None and execution_permitted(plan)
                if allowed and plan["require_scheduled"]:
                    from dagster import RunsFilter
                    from dagster._core.storage.dagster_run import DagsterRunStatus

                    queued = self._instance.get_runs(
                        filters=RunsFilter(
                            job_name=plan["root_job"],
                            statuses=[DagsterRunStatus.QUEUED],
                        ),
                        limit=1,
                        ascending=True,
                    )
                    if queued:
                        _reference_tick(plan, self._instance, queued[0])
            except (ValueError, OSError):
                allowed = False
            return config._replace(max_concurrent_runs=1 if allowed else 0)

    StagingRunCoordinator.__module__ = __name__
    StagingRunCoordinator.__qualname__ = name
    globals()[name] = StagingRunCoordinator
    return StagingRunCoordinator
