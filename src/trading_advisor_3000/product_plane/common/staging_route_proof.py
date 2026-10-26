"""Shared product authority for accepted staging route and compute-origin evidence."""

from __future__ import annotations

import re
from datetime import datetime

JOBS = ("moex_baseline_update_job", "moex_cf_catch_up_job", "research_data_prep_job")
STAGES = (
    "moex_baseline_update",
    "continuous_front_bars",
    "research_datasets",
    "research_indicator_frames",
    "research_derived_indicator_frames",
    "continuous_front_indicator_acceptance_report",
    "research_data_snapshot",
)


def check_route(payload: dict, *, compute_candidate: dict | None = None) -> None:
    if (
        payload["stage"] != "acceptance"
        or payload["changed_windows"] < 1
        or payload["runtime"].get("mode", "candidate") != "candidate"
    ):
        raise ValueError("Acceptance requires a nonempty real route execution")
    candidate = payload["candidate"]
    compute = compute_candidate or candidate
    upstream = None
    for job in JOBS:
        run = payload["runs"].get(job, {})
        if run.get("status") != "SUCCESS" or not run.get("run_id"):
            raise ValueError(f"Required real job did not succeed: {job}")
        if run.get("candidate_head_sha") != compute["head_sha"]:
            raise ValueError(f"Run belongs to another candidate: {job}")
        if upstream and run.get("upstream_run_id") != upstream:
            raise ValueError(f"Broken upstream lineage: {job}")
        upstream = run["run_id"]
    if len({payload["runs"][job]["run_id"] for job in JOBS}) != len(JOBS):
        raise ValueError("Real route jobs must have distinct actual run identities")
    schedule = payload["schedule"]
    if (
        schedule.get("name") != "moex_baseline_daily_update_schedule"
        or schedule.get("status") != "SUCCESS"
        or not schedule.get("tick_id")
        or schedule.get("run_id") != payload["runs"][JOBS[0]]["run_id"]
    ):
        raise ValueError("Real schedule tick is missing or not bound to root run")
    for field in ("source", "outputs"):
        entry = payload[field]
        if not entry.get("snapshot_id") or not re.fullmatch(
            r"[0-9a-f]{64}", str(entry.get("manifest_sha256", ""))
        ):
            raise ValueError(f"Missing accepted snapshot proof: {field}")
    if not payload["outputs"].get("data_snapshot_id"):
        raise ValueError("Published Research snapshot requires its Data parent")
    if payload["qc"].get("status") != "PASS":
        raise ValueError("Required quality checks did not pass")
    coverage = payload["coverage"]
    if coverage.get("status") != "PASS" or set(coverage.get("required_stages", [])) != set(STAGES):
        raise ValueError("Coverage does not match the protected production route")
    if not set(STAGES).issubset(coverage.get("executed_stages", [])):
        raise ValueError("Required production route stages did not actually execute")
    required = coverage.get("required_scenarios")
    executed = coverage.get("executed_scenarios")
    if (
        not isinstance(required, list)
        or not isinstance(executed, list)
        or any(not isinstance(name, str) or not name for name in [*required, *executed])
        or not set(required).issubset(executed)
    ):
        raise ValueError("Required behavior scenarios have not been proven")
    consumer = payload["consumer"]
    if (
        consumer.get("status") != "PASS"
        or type(consumer.get("rows")) is not int
        or consumer["rows"] < 1
        or consumer.get("snapshot_id") != payload["outputs"]["snapshot_id"]
        or consumer.get("entrypoint") != "research.io.load_backtest_frames"
        or consumer.get("cache_hit") is not False
        or not consumer.get("process_id")
        or not re.fullmatch(r"[0-9a-f]{64}", str(consumer.get("digest", "")))
    ):
        raise ValueError("Fresh normal consumer readback is missing or inconsistent")
    read_at = datetime.fromisoformat(consumer["read_at_utc"].replace("Z", "+00:00"))
    issued = datetime.fromisoformat(payload["issued_at_utc"].replace("Z", "+00:00"))
    if read_at.tzinfo is None or read_at > issued:
        raise ValueError("Invalid independent consumer read time")
    for key, expected in (
        ("code_sha256", candidate["executable_sha256"]),
        ("config_sha256", candidate["config_sha256"]),
        ("image_digest", candidate["image_digest"]),
    ):
        if payload["runtime"].get(key) != expected:
            raise ValueError(f"Actual runtime binding differs: {key}")
    budget = payload["runtime"].get("budget", {})
    claims = budget.get("native_claims", {})
    if (
        budget.get("status") != "PASS"
        or budget.get("plan_sha256") != payload["plan_sha256"]
        or not re.fullmatch(
            r"[0-9a-f]{32}|[0-9a-f]{64}", str(budget.get("root_reservation_id", ""))
        )
        or not isinstance(claims, dict)
        or not claims
        or any(type(count) is not int or count < 1 for count in claims.values())
    ):
        raise ValueError("Persistent execution-budget evidence is missing or inconsistent")


def verify_acceptance_route(envelope: dict, keys: dict, now: datetime | None = None) -> dict:
    """Authenticate full candidate acceptance; reference/branch receipts cannot grant admission."""
    from .staging_contracts import digest, verify_receipt

    payload = verify_receipt(envelope, keys, now=now)
    compute_candidate = None
    if "compute_origin" in payload:
        origin = payload["compute_origin"]
        signed = origin["signed_receipt"]
        if "compute_origin" in signed["payload"] or origin["receipt_sha256"] != digest(signed):
            raise ValueError("Compute origin must be one immutable original signed receipt")
        original = verify_receipt(
            signed,
            keys,
            now=datetime.fromisoformat(signed["payload"]["issued_at_utc"].replace("Z", "+00:00")),
        )
        check_route(original)
        compute_candidate = original["candidate"]
        for key in ("executable_sha256", "config_sha256", "image_digest"):
            if payload["candidate"][key] != compute_candidate[key]:
                raise ValueError(f"Compute evidence is not reusable after changed {key}")
        for key in (
            "source",
            "outputs",
            "runs",
            "schedule",
            "changed_windows",
            "qc",
            "coverage",
            "plan_sha256",
        ):
            if payload[key] != original[key]:
                raise ValueError(f"Compute origin identity changed: {key}")
        if payload["runtime"]["budget"] != original["runtime"]["budget"]:
            raise ValueError("Compute origin quota reservation changed")
    check_route(payload, compute_candidate=compute_candidate)
    return payload
