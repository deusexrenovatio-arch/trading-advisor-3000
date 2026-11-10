"""Opt-in bounded Dagster jobs; never registered in production definitions."""

from datetime import date
from pathlib import Path

from dagster import Field, job, op

from .observation_acquisition import acquire_observations, child


@op(config_schema={"root": str, "identity_input": str, "date_from": str, "date_till": str})
def acquire_native_observations(context):
    config = context.op_config
    root = Path(config["root"]).resolve()
    if root != Path("/trial") or not (root / "scope.json").is_file():
        raise ValueError("native trial requires its explicitly mounted verification root")
    result = acquire_observations(
        root,
        identity_input_path=child(root, config["identity_input"]),
        date_from=date.fromisoformat(config["date_from"]),
        date_till=date.fromisoformat(config["date_till"]),
    )
    context.add_output_metadata(
        {
            "source_rows": result["source_rows"],
            "requests": result["requests"],
            "source_manifest_sha256": result["source_manifest"]["sha256"],
        }
    )
    return result


@job
def native_observation_acquisition_job():
    acquire_native_observations()


@op(config_schema={"root": str, "intent_path": str, "fault": Field(str, is_required=False)})
def materialize_native_observations(context):
    import json

    from .observation_materialization import materialize_observation_command

    config = context.op_config
    root = Path(config["root"]).resolve()
    intent = json.loads(child(root, config["intent_path"]).read_bytes())
    result = materialize_observation_command(root, intent, fault=config.get("fault"))
    context.add_output_metadata(
        {
            "completion_sha256": result["completion"]["sha256"],
            "replayed": result["replayed"],
            "recovered": result["recovered"],
        }
    )
    return result


@job
def native_observation_materialization_job():
    materialize_native_observations()
