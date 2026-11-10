"""Opt-in Research trial job; deliberately absent from production definitions."""

import json
from pathlib import Path

from dagster import Field, job, op

from trading_advisor_3000.product_plane.common.immutable_files import child


@op(config_schema={"root": str, "intent_path": str, "fault": Field(str, is_required=False)})
def materialize_native_research_l0(context):
    from .native_observation_materialization import materialize_l0_command

    config = context.op_config
    root = Path(config["root"]).resolve()
    intent = json.loads(child(root, config["intent_path"]).read_bytes())
    result = materialize_l0_command(root, intent, fault=config.get("fault"))
    context.add_output_metadata(
        {
            "completion_sha256": result["completion"]["sha256"],
            "replayed": result["replayed"],
            "recovered": result["recovered"],
        }
    )
    return result


@job
def native_research_l0_materialization_job():
    materialize_native_research_l0()
