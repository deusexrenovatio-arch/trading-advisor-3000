from __future__ import annotations

import os

import pytest

SPARK_DELTA_PROFILE_SKIP_REASON = (
    "Spark/Delta tests require a configured Spark profile on local Windows: "
    "set HADOOP_HOME/winutils or run the Docker/Linux Spark proof profile"
)


def require_configured_spark_delta_profile() -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(SPARK_DELTA_PROFILE_SKIP_REASON)
