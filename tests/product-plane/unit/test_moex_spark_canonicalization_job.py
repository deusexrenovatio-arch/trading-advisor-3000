from __future__ import annotations

import inspect

from trading_advisor_3000.spark_jobs import moex_canonicalization_job


def test_moex_spark_job_accepts_bucket_map_and_uses_policy_join() -> None:
    signature = inspect.signature(moex_canonicalization_job.run_moex_canonicalization_spark_job)
    source = inspect.getsource(moex_canonicalization_job.run_moex_canonicalization_spark_job)

    assert "canonical_buckets_path" in signature.parameters
    assert "bucket_start_ts" in source
    assert "bucket_end_ts" in source
    assert "canonical_ts" in source
    assert "policy_id" in source
    assert "bucket_seconds" in source
