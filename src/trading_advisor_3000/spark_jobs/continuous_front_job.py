from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    delta_table_columns,
    has_delta_log,
)
from trading_advisor_3000.product_plane.research.admission import admitted_compute
from trading_advisor_3000.product_plane.research.continuous_front import (
    CONTINUOUS_FRONT_TABLES,
    continuous_front_store_contract,
)
from trading_advisor_3000.product_plane.research.datasets import (
    CALENDAR_EXPIRY_CONTINUOUS_FRONT_POLICY,
    ContinuousFrontPolicy,
)
from trading_advisor_3000.product_plane.runtime.stage_timings import (
    StageTimings,
    record_stage_timing,
    stage_timer,
)

from .canonical_bars_job import DEFAULT_SPARK_MASTER, _create_spark_session


@dataclass(frozen=True)
class ContinuousFrontSparkJobSpec:
    app_name: str = "ta3000-continuous-front-refresh"
    target_bars_table: str = "continuous_front_bars"
    target_roll_events_table: str = "continuous_front_roll_events"
    target_adjustment_ladder_table: str = "continuous_front_adjustment_ladder"
    target_qc_report_table: str = "continuous_front_qc_report"


@dataclass(frozen=True)
class ContinuousFrontRefreshWindow:
    instrument_id: str
    timeframe: str
    start_ts: str | None = None
    end_ts: str | None = None


def build_continuous_front_sql_plan() -> str:
    return """
WITH filtered_canonical_bars AS (
  SELECT contract_id, instrument_id, timeframe, ts, open, high, low, close, volume, open_interest
  FROM canonical_bars
  WHERE (:instrument_ids_empty OR instrument_id IN (:instrument_ids))
    AND (:timeframes_empty OR timeframe IN (:timeframes))
    AND (:start_ts IS NULL OR ts >= :start_ts)
    AND (:end_ts IS NULL OR ts <= :end_ts)
),
regular_session_bars AS (
  SELECT *
  FROM filtered_canonical_bars
  WHERE timeframe NOT RLIKE '^[0-9]+[mh]$'
     OR date_format(from_utc_timestamp(ts, 'Europe/Moscow'), 'HH:mm') >= '09:00'
    AND date_format(from_utc_timestamp(ts, 'Europe/Moscow'), 'HH:mm') < '23:50'
),
ranked_contract_bars AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY instrument_id, timeframe, ts
           ORDER BY open_interest DESC, volume DESC, contract_id DESC
         ) AS liquidity_rank
  FROM regular_session_bars
),
confirmed_roll_decisions AS (
  SELECT *,
         COUNT(*) OVER (
           PARTITION BY instrument_id, timeframe, candidate_maturity_rank
           ORDER BY ts ROWS BETWEEN :confirmation_bars_minus_one PRECEDING AND CURRENT ROW
         ) AS confirmation_run
  FROM ranked_contract_bars
),
active_timeline AS (
  SELECT *,
         MAX(confirmed_candidate_maturity_rank) OVER (
           PARTITION BY instrument_id, timeframe
           ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS active_maturity_rank
  FROM confirmed_roll_decisions
)
SELECT *
FROM active_timeline
ORDER BY instrument_id, timeframe, ts
""".strip()


def _spark_type(type_name: str):
    from pyspark.sql import types as T  # type: ignore[import-not-found]

    normalized = type_name.strip().lower()
    if normalized in {"string", "json"}:
        return T.StringType()
    if normalized == "timestamp":
        return T.TimestampType()
    if normalized == "date":
        return T.DateType()
    if normalized in {"bool", "boolean"}:
        return T.BooleanType()
    if normalized in {"double", "float"}:
        return T.DoubleType()
    if normalized in {"int", "integer"}:
        return T.IntegerType()
    if normalized in {"bigint", "long"}:
        return T.LongType()
    return T.StringType()


def _spark_schema(columns: dict[str, str]):
    from pyspark.sql import types as T  # type: ignore[import-not-found]

    return T.StructType(
        [T.StructField(name, _spark_type(type_name), True) for name, type_name in columns.items()]
    )


def _spark_sql_type(type_name: str) -> str:
    normalized = type_name.strip().lower()
    if normalized in {"string", "json"}:
        return "string"
    if normalized == "timestamp":
        return "timestamp"
    if normalized == "date":
        return "date"
    if normalized in {"bool", "boolean"}:
        return "boolean"
    if normalized in {"double", "float"}:
        return "double"
    if normalized in {"int", "integer"}:
        return "int"
    if normalized in {"bigint", "long"}:
        return "long"
    return "string"


def _require_spark_native_policy(policy: ContinuousFrontPolicy) -> None:
    unsupported: list[str] = []
    if policy.roll_policy_mode not in {
        "calendar_expiry_v1",
        "liquidity_oi_v1",
        "liquidity_volume_oi_v1",
    }:
        unsupported.append(f"roll_policy_mode={policy.roll_policy_mode}")
    if policy.adjustment_mode != "additive":
        unsupported.append(f"adjustment_mode={policy.adjustment_mode}")
    if policy.gap_type != "close_to_close":
        unsupported.append(f"gap_type={policy.gap_type}")
    allowed_reference_price_policy = {"decision_bar_close"}
    if policy.roll_policy_mode == "calendar_expiry_v1":
        expected = ContinuousFrontPolicy.from_config(CALENDAR_EXPIRY_CONTINUOUS_FRONT_POLICY)
        allowed_reference_price_policy.add("last_old_active_close_to_first_new_active_close")
        for field_name in (
            "calendar_roll_offset_trading_days",
            "expected_timeline_mode",
            "session_policy",
            "session_timezone",
            "session_start_time",
            "session_end_time",
            "tie_breaker",
        ):
            value = getattr(policy, field_name)
            if value != getattr(expected, field_name):
                unsupported.append(f"calendar_expiry_v1.{field_name}={value}")
    if policy.reference_price_policy not in allowed_reference_price_policy:
        unsupported.append(f"reference_price_policy={policy.reference_price_policy}")
    allowed_switch_timing = {"next_tradable_bar_after_decision_watermark"}
    if policy.roll_policy_mode == "calendar_expiry_v1":
        allowed_switch_timing.add("first_active_bar_on_or_after_roll_session")
    if policy.switch_timing not in allowed_switch_timing:
        unsupported.append(f"switch_timing={policy.switch_timing}")
    if not policy.decision_uses_closed_bar:
        unsupported.append("decision_uses_closed_bar=False")
    if not policy.effective_after_watermark:
        unsupported.append("effective_after_watermark=False")
    if unsupported:
        raise RuntimeError(
            "continuous_front Spark native contour does not support: " + ", ".join(unsupported)
        )


def _metric_columns(policy: ContinuousFrontPolicy) -> tuple[str, str]:
    if policy.roll_policy_mode == "liquidity_volume_oi_v1":
        return "volume", "open_interest"
    return policy.primary_metric, policy.secondary_metric


def _policy_timestamp() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _window_text(window: Mapping[str, object], key: str, *, required: bool) -> str | None:
    value = str(window.get(key) or "").strip()
    if required and not value:
        raise ValueError(f"continuous_front refresh window requires `{key}`")
    return value or None


def _normalize_refresh_windows(
    refresh_windows: Sequence[Mapping[str, object] | ContinuousFrontRefreshWindow] | None = None,
) -> tuple[ContinuousFrontRefreshWindow, ...]:
    normalized: list[ContinuousFrontRefreshWindow] = []
    for window in refresh_windows or ():
        if isinstance(window, ContinuousFrontRefreshWindow):
            normalized.append(window)
            continue
        if not isinstance(window, Mapping):
            raise TypeError("continuous_front refresh windows must be mappings")
        normalized.append(
            ContinuousFrontRefreshWindow(
                instrument_id=str(_window_text(window, "instrument_id", required=True)),
                timeframe=str(_window_text(window, "timeframe", required=True)),
                start_ts=_window_text(window, "start_ts", required=False),
                end_ts=_window_text(window, "end_ts", required=False),
            )
        )
    return tuple(normalized)


def _unique_scope_values(
    refresh_windows: tuple[ContinuousFrontRefreshWindow, ...],
    field_name: str,
) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()
    for window in refresh_windows:
        value = str(getattr(window, field_name))
        if value and value not in seen:
            values.append(value)
            seen.add(value)
    return tuple(values)


def _spark_sql_literal(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _scoped_delete_condition(filters: list[tuple[str, str, object]]) -> str:
    clauses: list[str] = []
    for column, operator, value in filters:
        normalized_operator = str(operator).strip().lower()
        if value is None:
            continue
        if normalized_operator in {"in", "not in"}:
            scoped_values = (
                tuple(item for item in value if item is not None)
                if isinstance(value, (list, tuple, set))
                else ()
            )
            if not scoped_values:
                continue
            literals = ", ".join(_spark_sql_literal(item) for item in scoped_values)
            sql_operator = "IN" if normalized_operator == "in" else "NOT IN"
            clauses.append(f"{column} {sql_operator} ({literals})")
            continue
        if normalized_operator not in {"=", ">=", "<=", ">", "<"}:
            raise ValueError(f"unsupported scoped Delta delete operator: {operator}")
        clauses.append(f"{column} {normalized_operator} {_spark_sql_literal(value)}")
    if not clauses:
        raise ValueError("scoped Delta delete requires at least one scope value")
    return " AND ".join(clauses)


def _scoped_delete_disjunction(filter_groups: Sequence[list[tuple[str, str, object]]]) -> str:
    conditions = [_scoped_delete_condition(filters) for filters in filter_groups]
    if not conditions:
        raise ValueError("scoped Delta delete requires at least one scope group")
    if len(conditions) == 1:
        return conditions[0]
    return " OR ".join(f"({condition})" for condition in conditions)


def _continuous_front_replace_filters(
    *,
    table_name: str,
    dataset_version: str,
    instrument_ids: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str | None,
    end_ts: str | None,
) -> list[tuple[str, str, object]]:
    filters: list[tuple[str, str, object]] = [("dataset_version", "=", dataset_version)]
    if instrument_ids:
        filters.append(("instrument_id", "in", [*instrument_ids]))
    if timeframes:
        filters.append(("timeframe", "in", [*timeframes]))
    time_column_by_table = {
        "continuous_front_bars": "ts",
        "continuous_front_roll_events": "effective_ts",
        "continuous_front_adjustment_ladder": "effective_ts",
    }
    time_column = time_column_by_table.get(table_name)
    if time_column and start_ts:
        filters.append((time_column, ">=", start_ts))
    if time_column and end_ts:
        filters.append((time_column, "<=", end_ts))
    return filters


def _continuous_front_replace_filter_groups(
    *,
    table_name: str,
    dataset_version: str,
    instrument_ids: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str | None,
    end_ts: str | None,
    refresh_windows: tuple[ContinuousFrontRefreshWindow, ...] = (),
) -> list[list[tuple[str, str, object]]]:
    if refresh_windows:
        return [
            _continuous_front_replace_filters(
                table_name=table_name,
                dataset_version=dataset_version,
                instrument_ids=(window.instrument_id,),
                timeframes=(window.timeframe,),
                start_ts=window.start_ts,
                end_ts=window.end_ts,
            )
            for window in refresh_windows
        ]
    return [
        _continuous_front_replace_filters(
            table_name=table_name,
            dataset_version=dataset_version,
            instrument_ids=instrument_ids,
            timeframes=timeframes,
            start_ts=start_ts,
            end_ts=end_ts,
        )
    ]


def _filter_to_refresh_windows(
    dataframe,
    refresh_windows: tuple[ContinuousFrontRefreshWindow, ...],
    *,
    ts_column: str = "ts",
):
    if not refresh_windows:
        return dataframe
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    condition = None
    for window in refresh_windows:
        scoped = (F.col("instrument_id") == F.lit(window.instrument_id)) & (
            F.col("timeframe") == F.lit(window.timeframe)
        )
        if window.start_ts:
            scoped = scoped & (F.col(ts_column) >= F.lit(window.start_ts).cast("timestamp"))
        if window.end_ts:
            scoped = scoped & (F.col(ts_column) <= F.lit(window.end_ts).cast("timestamp"))
        condition = scoped if condition is None else condition | scoped
    return dataframe.where(condition)


def _load_filtered_bars(
    *,
    spark: object,
    canonical_bars_path: Path,
    instrument_ids: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str | None,
    end_ts: str | None,
    refresh_windows: tuple[ContinuousFrontRefreshWindow, ...] = (),
):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    bars = (
        spark.read.format("delta")
        .load(str(canonical_bars_path))
        .select(
            "contract_id",
            "instrument_id",
            "timeframe",
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
        )
    )
    if instrument_ids:
        bars = bars.where(F.col("instrument_id").isin(list(instrument_ids)))
    if timeframes:
        bars = bars.where(F.col("timeframe").isin(list(timeframes)))
    if start_ts:
        bars = bars.where(F.col("ts") >= F.lit(start_ts).cast("timestamp"))
    if end_ts:
        bars = bars.where(F.col("ts") <= F.lit(end_ts).cast("timestamp"))
    bars = _filter_to_refresh_windows(bars, refresh_windows)
    return bars


def _is_intraday_timeframe():
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    return F.lower(F.col("timeframe")).rlike(r"^[0-9]+[mh]$")


def _local_session_ts(policy: ContinuousFrontPolicy):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    return F.from_utc_timestamp(F.col("ts"), policy.session_timezone)


def _session_date(policy: ContinuousFrontPolicy):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    return F.to_date(_local_session_ts(policy))


def _session_minute(policy: ContinuousFrontPolicy):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    local_ts = _local_session_ts(policy)
    return F.hour(local_ts) * F.lit(60) + F.minute(local_ts)


def _filter_policy_session(dataframe, policy: ContinuousFrontPolicy):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    local_minute = _session_minute(policy)
    in_session = (local_minute >= F.lit(policy.session_start_minute)) & (
        local_minute < F.lit(policy.session_end_minute)
    )
    return dataframe.where((~_is_intraday_timeframe()) | in_session)


def _with_maturity_columns(dataframe):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    root = F.upper(F.split(F.col("contract_id"), "@").getItem(0))
    month_code = F.regexp_extract(root, r"([FGHJKMNQUVXZ])(\d{1,2})(?:$|[._-])", 1)
    code_year = F.regexp_extract(root, r"([FGHJKMNQUVXZ])(\d{1,2})(?:$|[._-])", 2)
    numeric_month = F.regexp_extract(root, r"[-_](\d{1,2})[._](\d{1,2})(?:$|[._-])", 1)
    numeric_year = F.regexp_extract(root, r"[-_](\d{1,2})[._](\d{1,2})(?:$|[._-])", 2)
    month_from_code = (
        F.when(month_code == "F", F.lit(1))
        .when(month_code == "G", F.lit(2))
        .when(month_code == "H", F.lit(3))
        .when(month_code == "J", F.lit(4))
        .when(month_code == "K", F.lit(5))
        .when(month_code == "M", F.lit(6))
        .when(month_code == "N", F.lit(7))
        .when(month_code == "Q", F.lit(8))
        .when(month_code == "U", F.lit(9))
        .when(month_code == "V", F.lit(10))
        .when(month_code == "X", F.lit(11))
        .when(month_code == "Z", F.lit(12))
    )
    raw_year = F.when(code_year != "", code_year).otherwise(
        F.when(numeric_year != "", numeric_year)
    )
    maturity_year = (
        F.when(F.length(raw_year) == 1, F.lit(2020) + raw_year.cast("int"))
        .when(raw_year.cast("int") < 70, F.lit(2000) + raw_year.cast("int"))
        .otherwise(F.lit(1900) + raw_year.cast("int"))
    )
    maturity_month = F.when(month_code != "", month_from_code).otherwise(numeric_month.cast("int"))
    return (
        dataframe.withColumn("maturity_year", maturity_year)
        .withColumn(
            "maturity_month",
            maturity_month,
        )
        .withColumn(
            "maturity_rank",
            F.when(
                F.col("maturity_year").isNotNull() & F.col("maturity_month").between(1, 12),
                F.col("maturity_year") * F.lit(12) + F.col("maturity_month"),
            ),
        )
    )


def _calendar_source_pair(root, body_pin, receipt_pin):
    import json
    from urllib.parse import urlsplit

    from trading_advisor_3000.product_plane.common.immutable_files import pinned_bytes

    raw = pinned_bytes(Path(root), body_pin)
    receipt = json.loads(pinned_bytes(Path(root), receipt_pin))
    if not isinstance(receipt, dict):
        raise ValueError("calendar source receipt must be an object")
    body = receipt.get("body")
    recorded = receipt.get("body_sha256") or (
        body.get("sha256") if isinstance(body, dict) else None
    )
    url = urlsplit(receipt.get("final_url", ""))
    if (
        receipt.get("status") != 200
        or recorded != body_pin["sha256"]
        or receipt.get("body_bytes") != len(raw)
        or url.scheme != "https"
        or url.hostname not in {"www.moex.com", "moex.com", "iss.moex.com"}
    ):
        raise ValueError("calendar body/receipt/source association mismatch")
    return raw, url


def validate_calendar_sources(root, schedule, *, offset=None):
    """Admit the reviewed bounded source interpretation, never arbitrary HTML rules."""
    import json

    from trading_advisor_3000.product_plane.contracts.identity_v3 import _Evaluator

    if not isinstance(schedule, dict):
        raise ValueError(
            "source-bound calendar_roll_schedule is required for fresh calendar compute"
        )
    evaluator = _Evaluator({})
    evaluator.check(
        evaluator._document("market_identity.v3.json")["$defs"]["native_cf_calendar_roll_schedule"],
        schedule,
    )
    intervals = calendar_active_intervals(schedule)
    from trading_advisor_3000.product_plane.common.immutable_files import pinned_bytes

    packet = json.loads(
        pinned_bytes(
            Path(root),
            {
                "path": "source-facts/temporal/facts.json",
                "sha256": "c018f490796c58c3816152d61db40cdab8d20f09e86c61ce896d2107b090219d",
            },
        )
    )
    if packet["derived_inferences_not_raw_exchange_fields"]["planned_open_source_dates"][
        "dates"
    ] != ["2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17"]:
        raise ValueError("reviewed planned calendar interpretation mismatch")
    reviewed = {
        "n94172": (
            "annual_calendar",
            "bc73b14f1cbea57d7e389bb5a3a76bbb4443ad8ca94db24db2d13d286f1871e9",
            0,
        ),
        "n103379": (
            "dated_schedule",
            "79cb5b5620bdc320535665df614d4e81e22ed6890707d6f0678a4385c811fc34",
            1,
        ),
        "n95564": (
            "weekend_exception",
            "730ede1b16eb583903c22d2e9cc1e1752317f8f03693a6785ff87a648ab3ef09",
            2,
        ),
    }
    if {r["rule_id"] for r in schedule["governing_rules"]} != set(reviewed):
        raise ValueError("calendar source interpretation is not admitted")
    for rule in schedule["governing_rules"]:
        kind, digest, precedence = reviewed[rule["rule_id"]]
        _, url = _calendar_source_pair(root, rule["body"], rule["receipt"])
        if (rule["kind"], rule["body"]["sha256"], rule["precedence"]) != (
            kind,
            digest,
            precedence,
        ) or url.path != "/" + rule["rule_id"]:
            raise ValueError("calendar rule differs from reviewed source interpretation")
    for group in schedule["groups"]:
        if offset is not None and group["roll_offset_trading_days"] != offset:
            raise ValueError("calendar schedule offset differs from legacy policy")
        for item in group["planned_dates"]:
            day = item["source_date"]
            expected = "closed" if day in {"2026-09-12", "2026-09-13"} else "open"
            ids = {"n94172", "n95564" if expected == "closed" else "n103379"}
            if (
                not "2026-09-12" <= day < "2026-09-18"
                or item["state"] != expected
                or set(item["governing_rule_ids"]) != ids
            ):
                raise ValueError("calendar date lacks reviewed source admission")
        for component in group["components"]:
            source = component["last_trade_source"]
            raw, url = _calendar_source_pair(root, source["body"], source["receipt"])
            if (
                url.hostname != "iss.moex.com"
                or component["source_symbol"] not in url.path.split("/")[-1]
            ):
                raise ValueError("last-trade source symbol/URL mismatch")
            payload = json.loads(raw)
            block = payload[source["block"]]
            rows = [dict(zip(block["columns"], row, strict=True)) for row in block["data"]]
            matches = [r for r in rows if r.get(source["row_key"]) == source["row_value"]]
            if (
                len(matches) != 1
                or str(matches[0][source["value_column"]]) != component["last_trade_date"]
            ):
                raise ValueError("last-trade date differs from pinned original field")
            if source["kind"] == "moex_iss_description":
                if [r.get("value") for r in rows if r.get("name") == "SECID"] != [
                    component["source_symbol"]
                ]:
                    raise ValueError("description SECID differs from calendar component")
            elif source["row_value"] != component["source_symbol"]:
                raise ValueError("securities SECID differs from calendar component")
    return intervals


def calendar_active_intervals(schedule):
    """Compile admitted source dates; observed rows never establish contract lifetime."""
    from datetime import date, timedelta

    if not isinstance(schedule, dict):
        raise ValueError(
            "source-bound calendar_roll_schedule is required for fresh calendar compute"
        )
    rules = {r["rule_id"]: r for r in schedule["governing_rules"]}
    if len(rules) != len(schedule["governing_rules"]) or len(
        {r["precedence"] for r in rules.values()}
    ) != len(rules):
        raise ValueError("duplicate calendar rule or ambiguous precedence")
    result = []
    for group in schedule["groups"]:
        dates = group["planned_dates"]
        begin, end = (
            date.fromisoformat(group["coverage_from"]),
            date.fromisoformat(group["coverage_to"]),
        )
        expected = [(begin + timedelta(days=i)).isoformat() for i in range((end - begin).days)]
        if sorted(d["source_date"] for d in dates) != expected:
            raise ValueError("calendar coverage must explicitly contain every date once")
        for item in dates:
            ids = item["governing_rule_ids"]
            if not ids or len(set(ids)) != len(ids) or any(i not in rules for i in ids):
                raise ValueError("missing/duplicate governing calendar rule")
            for rule_id in ids:
                rule = rules[rule_id]
                if not (
                    rule["effective_from"] <= item["source_date"]
                    and (rule["effective_to"] is None or item["source_date"] < rule["effective_to"])
                ):
                    raise ValueError("calendar rule outside effective source dates")
        components = sorted(group["components"], key=lambda c: c["source_order"])
        if [c["source_order"] for c in components] != list(range(len(components))):
            raise ValueError("calendar component order must be contiguous")
        for field in ["component_key", "source_symbol", "last_trade_date"]:
            if len({c[field] for c in components}) != len(components):
                raise ValueError("duplicate/conflicting calendar component " + field)
        last_dates = [c["last_trade_date"] for c in components]
        if last_dates != sorted(last_dates):
            raise ValueError("calendar source order disagrees with last trade dates")
        opens = sorted(d["source_date"] for d in dates if d["state"] == "open")
        switches = []
        for component in components[:-1]:
            last = component["last_trade_date"]
            if last not in opens or opens.index(last) < group["roll_offset_trading_days"]:
                raise ValueError("insufficient planned open dates for last-trade offset")
            switches.append(opens[opens.index(last) - group["roll_offset_trading_days"]])
        if switches != sorted(set(switches)):
            raise ValueError("non-increasing planned roll dates")
        for index, component in enumerate(components):
            result.append(
                {
                    "group_key": group["group_key"],
                    "component_key": component["component_key"],
                    "source_order": index,
                    "active_from": group["coverage_from"] if index == 0 else switches[index - 1],
                    "active_to": group["coverage_to"]
                    if index == len(components) - 1
                    else switches[index],
                    "planned_roll_ordinal": index,
                }
            )
    return result


def roll_gap_expression(new_close, old_close):
    from pyspark.sql import functions as F

    return F.col(new_close).cast("double") - F.col(old_close).cast("double")


def calendar_selected_frame(frame, schedule, *, group_column, component_column, source_date):
    """Shared selector with explicit keys/date expression; preserves native buffers."""
    from pyspark.sql import functions as F

    intervals = calendar_active_intervals(schedule)

    def quoted(value):
        return "CAST(unhex('" + str(value).encode("utf-8").hex() + "') AS STRING)"

    values = ",".join(
        "("
        + ",".join(
            quoted(row[k]) for k in ["group_key", "component_key", "active_from", "active_to"]
        )
        + ","
        + str(row["planned_roll_ordinal"])
        + ")"
        for row in intervals
    )
    dates = frame.sparkSession.sql(
        "SELECT * FROM VALUES "
        + values
        + " AS t(_calendar_group,_calendar_component,_calendar_from,_calendar_to,"
        "planned_roll_ordinal)"
    )
    bars = frame.withColumn("_calendar_source_date", source_date)
    covered = F.lit(False)
    for group in schedule["groups"]:
        covered = covered | (
            (F.col(group_column) == F.lit(group["group_key"]))
            & F.col(component_column).isin([c["component_key"] for c in group["components"]])
            & (F.col("_calendar_source_date") >= F.lit(group["coverage_from"]))
            & (F.col("_calendar_source_date") < F.lit(group["coverage_to"]))
        )
    if bars.where(~covered | F.col("_calendar_source_date").isNull()).limit(1).count():
        raise ValueError("calendar requested source lacks admitted coverage/order")
    bars = bars.alias("bar")
    dates = dates.alias("schedule")
    return (
        bars.join(
            dates,
            (F.col("bar." + group_column) == F.col("schedule._calendar_group"))
            & (F.col("bar." + component_column) == F.col("schedule._calendar_component"))
            & (F.col("bar._calendar_source_date") >= F.col("schedule._calendar_from"))
            & (F.col("bar._calendar_source_date") < F.col("schedule._calendar_to")),
            "inner",
        )
        .select("bar.*", F.col("schedule.planned_roll_ordinal").cast("long"))
        .drop("_calendar_source_date")
    )


def _calendar_expiry_active_timeline(
    *, keyed, contract_keyed, policy: ContinuousFrontPolicy, calendar_roll_schedule=None
):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if any(
        g["roll_offset_trading_days"] != policy.calendar_roll_offset_trading_days
        for g in (calendar_roll_schedule or {}).get("groups", [])
    ):
        raise ValueError("calendar schedule offset differs from legacy policy")
    grouped = Window.partitionBy("instrument_id", "timeframe")
    source = keyed.withColumn("series_input_row_count", F.count(F.lit(1)).over(grouped))
    selected = calendar_selected_frame(
        source,
        calendar_roll_schedule,
        group_column="instrument_id",
        component_column="contract_id",
        source_date=_session_date(policy).cast("string"),
    )
    series_order = grouped.orderBy("ts")
    previous_contract_id = F.lag("contract_id").over(series_order)
    previous_ts = F.lag("ts").over(series_order)
    return (
        selected.withColumn("ts_index", F.dense_rank().over(series_order))
        .withColumn(
            "decision_ts",
            F.when(
                previous_contract_id.isNotNull() & (previous_contract_id != F.col("contract_id")),
                previous_ts,
            ),
        )
        .withColumn("old_reference_ts", F.col("decision_ts"))
        .select(
            "instrument_id",
            "timeframe",
            "ts",
            "ts_index",
            "series_input_row_count",
            F.col("contract_id").alias("candidate_contract_id"),
            F.col("planned_roll_ordinal").alias("candidate_maturity_rank"),
            F.col("contract_id").alias("active_contract_id"),
            F.col("planned_roll_ordinal").alias("active_maturity_rank"),
            "decision_ts",
            "old_reference_ts",
        )
    )


def _active_timeline(
    *, bars, policy: ContinuousFrontPolicy, calendar_bars=None, calendar_roll_schedule=None
):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F

    keyed = _with_maturity_columns(bars)
    if policy.roll_policy_mode == "calendar_expiry_v1":
        contract_source = calendar_bars if calendar_bars is not None else bars
        contract_keyed = _with_maturity_columns(contract_source)
        return _calendar_expiry_active_timeline(
            keyed=keyed,
            contract_keyed=contract_keyed,
            policy=policy,
            calendar_roll_schedule=calendar_roll_schedule,
        )

    primary_metric, secondary_metric = _metric_columns(policy)
    if primary_metric not in {"open_interest", "volume"} or secondary_metric not in {
        "open_interest",
        "volume",
    }:
        raise RuntimeError(
            "continuous_front Spark native contour supports only "
            "open_interest/volume ranking metrics"
        )

    by_ts = Window.partitionBy("instrument_id", "timeframe", "ts")
    series_order = Window.partitionBy("instrument_id", "timeframe").orderBy("ts")
    candidate_order = Window.partitionBy("instrument_id", "timeframe", "ts").orderBy(
        F.col(primary_metric).desc(),
        F.col(secondary_metric).desc(),
        F.col("contract_id").desc(),
    )
    ranked = (
        keyed.withColumn(
            "metric_total",
            F.sum(F.greatest(F.col(primary_metric).cast("double"), F.lit(0.0))).over(by_ts),
        )
        .withColumn(
            "candidate_metric", F.greatest(F.col(primary_metric).cast("double"), F.lit(0.0))
        )
        .withColumn(
            "candidate_share",
            F.when(F.col("metric_total") <= 0.0, F.lit(1.0)).otherwise(
                F.col("candidate_metric") / F.col("metric_total")
            ),
        )
        .withColumn("liquidity_rank", F.row_number().over(candidate_order))
        .withColumn("ts_index", F.dense_rank().over(series_order))
        .withColumn(
            "series_input_row_count",
            F.count(F.lit(1)).over(Window.partitionBy("instrument_id", "timeframe")),
        )
        .withColumn(
            "first_maturity_rank",
            F.min("maturity_rank").over(Window.partitionBy("instrument_id", "timeframe")),
        )
    )
    candidates = ranked.where(F.col("liquidity_rank") == 1).select(
        "instrument_id",
        "timeframe",
        "ts",
        "ts_index",
        "series_input_row_count",
        F.col("contract_id").alias("candidate_contract_id"),
        F.col("maturity_rank").alias("candidate_maturity_rank"),
        "candidate_metric",
        "candidate_share",
        "first_maturity_rank",
    )
    recent_series_window = series_order.rowsBetween(-(policy.confirmation_bars - 1), 0)
    candidates = (
        candidates.withColumn(
            "confirmation_count",
            F.count("candidate_maturity_rank").over(recent_series_window),
        )
        .withColumn(
            "confirmation_distinct_maturity_count",
            F.size(
                F.array_distinct(
                    F.collect_list("candidate_maturity_rank").over(recent_series_window)
                )
            ),
        )
        .withColumn(
            "confirmed_candidate_maturity_rank",
            F.when(
                (F.col("ts_index") > 1)
                & (F.col("candidate_share") >= F.lit(policy.candidate_share_min))
                & (F.col("confirmation_count") >= F.lit(policy.confirmation_bars))
                & (F.col("confirmation_distinct_maturity_count") == F.lit(1)),
                F.col("candidate_maturity_rank"),
            ),
        )
    )
    timeline = candidates.withColumn(
        "effective_candidate_maturity_rank",
        F.lag("confirmed_candidate_maturity_rank").over(series_order),
    ).withColumn(
        "decision_ts",
        F.lag(F.when(F.col("confirmed_candidate_maturity_rank").isNotNull(), F.col("ts"))).over(
            series_order
        ),
    )
    active_maturity = F.greatest(
        F.col("first_maturity_rank"),
        F.max("effective_candidate_maturity_rank").over(
            series_order.rowsBetween(Window.unboundedPreceding, 0)
        ),
    )
    return (
        timeline.withColumn("active_maturity_rank", active_maturity)
        .withColumn("active_contract_id", F.lit(None).cast("string"))
        .withColumn("old_reference_ts", F.col("decision_ts"))
        .select(
            "instrument_id",
            "timeframe",
            "ts",
            "ts_index",
            "series_input_row_count",
            "candidate_contract_id",
            "candidate_maturity_rank",
            "active_contract_id",
            "active_maturity_rank",
            "decision_ts",
            "old_reference_ts",
        )
    )


def _build_native_qc(
    *,
    keyed_bars,
    continuous_bars,
    timeline_with_rolls,
    roll_events,
    policy,
    dataset_version,
    run_id,
    created_at,
):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    base_qc = keyed_bars.withColumn(
        "duplicate_row_count",
        F.count(F.lit(1)).over(
            Window.partitionBy("contract_id", "instrument_id", "timeframe", "ts")
        ),
    )
    qc_failures = base_qc.groupBy("instrument_id", "timeframe").agg(
        F.count(F.lit(1)).cast("long").alias("input_row_count"),
        F.sum(F.when(F.col("duplicate_row_count") > 1, F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("duplicate_key_count"),
        F.sum(
            F.when(
                (F.col("high") < F.greatest(F.col("open"), F.col("close")))
                | (F.col("low") > F.least(F.col("open"), F.col("close"))),
                F.lit(1),
            ).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("ohlc_error_count"),
        F.sum(
            F.when((F.col("volume") < 0) | (F.col("open_interest") < 0), F.lit(1)).otherwise(
                F.lit(0)
            )
        )
        .cast("long")
        .alias("negative_volume_oi_count"),
        F.sum(F.when(F.col("maturity_rank").isNull(), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("maturity_parse_error_count"),
    )
    output_duplicate_qc = (
        continuous_bars.withColumn(
            "output_duplicate_row_count",
            F.count(F.lit(1)).over(Window.partitionBy("instrument_id", "timeframe", "ts")),
        )
        .groupBy("instrument_id", "timeframe")
        .agg(
            F.sum(F.when(F.col("output_duplicate_row_count") > 1, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("output_duplicate_key_count")
        )
    )
    missing_active = timeline_with_rolls.groupBy("instrument_id", "timeframe").agg(
        F.sum(F.when(F.col("active_contract_id").isNull(), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("missing_active_bar_count")
    )
    event_qc = roll_events.groupBy("instrument_id", "timeframe").agg(
        F.count(F.lit(1)).cast("long").alias("roll_event_count"),
        F.sum(
            F.when(
                F.col("old_reference_price").isNull() | F.col("new_reference_price").isNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("missing_reference_price_count"),
        F.max(F.abs(F.col("additive_gap"))).alias("gap_abs_max"),
        F.avg(F.abs(F.col("additive_gap"))).alias("gap_abs_mean"),
    )
    output_counts = continuous_bars.groupBy("instrument_id", "timeframe").agg(
        F.count(F.lit(1)).cast("long").alias("output_row_count")
    )
    qc = (
        qc_failures.join(missing_active, ["instrument_id", "timeframe"], "left")
        .join(event_qc, ["instrument_id", "timeframe"], "left")
        .join(output_counts, ["instrument_id", "timeframe"], "left")
        .join(output_duplicate_qc, ["instrument_id", "timeframe"], "left")
        .withColumn("dataset_version", F.lit(dataset_version))
        .withColumn("roll_policy_version", F.lit(policy.roll_policy_version))
        .withColumn("adjustment_policy_version", F.lit(policy.adjustment_policy_version))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("started_at", F.lit(created_at))
        .withColumn("completed_at", F.lit(_policy_timestamp()))
        .withColumn("timeline_error_count", F.lit(0).cast("long"))
        .withColumn("future_causality_violation_count", F.lit(0).cast("long"))
        .withColumn(
            "duplicate_key_count",
            (
                F.coalesce(F.col("duplicate_key_count"), F.lit(0))
                + F.coalesce(F.col("output_duplicate_key_count"), F.lit(0))
            ).cast("long"),
        )
        .withColumn(
            "missing_active_bar_count",
            F.coalesce("missing_active_bar_count", F.lit(0)).cast("long"),
        )
        .withColumn("roll_event_count", F.coalesce("roll_event_count", F.lit(0)).cast("long"))
        .withColumn(
            "missing_reference_price_count",
            F.coalesce("missing_reference_price_count", F.lit(0)).cast("long")
            + F.col("maturity_parse_error_count"),
        )
        .withColumn("output_row_count", F.coalesce("output_row_count", F.lit(0)).cast("long"))
        .withColumn("gap_abs_max", F.coalesce("gap_abs_max", F.lit(0.0)))
        .withColumn("gap_abs_mean", F.coalesce("gap_abs_mean", F.lit(0.0)))
        .withColumn(
            "blocked_reason",
            F.when(F.col("missing_active_bar_count") > 0, F.lit("missing_active_bar_count"))
            .when(F.col("duplicate_key_count") > 0, F.lit("duplicate_key_count"))
            .when(F.col("ohlc_error_count") > 0, F.lit("ohlc_error_count"))
            .when(F.col("negative_volume_oi_count") > 0, F.lit("negative_volume_oi_count"))
            .when(
                F.col("missing_reference_price_count") > 0, F.lit("missing_reference_price_count")
            ),
        )
        .withColumn(
            "status",
            F.when(F.col("blocked_reason").isNotNull(), F.lit("BLOCKED")).otherwise(F.lit("PASS")),
        )
        .drop("output_duplicate_key_count")
    )
    return qc


def _build_spark_native_tables(
    *,
    spark: object,
    canonical_bars_path: Path,
    canonical_contract_economics_path: Path | None = None,
    dataset_version: str,
    policy: ContinuousFrontPolicy,
    run_id: str,
    instrument_ids: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str | None,
    end_ts: str | None,
    refresh_windows: tuple[ContinuousFrontRefreshWindow, ...] = (),
    calendar_roll_schedule=None,
):
    from pyspark.sql import Window  # type: ignore[import-not-found]
    from pyspark.sql import functions as F

    created_at = _policy_timestamp()
    scoped_instrument_ids = (
        _unique_scope_values(refresh_windows, "instrument_id")
        if refresh_windows
        else instrument_ids
    )
    scoped_timeframes = (
        _unique_scope_values(refresh_windows, "timeframe") if refresh_windows else timeframes
    )
    raw_bars = _load_filtered_bars(
        spark=spark,
        canonical_bars_path=canonical_bars_path,
        instrument_ids=scoped_instrument_ids,
        timeframes=scoped_timeframes,
        start_ts=None if refresh_windows else start_ts,
        end_ts=None if refresh_windows else end_ts,
        # Roll numbering and predecessor state require the causal prefix. A
        # correction can also change later rolls, so filter only the outputs.
        refresh_windows=tuple(
            replace(window, start_ts=None, end_ts=None) for window in refresh_windows
        ),
    )
    bars = _filter_policy_session(raw_bars, policy)
    keyed_bars = _with_maturity_columns(bars)
    timeline = _active_timeline(
        bars=bars, policy=policy, calendar_roll_schedule=calendar_roll_schedule
    )
    tl = timeline.alias("tl")
    kb = keyed_bars.select(
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest",
        "maturity_rank",
    ).alias("kb")
    active_joined = tl.join(
        kb,
        on=(
            (F.col("tl.instrument_id") == F.col("kb.instrument_id"))
            & (F.col("tl.timeframe") == F.col("kb.timeframe"))
            & (F.col("tl.ts") == F.col("kb.ts"))
            & (
                (
                    F.col("tl.active_contract_id").isNotNull()
                    & (F.col("tl.active_contract_id") == F.col("kb.contract_id"))
                )
                | (
                    F.col("tl.active_contract_id").isNull()
                    & (F.col("tl.active_maturity_rank") == F.col("kb.maturity_rank"))
                )
            )
        ),
        how="left",
    ).select(
        F.col("tl.instrument_id").alias("instrument_id"),
        F.col("tl.timeframe").alias("timeframe"),
        F.col("tl.ts").alias("ts"),
        F.col("tl.ts_index").alias("ts_index"),
        F.col("tl.series_input_row_count").alias("series_input_row_count"),
        F.col("tl.candidate_contract_id").alias("candidate_contract_id"),
        F.col("tl.candidate_maturity_rank").alias("candidate_maturity_rank"),
        F.col("tl.active_maturity_rank").alias("active_maturity_rank"),
        F.col("tl.decision_ts").alias("decision_ts"),
        F.col("tl.old_reference_ts").alias("old_reference_ts"),
        F.coalesce(F.col("tl.active_contract_id"), F.col("kb.contract_id")).alias(
            "active_contract_id"
        ),
        F.col("kb.open").alias("native_open"),
        F.col("kb.high").alias("native_high"),
        F.col("kb.low").alias("native_low"),
        F.col("kb.close").alias("native_close"),
        F.col("kb.volume").alias("native_volume"),
        F.col("kb.open_interest").alias("native_open_interest"),
    )
    series_order = Window.partitionBy("instrument_id", "timeframe").orderBy("ts")
    previous_active = F.lag("active_contract_id").over(series_order)
    timeline_with_rolls = active_joined.withColumn(
        "previous_active_contract_id", previous_active
    ).withColumn(
        "is_roll_bar",
        F.col("active_contract_id").isNotNull()
        & F.col("previous_active_contract_id").isNotNull()
        & (F.col("active_contract_id") != F.col("previous_active_contract_id")),
    )
    event_base = (
        timeline_with_rolls.where(F.col("is_roll_bar"))
        .select(
            "instrument_id",
            "timeframe",
            F.col("previous_active_contract_id").alias("old_contract_id"),
            F.col("active_contract_id").alias("new_contract_id"),
            F.col("decision_ts").alias("decision_ts"),
            F.col("old_reference_ts").alias("old_reference_ts"),
            F.col("ts").alias("effective_ts"),
            F.when(F.lit(policy.roll_policy_mode == "calendar_expiry_v1"), F.col("ts"))
            .otherwise(F.col("decision_ts"))
            .alias("new_reference_ts"),
        )
        .where(F.col("decision_ts").isNotNull())
    )
    old_refs = keyed_bars.select(
        F.col("instrument_id").alias("old_instrument_id"),
        F.col("timeframe").alias("old_timeframe"),
        F.col("ts").alias("old_ts"),
        F.col("contract_id").alias("old_contract_id_ref"),
        F.col("close").alias("old_reference_price"),
    )
    new_refs = keyed_bars.select(
        F.col("instrument_id").alias("new_instrument_id"),
        F.col("timeframe").alias("new_timeframe"),
        F.col("ts").alias("new_ts"),
        F.col("contract_id").alias("new_contract_id_ref"),
        F.col("close").alias("new_reference_price"),
    )
    event_order = Window.partitionBy("instrument_id", "timeframe").orderBy("effective_ts")
    eb = event_base.alias("eb")
    old_ref = old_refs.alias("old_ref")
    new_ref = new_refs.alias("new_ref")
    roll_events = (
        eb.join(
            old_ref,
            (F.col("eb.instrument_id") == F.col("old_ref.old_instrument_id"))
            & (F.col("eb.timeframe") == F.col("old_ref.old_timeframe"))
            & (F.col("eb.old_reference_ts") == F.col("old_ref.old_ts"))
            & (F.col("eb.old_contract_id") == F.col("old_ref.old_contract_id_ref")),
            "left",
        )
        .join(
            new_ref,
            (F.col("eb.instrument_id") == F.col("new_ref.new_instrument_id"))
            & (F.col("eb.timeframe") == F.col("new_ref.new_timeframe"))
            & (F.col("eb.new_reference_ts") == F.col("new_ref.new_ts"))
            & (F.col("eb.new_contract_id") == F.col("new_ref.new_contract_id_ref")),
            "left",
        )
        .select(
            F.col("eb.instrument_id").alias("instrument_id"),
            F.col("eb.timeframe").alias("timeframe"),
            F.col("eb.old_contract_id").alias("old_contract_id"),
            F.col("eb.new_contract_id").alias("new_contract_id"),
            F.col("eb.decision_ts").alias("decision_ts"),
            F.col("eb.effective_ts").alias("effective_ts"),
            F.col("eb.old_reference_ts").alias("last_old_bar_ts"),
            F.col("old_reference_price"),
            F.col("new_reference_price"),
        )
        .withColumn("roll_sequence", F.row_number().over(event_order))
        .withColumn(
            "roll_event_id",
            F.concat_ws(
                "-",
                F.lit("CFRSPARK"),
                F.lit(dataset_version),
                F.col("instrument_id"),
                F.col("timeframe"),
                F.format_string("%04d", F.col("roll_sequence")),
            ),
        )
        .withColumn(
            "additive_gap", roll_gap_expression("new_reference_price", "old_reference_price")
        )
        .withColumn("dataset_version", F.lit(dataset_version))
        .withColumn("roll_policy_version", F.lit(policy.roll_policy_version))
        .withColumn("adjustment_policy_version", F.lit(policy.adjustment_policy_version))
        .withColumn("first_new_bar_ts", F.col("effective_ts"))
        .withColumn("ratio_gap", F.lit(None).cast("double"))
        .withColumn("old_reference_source", F.lit(policy.reference_price_policy))
        .withColumn("new_reference_source", F.lit(policy.reference_price_policy))
        .withColumn("roll_reason", F.lit(policy.roll_policy_mode))
        .withColumn(
            "causality_watermark_ts",
            F.when(
                F.lit(policy.roll_policy_mode == "calendar_expiry_v1"), F.col("effective_ts")
            ).otherwise(F.col("decision_ts")),
        )
        .withColumn("created_at", F.lit(created_at))
    )
    ladder = (
        roll_events.select(
            "dataset_version",
            "roll_policy_version",
            "adjustment_policy_version",
            "instrument_id",
            "timeframe",
            "roll_event_id",
            "roll_sequence",
            "effective_ts",
            "additive_gap",
            "created_at",
        )
        .withColumn(
            "cumulative_offset_before",
            F.col("additive_gap"),
        )
        .withColumn("cumulative_offset_after", F.lit(0.0))
        .withColumn("ratio_gap", F.lit(None).cast("double"))
        .withColumn("ratio_factor_before", F.lit(None).cast("double"))
        .withColumn("ratio_factor_after", F.lit(None).cast("double"))
    )
    event_for_bars = roll_events.select(
        F.col("instrument_id").alias("event_instrument_id"),
        F.col("timeframe").alias("event_timeframe"),
        F.col("effective_ts").alias("event_effective_ts"),
        F.col("old_contract_id").alias("event_old_contract_id"),
        F.col("roll_event_id").alias("event_roll_event_id"),
        F.col("roll_sequence").alias("event_roll_sequence"),
    )
    bar_base = (
        timeline_with_rolls.alias("bar")
        .where(F.col("active_contract_id").isNotNull())
        .join(
            event_for_bars.alias("event"),
            (F.col("bar.instrument_id") == F.col("event.event_instrument_id"))
            & (F.col("bar.timeframe") == F.col("event.event_timeframe"))
            & (F.col("bar.ts") == F.col("event.event_effective_ts")),
            "left",
        )
    )
    bar_order = Window.partitionBy("instrument_id", "timeframe").orderBy("ts")
    roll_epoch = F.sum(
        F.when(F.col("event_roll_sequence").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    ).over(bar_order.rowsBetween(Window.unboundedPreceding, 0))
    bars_with_offsets = (
        bar_base.withColumn("roll_epoch", roll_epoch)
        .withColumn("cumulative_additive_offset", F.lit(0.0))
        .withColumn(
            "roll_event_id",
            F.last("event_roll_event_id", True).over(
                bar_order.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )
        .withColumn(
            "previous_contract_id",
            F.last("event_old_contract_id", True).over(
                bar_order.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )
        .withColumn(
            "bars_since_roll",
            F.row_number().over(
                Window.partitionBy("instrument_id", "timeframe", "roll_epoch").orderBy("ts")
            )
            - F.lit(1),
        )
    )
    if canonical_contract_economics_path is not None:
        econ = (
            spark.read.format("delta")
            .load(str(canonical_contract_economics_path))
            .select(
                F.col("contract_id").alias("econ_contract_id"),
                F.regexp_replace(F.col("contract_id").cast("string"), "@MOEX$", "").alias(
                    "econ_contract_join_key"
                ),
                F.col("effective_from_ts").alias("econ_effective_from_ts"),
                F.col("effective_to_ts").alias("econ_effective_to_ts"),
                F.col("step_price_rub").alias("execution_step_price_rub"),
                F.col("lot_volume").alias("execution_lot_volume"),
                F.col("tick_value_currency").alias("execution_tick_value_currency"),
                F.col("margin_required_estimate").alias("execution_margin_required_estimate"),
                F.col("margin_buffer_pct").alias("execution_margin_buffer_pct"),
                F.col("effective_from_ts").alias("economics_effective_from_ts"),
                F.col("model_version").alias("economics_model_version"),
            )
        )
        bars_with_execution = (
            bars_with_offsets.alias("bar")
            .join(
                econ.alias("econ"),
                (
                    F.regexp_replace(F.col("bar.active_contract_id").cast("string"), "@MOEX$", "")
                    == F.col("econ.econ_contract_join_key")
                )
                & (F.col("bar.ts") >= F.col("econ.econ_effective_from_ts"))
                & (
                    F.col("econ.econ_effective_to_ts").isNull()
                    | (F.col("bar.ts") < F.col("econ.econ_effective_to_ts"))
                ),
                "left",
            )
            .withColumn(
                "__economics_asof_rank",
                F.row_number().over(
                    Window.partitionBy(
                        F.col("bar.instrument_id"),
                        F.col("bar.timeframe"),
                        F.col("bar.ts"),
                        F.col("bar.active_contract_id"),
                    ).orderBy(F.col("econ.econ_effective_from_ts").desc_nulls_last())
                ),
            )
            .where(F.col("__economics_asof_rank") == F.lit(1))
            .select(
                F.col("bar.*"),
                F.col("econ.execution_step_price_rub"),
                F.col("econ.execution_lot_volume"),
                F.col("econ.execution_tick_value_currency"),
                F.col("econ.execution_margin_required_estimate"),
                F.col("econ.execution_margin_buffer_pct"),
                F.col("econ.economics_effective_from_ts"),
                F.col("econ.economics_model_version"),
            )
        )
    else:
        bars_with_execution = (
            bars_with_offsets.withColumn("execution_step_price_rub", F.lit(None).cast("double"))
            .withColumn("execution_lot_volume", F.lit(None).cast("double"))
            .withColumn("execution_tick_value_currency", F.lit(None).cast("double"))
            .withColumn("execution_margin_required_estimate", F.lit(None).cast("double"))
            .withColumn("execution_margin_buffer_pct", F.lit(None).cast("double"))
            .withColumn("economics_effective_from_ts", F.lit(None).cast("timestamp"))
            .withColumn("economics_model_version", F.lit(None).cast("string"))
        )
    continuous_bars = bars_with_execution.select(
        F.lit(dataset_version).alias("dataset_version"),
        F.lit(policy.roll_policy_version).alias("roll_policy_version"),
        F.lit(policy.adjustment_policy_version).alias("adjustment_policy_version"),
        F.col("instrument_id"),
        F.col("timeframe"),
        F.col("ts"),
        F.col("active_contract_id"),
        "previous_contract_id",
        F.col("candidate_contract_id"),
        "roll_epoch",
        "roll_event_id",
        F.col("is_roll_bar"),
        F.col("is_roll_bar").alias("is_first_bar_after_roll"),
        "bars_since_roll",
        "native_open",
        "native_high",
        "native_low",
        "native_close",
        F.col("native_volume").cast("long").alias("native_volume"),
        F.col("native_open_interest").cast("long").alias("native_open_interest"),
        F.col("native_open").alias("continuous_open"),
        F.col("native_high").alias("continuous_high"),
        F.col("native_low").alias("continuous_low"),
        F.col("native_close").alias("continuous_close"),
        F.lit(policy.adjustment_mode).alias("adjustment_mode"),
        "cumulative_additive_offset",
        F.lit(None).cast("double").alias("ratio_factor"),
        F.lit(policy.price_space).alias("price_space"),
        F.when(
            F.lit(policy.roll_policy_mode == "calendar_expiry_v1") & F.col("is_roll_bar"),
            F.col("ts"),
        )
        .otherwise(F.coalesce(F.col("decision_ts"), F.col("ts")))
        .alias("causality_watermark_ts"),
        F.col("series_input_row_count").cast("long").alias("input_row_count"),
        "execution_step_price_rub",
        "execution_lot_volume",
        "execution_tick_value_currency",
        "execution_margin_required_estimate",
        "execution_margin_buffer_pct",
        "economics_effective_from_ts",
        "economics_model_version",
        F.lit(created_at).alias("created_at"),
    )
    qc = _build_native_qc(
        keyed_bars=keyed_bars,
        continuous_bars=continuous_bars,
        timeline_with_rolls=timeline_with_rolls,
        roll_events=roll_events,
        policy=policy,
        dataset_version=dataset_version,
        run_id=run_id,
        created_at=created_at,
    )
    tables = {
        "continuous_front_bars": continuous_bars,
        "continuous_front_roll_events": roll_events,
        "continuous_front_adjustment_ladder": ladder,
        "continuous_front_qc_report": qc,
    }
    if refresh_windows:
        tail_windows = tuple(replace(window, end_ts=None) for window in refresh_windows)
        for name in CONTINUOUS_FRONT_TABLES:
            if name != "continuous_front_qc_report":
                tables[name] = _filter_to_refresh_windows(
                    tables[name],
                    tail_windows,
                    ts_column="ts" if name == "continuous_front_bars" else "effective_ts",
                )
    return tables


def _cast_dataframe_to_contract(dataframe, manifest_entry: dict[str, object]):
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    columns = dict(manifest_entry["columns"])
    selected = []
    for column_name, type_name in columns.items():
        spark_type = _spark_sql_type(str(type_name))
        if column_name in dataframe.columns:
            selected.append(F.col(column_name).cast(spark_type).alias(column_name))
        else:
            selected.append(F.lit(None).cast(spark_type).alias(column_name))
    return dataframe.select(*selected)


def _write_spark_dataframe_tables(
    *,
    spark: object,
    output_dir: Path,
    tables: dict[str, object],
    dataset_version: str,
    instrument_ids: tuple[str, ...] = (),
    timeframes: tuple[str, ...] = (),
    start_ts: str | None = None,
    end_ts: str | None = None,
    refresh_windows: tuple[ContinuousFrontRefreshWindow, ...] = (),
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    contract = continuous_front_store_contract()
    output_paths: dict[str, str] = {}
    for table_name in CONTINUOUS_FRONT_TABLES:
        table_path = output_dir / f"{table_name}.delta"
        table_path.parent.mkdir(parents=True, exist_ok=True)
        dataframe = tables.get(table_name)
        if dataframe is None:
            casted = spark.createDataFrame(
                [], schema=_spark_schema(dict(contract[table_name]["columns"]))
            )
        else:
            casted = _cast_dataframe_to_contract(dataframe, dict(contract[table_name]))
        if not has_delta_log(table_path):
            writer = (
                casted.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            )
            partition_by = list(contract[table_name].get("partition_by") or [])
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(str(table_path))
        else:
            from delta.tables import DeltaTable  # type: ignore[import-not-found]

            DeltaTable.forPath(spark, str(table_path)).delete(
                _scoped_delete_disjunction(
                    _continuous_front_replace_filter_groups(
                        table_name=table_name,
                        dataset_version=dataset_version,
                        instrument_ids=instrument_ids,
                        timeframes=timeframes,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        refresh_windows=refresh_windows,
                    )
                )
            )
            casted.write.format("delta").mode("append").option("mergeSchema", "true").save(
                str(table_path)
            )
        output_paths[table_name] = table_path.as_posix()
    return output_paths


def _validate_spark_promoted_contracts(output_paths: dict[str, str]) -> list[str]:
    errors: list[str] = []
    contract = continuous_front_store_contract()
    for table_name in CONTINUOUS_FRONT_TABLES:
        table_path = Path(output_paths[table_name])
        if not has_delta_log(table_path):
            errors.append(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
            continue
        expected_columns = list(dict(contract[table_name]["columns"]).keys())
        actual_columns = list(delta_table_columns(table_path))
        missing = [column for column in expected_columns if column not in actual_columns]
        extra = [column for column in actual_columns if column not in expected_columns]
        if missing or extra:
            details: list[str] = []
            if missing:
                details.append("missing columns: " + ", ".join(missing))
            if extra:
                details.append("extra columns: " + ", ".join(extra))
            errors.append(f"schema mismatch for `{table_name}` ({'; '.join(details)})")
    return errors


def _qc_rows_from_spark(qc_dataframe) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in qc_dataframe.toLocalIterator():
        payload = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        rows.append({str(key): value for key, value in payload.items()})
    return rows


@admitted_compute(
    output="output_dir",
    sources=(
        "canonical_bars_path",
        "canonical_session_calendar_path",
        "canonical_roll_map_path",
        "canonical_contract_economics_path",
    ),
    input_roots=(),
    windows="refresh_windows",
)
def run_continuous_front_spark_job(
    *,
    canonical_bars_path: Path,
    canonical_session_calendar_path: Path,
    canonical_roll_map_path: Path,
    output_dir: Path,
    dataset_version: str,
    policy: ContinuousFrontPolicy | None = None,
    run_id: str = "continuous_front_refresh",
    instrument_ids: tuple[str, ...] = (),
    timeframes: tuple[str, ...] = (),
    start_ts: str | None = None,
    end_ts: str | None = None,
    refresh_windows: Sequence[Mapping[str, object] | ContinuousFrontRefreshWindow] | None = None,
    canonical_contract_economics_path: Path | None = None,
    execution_economics_required: bool = False,
    calendar_roll_schedule=None,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory: Callable[[str, str], object] | None = None,
) -> dict[str, object]:
    spec = ContinuousFrontSparkJobSpec()
    resolved_policy = policy or ContinuousFrontPolicy()
    _require_spark_native_policy(resolved_policy)
    normalized_refresh_windows = tuple(
        replace(window, end_ts=None) for window in _normalize_refresh_windows(refresh_windows)
    )
    scoped_instrument_ids = (
        _unique_scope_values(normalized_refresh_windows, "instrument_id")
        if normalized_refresh_windows
        else instrument_ids
    )
    scoped_timeframes = (
        _unique_scope_values(normalized_refresh_windows, "timeframe")
        if normalized_refresh_windows
        else timeframes
    )
    if execution_economics_required and canonical_contract_economics_path is None:
        raise RuntimeError(
            "canonical_contract_economics_path is required when execution_economics_required=True"
        )
    if resolved_policy.roll_policy_mode == "calendar_expiry_v1":
        validate_calendar_sources(
            output_dir,
            calendar_roll_schedule,
            offset=resolved_policy.calendar_roll_offset_trading_days,
        )
    spark_factory = spark_session_factory or _create_spark_session
    stage_timings: StageTimings = {}
    stage_started = stage_timer()
    spark = spark_factory(spec.app_name, spark_master)
    cache_cleanup = ExitStack()
    record_stage_timing(stage_timings, "start_spark", stage_started, master=spark_master)
    try:
        stage_started = stage_timer()
        for table_path in (
            canonical_bars_path,
            canonical_session_calendar_path,
            canonical_roll_map_path,
        ):
            if not has_delta_log(table_path):
                raise RuntimeError(f"missing canonical delta table: {table_path.as_posix()}")
        if canonical_contract_economics_path is not None and not has_delta_log(
            canonical_contract_economics_path
        ):
            raise RuntimeError(
                "missing canonical contract economics delta table: "
                f"{canonical_contract_economics_path.as_posix()}"
            )
        record_stage_timing(stage_timings, "validate_inputs", stage_started)

        stage_started = stage_timer()
        spark.read.format("delta").load(str(canonical_session_calendar_path)).select(
            "instrument_id"
        ).count()
        spark.read.format("delta").load(str(canonical_roll_map_path)).select(
            "instrument_id"
        ).count()
        record_stage_timing(stage_timings, "source_sidecar_counts", stage_started)

        stage_started = stage_timer()
        tables = _build_spark_native_tables(
            spark=spark,
            canonical_bars_path=canonical_bars_path,
            canonical_contract_economics_path=canonical_contract_economics_path,
            dataset_version=dataset_version,
            policy=resolved_policy,
            run_id=run_id,
            instrument_ids=scoped_instrument_ids,
            timeframes=scoped_timeframes,
            start_ts=start_ts,
            end_ts=end_ts,
            refresh_windows=normalized_refresh_windows,
            calendar_roll_schedule=calendar_roll_schedule,
        )
        from pyspark import StorageLevel

        for dataframe in tables.values():
            dataframe.persist(StorageLevel.DISK_ONLY)
            cache_cleanup.callback(dataframe.unpersist)
        record_stage_timing(stage_timings, "build_spark_native_tables", stage_started)

        staging_dir = output_dir / "_staging" / "continuous_front_spark" / run_id
        stage_started = stage_timer()
        staged_output_paths = _write_spark_dataframe_tables(
            spark=spark,
            output_dir=staging_dir,
            tables=tables,
            dataset_version=dataset_version,
            instrument_ids=scoped_instrument_ids,
            timeframes=scoped_timeframes,
            start_ts=start_ts,
            end_ts=end_ts,
            refresh_windows=normalized_refresh_windows,
        )
        record_stage_timing(
            stage_timings,
            "write_staging_tables",
            stage_started,
            table_count=len(staged_output_paths),
        )

        stage_started = stage_timer()
        qc_rows = _qc_rows_from_spark(tables["continuous_front_qc_report"])
        blocking_rows = [row for row in qc_rows if row.get("status") == "BLOCKED"]
        if blocking_rows:
            blocked = "; ".join(
                f"{row.get('instrument_id')}|{row.get('timeframe')}:{row.get('blocked_reason')}"
                for row in blocking_rows
            )
            raise RuntimeError(f"continuous_front Spark QC failed closed: {blocked}")
        record_stage_timing(
            stage_timings,
            "qc",
            stage_started,
            qc_row_count=len(qc_rows),
            blocking_row_count=len(blocking_rows),
        )

        stage_started = stage_timer()
        output_paths = _write_spark_dataframe_tables(
            spark=spark,
            output_dir=output_dir,
            tables=tables,
            dataset_version=dataset_version,
            instrument_ids=scoped_instrument_ids,
            timeframes=scoped_timeframes,
            start_ts=start_ts,
            end_ts=end_ts,
            refresh_windows=normalized_refresh_windows,
        )
        record_stage_timing(
            stage_timings,
            "write_output_tables",
            stage_started,
            table_count=len(output_paths),
        )

        stage_started = stage_timer()
        contract_errors = _validate_spark_promoted_contracts(output_paths)
        if contract_errors:
            raise RuntimeError(
                "continuous_front Spark contract validation failed: " + "; ".join(contract_errors)
            )
        record_stage_timing(
            stage_timings,
            "contract_validation",
            stage_started,
            error_count=len(contract_errors),
        )

        stage_started = stage_timer()
        rows_by_table = {
            table_name: count_delta_table_rows(
                Path(path), filters=[("dataset_version", "=", dataset_version)]
            )
            for table_name, path in output_paths.items()
        }
        record_stage_timing(
            stage_timings,
            "row_counts",
            stage_started,
            table_count=len(rows_by_table),
            row_count=sum(rows_by_table.values()),
        )
        return {
            "success": True,
            "status": "PASS",
            "run_id": run_id,
            "dataset_version": dataset_version,
            "policy": resolved_policy.to_config_dict(),
            "output_paths": output_paths,
            "staged_output_paths": staged_output_paths,
            "rows_by_table": rows_by_table,
            "qc_rows": qc_rows,
            "contract_check_errors": contract_errors,
            "stage_timings": stage_timings,
            "refresh_window_count": len(normalized_refresh_windows),
            "delta_manifest": continuous_front_store_contract(),
            "spark_profile": {
                "app_name": spec.app_name,
                "master": spark_master,
                "delta_reader": "spark",
                "delta_writer": "spark",
                "causal_roll_engine": "spark-native-window-batch",
                "refresh_window_count": len(normalized_refresh_windows),
                "sql_plan": build_continuous_front_sql_plan(),
            },
        }
    finally:
        try:
            cache_cleanup.close()
        finally:
            stop = getattr(spark, "stop", None)
            if callable(stop):
                stop()
