from __future__ import annotations

from dataclasses import dataclass
import json

from trading_advisor_3000.product_plane.contracts import Timeframe


GOLD_FEATURE_NUMERIC_COLUMNS: tuple[str, ...] = (
    "last_close",
    "last_volume",
    "atr_14",
    "natr_14",
    "ema_12",
    "ema_26",
    "sma_20",
    "sma_50",
    "rsi_14",
    "macd",
    "macd_signal",
    "macd_hist",
    "bb_width",
    "bb_percent_b",
    "adx_14",
    "stoch_k",
    "stoch_d",
    "cci_20",
    "willr_14",
    "mom_10",
    "roc_10",
    "mfi_14",
    "donchian_position",
    "trend_score",
    "momentum_score",
    "volatility_score",
    "volume_pressure",
    "supertrend_direction",
)
GOLD_FEATURE_STRING_COLUMNS: tuple[str, ...] = ("breakout_state",)
GOLD_FEATURE_TYPED_COLUMNS: tuple[str, ...] = GOLD_FEATURE_NUMERIC_COLUMNS + GOLD_FEATURE_STRING_COLUMNS


def _required_text(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty string")
    return value.strip()


def _required_number(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    return float(value)


def _required_non_negative(name: str, value: object) -> float:
    normalized = _required_number(name, value)
    if normalized < 0:
        raise ValueError(f"{name} must be non-negative")
    return normalized


def _required_features_json(value: object) -> dict[str, float | str]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("features_json must be an object")
    normalized: dict[str, float | str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("features_json keys must be non-empty strings")
        if isinstance(item, bool) or not isinstance(item, (int, float, str)):
            raise ValueError(f"features_json[{key}] must be number or string")
        normalized[key] = float(item) if isinstance(item, (int, float)) and not isinstance(item, bool) else item
    return normalized


def _require_keys(
    payload: dict[str, object],
    *,
    required: set[str],
    optional: set[str] | None = None,
) -> None:
    optional = optional or set()
    allowed = required | optional
    extra = sorted(set(payload) - allowed)
    if extra:
        raise ValueError(f"unsupported fields: {', '.join(extra)}")
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")


@dataclass(frozen=True)
class TechnicalIndicatorSnapshot:
    indicator_snapshot_id: str
    contract_id: str
    instrument_id: str
    timeframe: Timeframe
    ts: str
    indicator_set_version: str
    source_bar_fingerprint: str
    close: float
    volume: float
    atr_14: float
    natr_14: float
    ema_12: float
    ema_26: float
    sma_20: float
    sma_50: float
    rsi_14: float
    macd: float
    macd_signal: float
    macd_hist: float
    bb_lower: float
    bb_mid: float
    bb_upper: float
    bb_width: float
    bb_percent_b: float
    adx_14: float
    dmp_14: float
    dmn_14: float
    stoch_k: float
    stoch_d: float
    cci_20: float
    willr_14: float
    mom_10: float
    roc_10: float
    obv: float
    mfi_14: float
    donchian_low_20: float
    donchian_mid_20: float
    donchian_high_20: float
    supertrend: float
    supertrend_direction: float
    rvol_20: float
    computed_at_utc: str
    indicator_values_json: dict[str, float | str]

    def __post_init__(self) -> None:
        if self.volume < 0:
            raise ValueError("volume must be non-negative")
        if self.atr_14 < 0:
            raise ValueError("atr_14 must be non-negative")
        if self.natr_14 < 0:
            raise ValueError("natr_14 must be non-negative")
        if self.bb_width < 0:
            raise ValueError("bb_width must be non-negative")
        if self.rvol_20 < 0:
            raise ValueError("rvol_20 must be non-negative")
        if self.donchian_low_20 > self.donchian_high_20:
            raise ValueError("donchian_low_20 must be <= donchian_high_20")

    def to_dict(self) -> dict[str, object]:
        return {
            "indicator_snapshot_id": self.indicator_snapshot_id,
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe.value,
            "ts": self.ts,
            "indicator_set_version": self.indicator_set_version,
            "source_bar_fingerprint": self.source_bar_fingerprint,
            "close": self.close,
            "volume": self.volume,
            "atr_14": self.atr_14,
            "natr_14": self.natr_14,
            "ema_12": self.ema_12,
            "ema_26": self.ema_26,
            "sma_20": self.sma_20,
            "sma_50": self.sma_50,
            "rsi_14": self.rsi_14,
            "macd": self.macd,
            "macd_signal": self.macd_signal,
            "macd_hist": self.macd_hist,
            "bb_lower": self.bb_lower,
            "bb_mid": self.bb_mid,
            "bb_upper": self.bb_upper,
            "bb_width": self.bb_width,
            "bb_percent_b": self.bb_percent_b,
            "adx_14": self.adx_14,
            "dmp_14": self.dmp_14,
            "dmn_14": self.dmn_14,
            "stoch_k": self.stoch_k,
            "stoch_d": self.stoch_d,
            "cci_20": self.cci_20,
            "willr_14": self.willr_14,
            "mom_10": self.mom_10,
            "roc_10": self.roc_10,
            "obv": self.obv,
            "mfi_14": self.mfi_14,
            "donchian_low_20": self.donchian_low_20,
            "donchian_mid_20": self.donchian_mid_20,
            "donchian_high_20": self.donchian_high_20,
            "supertrend": self.supertrend,
            "supertrend_direction": self.supertrend_direction,
            "rvol_20": self.rvol_20,
            "computed_at_utc": self.computed_at_utc,
            "indicator_values_json": self.indicator_values_json,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "TechnicalIndicatorSnapshot":
        _require_keys(
            payload,
            required={
                "indicator_snapshot_id",
                "contract_id",
                "instrument_id",
                "timeframe",
                "ts",
                "indicator_set_version",
                "source_bar_fingerprint",
                "close",
                "volume",
                "atr_14",
                "natr_14",
                "ema_12",
                "ema_26",
                "sma_20",
                "sma_50",
                "rsi_14",
                "macd",
                "macd_signal",
                "macd_hist",
                "bb_lower",
                "bb_mid",
                "bb_upper",
                "bb_width",
                "bb_percent_b",
                "adx_14",
                "dmp_14",
                "dmn_14",
                "stoch_k",
                "stoch_d",
                "cci_20",
                "willr_14",
                "mom_10",
                "roc_10",
                "obv",
                "mfi_14",
                "donchian_low_20",
                "donchian_mid_20",
                "donchian_high_20",
                "supertrend",
                "supertrend_direction",
                "rvol_20",
                "computed_at_utc",
                "indicator_values_json",
            },
        )
        return cls(
            indicator_snapshot_id=_required_text("indicator_snapshot_id", payload.get("indicator_snapshot_id")),
            contract_id=_required_text("contract_id", payload.get("contract_id")),
            instrument_id=_required_text("instrument_id", payload.get("instrument_id")),
            timeframe=Timeframe(_required_text("timeframe", payload.get("timeframe"))),
            ts=_required_text("ts", payload.get("ts")),
            indicator_set_version=_required_text("indicator_set_version", payload.get("indicator_set_version")),
            source_bar_fingerprint=_required_text("source_bar_fingerprint", payload.get("source_bar_fingerprint")),
            close=_required_number("close", payload.get("close")),
            volume=_required_non_negative("volume", payload.get("volume")),
            atr_14=_required_non_negative("atr_14", payload.get("atr_14")),
            natr_14=_required_non_negative("natr_14", payload.get("natr_14")),
            ema_12=_required_number("ema_12", payload.get("ema_12")),
            ema_26=_required_number("ema_26", payload.get("ema_26")),
            sma_20=_required_number("sma_20", payload.get("sma_20")),
            sma_50=_required_number("sma_50", payload.get("sma_50")),
            rsi_14=_required_number("rsi_14", payload.get("rsi_14")),
            macd=_required_number("macd", payload.get("macd")),
            macd_signal=_required_number("macd_signal", payload.get("macd_signal")),
            macd_hist=_required_number("macd_hist", payload.get("macd_hist")),
            bb_lower=_required_number("bb_lower", payload.get("bb_lower")),
            bb_mid=_required_number("bb_mid", payload.get("bb_mid")),
            bb_upper=_required_number("bb_upper", payload.get("bb_upper")),
            bb_width=_required_non_negative("bb_width", payload.get("bb_width")),
            bb_percent_b=_required_number("bb_percent_b", payload.get("bb_percent_b")),
            adx_14=_required_number("adx_14", payload.get("adx_14")),
            dmp_14=_required_number("dmp_14", payload.get("dmp_14")),
            dmn_14=_required_number("dmn_14", payload.get("dmn_14")),
            stoch_k=_required_number("stoch_k", payload.get("stoch_k")),
            stoch_d=_required_number("stoch_d", payload.get("stoch_d")),
            cci_20=_required_number("cci_20", payload.get("cci_20")),
            willr_14=_required_number("willr_14", payload.get("willr_14")),
            mom_10=_required_number("mom_10", payload.get("mom_10")),
            roc_10=_required_number("roc_10", payload.get("roc_10")),
            obv=_required_number("obv", payload.get("obv")),
            mfi_14=_required_number("mfi_14", payload.get("mfi_14")),
            donchian_low_20=_required_number("donchian_low_20", payload.get("donchian_low_20")),
            donchian_mid_20=_required_number("donchian_mid_20", payload.get("donchian_mid_20")),
            donchian_high_20=_required_number("donchian_high_20", payload.get("donchian_high_20")),
            supertrend=_required_number("supertrend", payload.get("supertrend")),
            supertrend_direction=_required_number(
                "supertrend_direction",
                payload.get("supertrend_direction"),
            ),
            rvol_20=_required_non_negative("rvol_20", payload.get("rvol_20")),
            computed_at_utc=_required_text("computed_at_utc", payload.get("computed_at_utc")),
            indicator_values_json=_required_features_json(payload.get("indicator_values_json")),
        )


@dataclass(frozen=True)
class FeatureSnapshot:
    snapshot_id: str
    contract_id: str
    instrument_id: str
    timeframe: Timeframe
    ts: str
    feature_set_version: str
    regime: str
    atr: float
    ema_fast: float
    ema_slow: float
    donchian_high: float
    donchian_low: float
    rvol: float
    features_json: dict[str, float | str]

    def __post_init__(self) -> None:
        if self.atr < 0:
            raise ValueError("atr must be non-negative")
        if self.rvol < 0:
            raise ValueError("rvol must be non-negative")
        if self.donchian_low > self.donchian_high:
            raise ValueError("donchian_low must be <= donchian_high")

    def to_dict(self) -> dict[str, object]:
        row: dict[str, object] = {
            "snapshot_id": self.snapshot_id,
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe.value,
            "ts": self.ts,
            "feature_set_version": self.feature_set_version,
            "regime": self.regime,
            "atr": self.atr,
            "ema_fast": self.ema_fast,
            "ema_slow": self.ema_slow,
            "donchian_high": self.donchian_high,
            "donchian_low": self.donchian_low,
            "rvol": self.rvol,
            "features_json": self.features_json,
        }
        for key in GOLD_FEATURE_NUMERIC_COLUMNS:
            row[key] = _required_number(key, self.features_json.get(key))
        for key in GOLD_FEATURE_STRING_COLUMNS:
            row[key] = _required_text(key, self.features_json.get(key))
        return row

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "FeatureSnapshot":
        _require_keys(
            payload,
            required={
                "snapshot_id",
                "contract_id",
                "instrument_id",
                "timeframe",
                "ts",
                "feature_set_version",
                "regime",
                "atr",
                "ema_fast",
                "ema_slow",
                "donchian_high",
                "donchian_low",
                "rvol",
                "features_json",
            },
            optional=set(GOLD_FEATURE_TYPED_COLUMNS),
        )
        return cls(
            snapshot_id=_required_text("snapshot_id", payload.get("snapshot_id")),
            contract_id=_required_text("contract_id", payload.get("contract_id")),
            instrument_id=_required_text("instrument_id", payload.get("instrument_id")),
            timeframe=Timeframe(_required_text("timeframe", payload.get("timeframe"))),
            ts=_required_text("ts", payload.get("ts")),
            feature_set_version=_required_text(
                "feature_set_version",
                payload.get("feature_set_version"),
            ),
            regime=_required_text("regime", payload.get("regime")),
            atr=_required_non_negative("atr", payload.get("atr")),
            ema_fast=_required_number("ema_fast", payload.get("ema_fast")),
            ema_slow=_required_number("ema_slow", payload.get("ema_slow")),
            donchian_high=_required_number("donchian_high", payload.get("donchian_high")),
            donchian_low=_required_number("donchian_low", payload.get("donchian_low")),
            rvol=_required_non_negative("rvol", payload.get("rvol")),
            features_json=_required_features_json(payload.get("features_json")),
        )
