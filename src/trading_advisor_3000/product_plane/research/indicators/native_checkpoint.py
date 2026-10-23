"""Bounded continuation of native pandas-ta calculations.

The library has no checkpoint argument. Private copies of its functions receive
state-aware EMA/RMA/cumulative helpers; library/module globals are never patched.
MACD's local numpy EMA and Supertrend's local loop require explicit adapters.
Their initialization and recurrence follow pandas-ta-classic 0.4.47. Native
functions remain the bootstrap and parity oracle. Unknown backends fail closed.
"""

from __future__ import annotations

import hashlib
import inspect
from functools import lru_cache
from types import FunctionType, SimpleNamespace

import numpy as np
import pandas as pd
import pandas_ta_classic as ta

FORMAT = "native-checkpoint.v1"
BUFFER = 512


class CheckpointUnavailable(ValueError):
    """A complete native recomputation is required, never approximate warmup."""


def identity():
    names = (
        "sma",
        "ema",
        "rma",
        "hma",
        "slope",
        "atr",
        "natr",
        "chop",
        "supertrend",
        "rsi",
        "roc",
        "mom",
        "obv",
        "mfi",
        "cmf",
        "vwma",
        "ad",
        "adosc",
        "efi",
        "pvt",
        "cci",
        "willr",
        "stochrsi",
        "uo",
        "ui",
        "stoch",
        "macd",
        "adx",
        "aroon",
        "donchian",
        "bbands",
        "kc",
        "ppo",
        "tsi",
        "trix",
        "kst",
        "pvo",
        "ma",
        "true_range",
        "hl2",
        "wma",
    )
    from pandas_ta_classic.utils import non_zero_range, signed_series

    return _source_identity(
        tuple(getattr(ta, name) for name in names) + (non_zero_range, signed_series),
        str(ta.version),
        str(ta.Imports.get("talib")),
    )


@lru_cache(maxsize=4)
def _source_identity(functions, version, backend):
    try:
        source = "".join(inspect.getsource(function) for function in functions)
    except (OSError, TypeError):
        return "unsupported-native-source"
    return hashlib.sha256((version + backend + source).encode()).hexdigest()


SUPPORTED_NATIVE_IDENTITY = "0c8f482072f357df2d562c5c230790a85116934949f864b0cb2885fcbacf52ac"


class NativeCheckpoint:
    def __init__(self, previous=None, cutoff=None):
        self.previous = previous or {}
        self.cutoff = cutoff
        if not isinstance(self.previous, dict):
            raise CheckpointUnavailable("malformed checkpoint object")
        if identity() != SUPPORTED_NATIVE_IDENTITY:
            raise CheckpointUnavailable("unsupported native indicator source/backend")
        if cutoff is not None and (
            self.previous.get("format") != FORMAT or self.previous.get("identity") != identity()
        ):
            raise CheckpointUnavailable("native checkpoint identity changed")
        if ta.Imports.get("talib"):
            raise CheckpointUnavailable("TA-Lib checkpoint backend is not implemented")
        self.streams = {}
        self.counter = 0
        self.clones = {}
        self.formula_rows = 0
        self.full_history_rows = 0

    def export(self):
        return dict(format=FORMAT, identity=identity(), streams=self.streams)

    def _key(self, name):
        key = f"{self.counter}:{name}"
        self.counter += 1
        return key

    def _remember(self, key, values, **extra):
        tail = values.iloc[-BUFFER:]
        self.streams[key] = dict(
            index=[int(i) for i in tail.index],
            values=[None if pd.isna(v) else float(v) for v in tail],
            **extra,
        )
        return values

    def _cached(self, key, index):
        streams = self.previous.get("streams", {})
        if not isinstance(streams, dict):
            raise CheckpointUnavailable("malformed hidden stream map")
        old = streams.get(key)
        if not isinstance(old, dict):
            raise CheckpointUnavailable(f"missing hidden stream {key}")
        try:
            if len(old["values"]) != len(old["index"]) or not old["index"]:
                raise ValueError("invalid hidden stream lengths")
            cached = pd.Series(old["values"], index=old["index"], dtype=float)
            if not cached.index.is_unique or not cached.index.is_monotonic_increasing:
                raise ValueError("invalid hidden stream keys")
        except (KeyError, TypeError, ValueError) as error:
            raise CheckpointUnavailable("malformed hidden stream") from error
        prefix = index[index < self.cutoff]
        if not prefix.isin(cached.index).all():
            raise CheckpointUnavailable(f"hidden stream buffer exhausted: {key}")
        return old, cached.reindex(index)

    def _smooth(self, name, close, length=None, **kwargs):
        length = int(length or 10)
        key = self._key(f"{name}:{length}")
        if self.cutoff is None:
            result = getattr(ta, name)(close, length=length, **kwargs)
            if result is None:
                return None
            return self._remember(key, result, clean_end=bool(pd.notna(close.iloc[-1])))
        if kwargs.get("adjust", False) or kwargs.get("offset", 0):
            raise CheckpointUnavailable("unsupported smoothing options")
        old, result = self._cached(key, close.index)
        positions = close.index[close.index >= self.cutoff]
        if len(positions):
            last = old["values"][-1]
            values = close.loc[positions].to_numpy(dtype=float)
            if last is None or not old.get("clean_end") or not np.isfinite(values).all():
                raise CheckpointUnavailable("nonfinite smoothing input/state")
            seeded = pd.Series(np.r_[float(last), values])
            window = (
                seeded.ewm(alpha=1.0 / length, adjust=False)
                if name == "rma"
                else seeded.ewm(span=length, adjust=False)
            )
            updated = window.mean().iloc[1:]
            result.loc[positions] = updated.to_numpy()
        return self._remember(key, result, clean_end=bool(pd.notna(close.iloc[-1])))

    def _cumulative(self, name, *args, **kwargs):
        key = self._key(name)
        original = getattr(ta, name)
        if self.cutoff is None:
            if name == "ad":
                namespace = original.__globals__.copy()
                namespace["non_zero_range"] = self._non_zero_range
                original = FunctionType(original.__code__, namespace, name, original.__defaults__)
                original.__kwdefaults__ = getattr(ta, name).__kwdefaults__
            result = original(*args, **kwargs)
            return self._remember(key, result)
        close = kwargs.get("close", args[0] if args else None)
        if name == "ad":
            high, low, close, volume = (
                args if args else (kwargs["high"], kwargs["low"], kwargs["close"], kwargs["volume"])
            )
            # Native helper adds epsilon to the entire range if any zero exists.
            increments = (2 * close - (high + low)) * (volume / self._non_zero_range(high, low))
        elif name == "obv":
            volume = kwargs.get("volume", args[1] if len(args) > 1 else None)
            from pandas_ta_classic.utils import signed_series

            increments = signed_series(close, initial=1) * volume
        else:
            volume = kwargs.get("volume", args[1] if len(args) > 1 else None)
            increments = ta.roc(close, length=int(kwargs.get("drift", 1))) * volume
        old, result = self._cached(key, close.index)
        positions = close.index[close.index >= self.cutoff]
        if len(positions):
            seed = old["values"][-1]
            values = increments.loc[positions].to_numpy(dtype=float)
            if seed is None or not np.isfinite(values).all():
                raise CheckpointUnavailable("nonfinite cumulative input/state")
            result.loc[positions] = np.cumsum(np.r_[float(seed), values])[1:]
        return self._remember(key, result)

    def _numpy_ema(self, close, length, seed_end, scalar_seed=False):
        key = self._key(f"numpy_ema:{length}")
        if self.cutoff is None:
            values = np.full(len(close), np.nan)
            if seed_end < len(close):
                values[seed_end] = (
                    close.iloc[0]
                    if scalar_seed
                    else close.iloc[seed_end - length + 1 : seed_end + 1].mean()
                )
                k = 2.0 / (length + 1)
                for i in range(seed_end + 1, len(close)):
                    values[i] = k * close.iloc[i] + (1 - k) * values[i - 1]
            result = pd.Series(values, index=close.index)
        else:
            old, result = self._cached(key, close.index)
            seed = old["values"][-1]
            if seed is None:
                raise CheckpointUnavailable("MACD is not warmed up")
            k = 2.0 / (length + 1)
            for i in close.index[close.index >= self.cutoff]:
                seed = k * close.loc[i] + (1 - k) * seed
                result.loc[i] = seed
        return self._remember(key, result)

    def _macd(self, close, fast=12, slow=26, signal=9, **kwargs):
        if len(close) < max(fast, slow, signal):
            return None
        fast_ema = self._numpy_ema(close, fast, fast - 1)
        slow_ema = self._numpy_ema(close, slow, slow - 1)
        macd = fast_ema - slow_ema
        signal_ema = self._numpy_ema(macd, signal, slow + signal - 2)
        if self.cutoff is None:
            return ta.macd(close, fast=fast, slow=slow, signal=signal, **kwargs)
        suffix = f"_{fast}_{slow}_{signal}"
        return pd.DataFrame(
            {
                "MACD" + suffix: macd,
                "MACDs" + suffix: signal_ema,
                "MACDh" + suffix: macd - signal_ema,
            }
        )

    def _adosc(self, high, low, close, volume, fast=3, slow=10, **kwargs):
        if len(close) < max(fast, slow):
            return None
        ad = self._cumulative("ad", high=high, low=low, close=close, volume=volume)
        fast_ema = self._numpy_ema(ad, fast, 0, scalar_seed=True)
        slow_ema = self._numpy_ema(ad, slow, 0, scalar_seed=True)
        if self.cutoff is None:
            return ta.adosc(high, low, close, volume, fast=fast, slow=slow, **kwargs)
        return fast_ema - slow_ema

    def _supertrend(self, high, low, close, length=7, multiplier=3.0, **kwargs):
        key = self._key("supertrend")
        atr = self._adapt(ta.atr)(high, low, close, length=length)
        if atr is None:
            return None
        upper = (high + low) / 2 + multiplier * atr
        lower = (high + low) / 2 - multiplier * atr
        suffix = f"_{length}_{float(multiplier)}"
        if self.cutoff is None:
            result = ta.supertrend(high, low, close, length, multiplier, **kwargs)
            direction = result["SUPERTd" + suffix]
            trend = result["SUPERT" + suffix]
            last_dir = int(direction.iloc[-1])
            last_upper = float(trend.iloc[-1] if last_dir < 0 else upper.iloc[-1])
            last_lower = float(trend.iloc[-1] if last_dir > 0 else lower.iloc[-1])
        else:
            old, trend = self._cached(key, close.index)
            last_dir, last_upper, last_lower = old["direction"], old["upper"], old["lower"]
            directions = pd.Series(np.nan, index=close.index)
            for i in close.index[close.index >= self.cutoff]:
                direction = (
                    1
                    if close.loc[i] > last_upper
                    else (-1 if close.loc[i] < last_lower else last_dir)
                )
                up, lo = upper.loc[i], lower.loc[i]
                if last_lower <= close.loc[i] <= last_upper:
                    if direction > 0 and lo < last_lower:
                        lo = last_lower
                    if direction < 0 and up > last_upper:
                        up = last_upper
                trend.loc[i] = lo if direction > 0 else up
                directions.loc[i] = direction
                last_dir, last_upper, last_lower = direction, up, lo
            result = pd.DataFrame({"SUPERT" + suffix: trend, "SUPERTd" + suffix: directions})
        self._remember(
            key, result["SUPERT" + suffix], direction=last_dir, upper=last_upper, lower=last_lower
        )
        return result

    def _adapt(self, function):
        name = function.__name__
        if name in ("ema", "rma"):
            return lambda close, length=None, **kw: self._smooth(name, close, length, **kw)
        if name in ("ad", "obv", "pvt"):
            return lambda *a, **kw: self._cumulative(name, *a, **kw)
        if name == "macd":
            return self._macd
        if name == "adosc":
            return self._adosc
        if name == "supertrend":
            return self._supertrend
        if name == "non_zero_range":
            return self._non_zero_range
        if function in self.clones:
            return self.clones[function]
        namespace = function.__globals__.copy()
        clone = FunctionType(
            function.__code__, namespace, name, function.__defaults__, function.__closure__
        )
        clone.__kwdefaults__ = function.__kwdefaults__
        self.clones[function] = clone
        for helper in (
            "ema",
            "rma",
            "ma",
            "atr",
            "rsi",
            "ad",
            "obv",
            "pvt",
            "macd",
            "supertrend",
            "non_zero_range",
            "true_range",
            "hl2",
        ):
            dependency = namespace.get(helper)
            if isinstance(dependency, FunctionType):
                namespace[helper] = self._adapt(dependency)
        return clone

    def _non_zero_range(self, high, low):
        key = self._key("non_zero_range")
        difference = high - low
        zero = bool(difference.eq(0).any())
        if self.cutoff is not None:
            old = self.previous.get("streams", {}).get(key)
            if old is None:
                raise CheckpointUnavailable("missing zero-range regime")
            new_zero = bool(difference.loc[difference.index >= self.cutoff].eq(0).any())
            if new_zero and not old["zero"]:
                raise CheckpointUnavailable("new global zero-range regime")
            zero = zero or old["zero"]
        self.streams[key] = {"zero": zero}
        if zero:
            difference = difference + np.finfo(float).eps
        return difference

    def compute(self, compute_spec, frame, spec):
        self.counter = 0
        self.formula_rows += len(frame)
        self.full_history_rows += len(frame) if self.cutoff is None else 0
        namespace = compute_spec.__globals__.copy()
        adapted = {
            name: self._adapt(value)
            for name, value in vars(ta).items()
            if isinstance(value, FunctionType)
        }
        namespace["ta"] = SimpleNamespace(**{**vars(ta), **adapted})
        function = FunctionType(
            compute_spec.__code__,
            namespace,
            compute_spec.__name__,
            compute_spec.__defaults__,
            compute_spec.__closure__,
        )
        function.__kwdefaults__ = compute_spec.__kwdefaults__
        return function(frame, spec)
