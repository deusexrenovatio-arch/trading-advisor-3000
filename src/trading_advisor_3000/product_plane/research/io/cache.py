from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchCacheKey:
    scope: str
    version_keys: tuple[str, ...]
    timeframe: str

    def cache_id(self) -> str:
        payload = "|".join((self.scope, *self.version_keys, self.timeframe))
        return "RCACHE-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12].upper()

