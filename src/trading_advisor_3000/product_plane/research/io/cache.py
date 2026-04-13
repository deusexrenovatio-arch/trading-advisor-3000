from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ResearchCacheKey:
    scope: str
    version_keys: tuple[str, ...]
    timeframe: str

    def cache_id(self) -> str:
        payload = "|".join((self.scope, *self.version_keys, self.timeframe))
        return "RCACHE-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12].upper()


@dataclass
class ResearchFrameCache:
    _entries: dict[str, Any] = field(default_factory=dict)

    def get(self, key: ResearchCacheKey) -> Any | None:
        return self._entries.get(key.cache_id())

    def set(self, key: ResearchCacheKey, value: Any) -> None:
        self._entries[key.cache_id()] = value
