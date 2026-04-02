from __future__ import annotations

from enum import StrEnum


class Mode(StrEnum):
    SHADOW = "shadow"
    PAPER = "paper"
    LIVE = "live"


class TradeSide(StrEnum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class PublicationState(StrEnum):
    CANDIDATE = "candidate"
    PUBLISHED = "published"
    CLOSED = "closed"
    CANCELED = "canceled"


class PublicationType(StrEnum):
    CREATE = "create"
    EDIT = "edit"
    CLOSE = "close"
    CANCEL = "cancel"


class Timeframe(StrEnum):
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"
