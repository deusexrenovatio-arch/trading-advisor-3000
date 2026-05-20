from __future__ import annotations

import numbers

BAR_USAGE_POLICY_ID = "moex_bar_usage_v1"

PRICE_RISK = 1
ENTRY_ALLOWED = 2
EXIT_ALLOWED = 4
RANGE_UPDATE = 8
VOLUME_UPDATE = 16
SESSION_UPDATE = 32
OI_UPDATE = 64
BOUNDARY_BAR = 128
SHORTENED_BAR = 256

BAR_USAGE_PROFILE_FLAGS: dict[str, int] = {
    "regular_trading": (
        PRICE_RISK
        | ENTRY_ALLOWED
        | EXIT_ALLOWED
        | RANGE_UPDATE
        | VOLUME_UPDATE
        | SESSION_UPDATE
        | OI_UPDATE
    ),
    "risk_only": PRICE_RISK | EXIT_ALLOWED,
    "incomplete": PRICE_RISK | EXIT_ALLOWED,
    "boundary_risk": PRICE_RISK | EXIT_ALLOWED | BOUNDARY_BAR,
    "shortened_risk": PRICE_RISK | EXIT_ALLOWED | SHORTENED_BAR,
}
SPECIAL_SESSION_CLASSES = frozenset(
    {
        "short",
        "weekend",
        "weekend_extended",
        "weekend_special",
        "holiday",
        "holiday_special",
        "option_expiration",
    }
)
KNOWN_SESSION_CLASSES = frozenset({"regular", "partial_or_gap"}) | SPECIAL_SESSION_CLASSES


def classify_bar_usage_profile(
    session_class: str,
    *,
    boundary_bar: bool = False,
    shortened_bar: bool = False,
    weekly_missing_expected_session: bool = False,
) -> str:
    if session_class not in KNOWN_SESSION_CLASSES:
        raise ValueError(f"unknown session class for bar usage policy: {session_class}")
    if shortened_bar:
        return "shortened_risk"
    if boundary_bar:
        return "boundary_risk"
    if weekly_missing_expected_session or session_class == "partial_or_gap":
        return "incomplete"
    if session_class in SPECIAL_SESSION_CLASSES:
        return "risk_only"
    return "regular_trading"


def bar_usage_flags_for_profile(profile: str) -> int:
    try:
        return BAR_USAGE_PROFILE_FLAGS[profile]
    except KeyError as exc:
        raise ValueError(f"unknown bar usage profile: {profile}") from exc


def validate_bar_usage_profile_flags(profile: str, flags: int) -> None:
    expected = bar_usage_flags_for_profile(profile)
    if not isinstance(flags, numbers.Integral):
        raise TypeError(
            f"bar usage flags must be an integer value: profile={profile}; actual={flags!r}"
        )
    if flags != expected:
        raise ValueError(
            "bar usage profile/flags mismatch: "
            f"profile={profile}; expected={expected}; actual={flags}"
        )
