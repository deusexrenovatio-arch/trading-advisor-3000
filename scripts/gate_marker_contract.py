from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from gate_common import CommandSpec, command_text


ALLOWED_GATE_SNAPSHOT_MODES = {"changed-files", "contract-only"}


class GateMarkerError(RuntimeError):
    """Raised when gate marker inputs violate the enforced marker contract."""


@dataclass(frozen=True)
class GateMarkerResolution:
    snapshot_mode: str
    profile: str
    snapshot_explicit: bool
    profile_explicit: bool
    deprecation_messages: tuple[str, ...]


def infer_snapshot_mode(
    *,
    from_git: bool,
    base_ref: str | None,
    head_ref: str | None,
    from_stdin: bool,
    explicit_changed_files: Sequence[str],
    default_when_no_selector: str,
) -> str:
    if from_stdin or explicit_changed_files:
        return "changed-files"
    if from_git or (base_ref and head_ref):
        return "changed-files"
    return default_when_no_selector


def includes_policy_critical_command(
    commands: Sequence[str | CommandSpec],
    *,
    critical_script_paths: Sequence[str],
) -> bool:
    markers = tuple(item.replace("\\", "/").strip().lower() for item in critical_script_paths if item.strip())
    for item in commands:
        rendered = command_text(item).replace("\\", "/").strip().lower()
        if any(marker in rendered for marker in markers):
            return True
    return False


def _selector_hint(
    *,
    from_git: bool,
    base_ref: str | None,
    head_ref: str | None,
    from_stdin: bool,
    explicit_changed_files: Sequence[str],
) -> str:
    parts: list[str] = []
    if from_stdin:
        parts.append("--stdin")
    if explicit_changed_files:
        parts.append("--changed-files")
    if from_git:
        parts.append("--from-git")
    if base_ref and head_ref:
        parts.append("--base-ref/--head-ref")
    if not parts:
        parts.append("no snapshot selectors")
    return ", ".join(parts)


def resolve_gate_markers(
    *,
    gate_name: str,
    from_git: bool,
    base_ref: str | None,
    head_ref: str | None,
    from_stdin: bool,
    explicit_changed_files: Sequence[str],
    snapshot_mode_raw: str | None,
    profile_raw: str | None,
    policy_critical: bool,
    enforce_explicit_markers: bool,
    default_when_no_selector: str,
) -> GateMarkerResolution:
    expected_snapshot = infer_snapshot_mode(
        from_git=from_git,
        base_ref=base_ref,
        head_ref=head_ref,
        from_stdin=from_stdin,
        explicit_changed_files=explicit_changed_files,
        default_when_no_selector=default_when_no_selector,
    )
    selector_hint = _selector_hint(
        from_git=from_git,
        base_ref=base_ref,
        head_ref=head_ref,
        from_stdin=from_stdin,
        explicit_changed_files=explicit_changed_files,
    )

    snapshot_explicit = snapshot_mode_raw is not None
    profile_explicit = profile_raw is not None

    if snapshot_explicit:
        snapshot_mode = str(snapshot_mode_raw or "").strip().lower()
        if not snapshot_mode:
            raise GateMarkerError(
                f"{gate_name}: empty snapshot marker is not allowed; "
                "use `--snapshot-mode changed-files` or `--snapshot-mode contract-only`."
            )
        if snapshot_mode not in ALLOWED_GATE_SNAPSHOT_MODES:
            allowed = ", ".join(sorted(ALLOWED_GATE_SNAPSHOT_MODES))
            raise GateMarkerError(
                f"{gate_name}: unknown snapshot marker `{snapshot_mode}`; expected one of: {allowed}."
            )
    else:
        snapshot_mode = expected_snapshot

    if snapshot_mode != expected_snapshot:
        raise GateMarkerError(
            f"{gate_name}: conflicting snapshot marker `{snapshot_mode}` for selector set ({selector_hint}); "
            f"expected `{expected_snapshot}`."
        )

    if profile_explicit:
        profile = str(profile_raw or "").strip()
        if not profile:
            raise GateMarkerError(
                f"{gate_name}: empty profile marker is not allowed; use `--profile none` when no profile is selected."
            )
    else:
        profile = "none"

    require_explicit = bool(policy_critical or enforce_explicit_markers)
    if require_explicit and not snapshot_explicit:
        raise GateMarkerError(
            f"{gate_name}: explicit snapshot marker is required on this policy-critical path; "
            f"rerun with `--snapshot-mode {expected_snapshot}`."
        )
    if require_explicit and not profile_explicit:
        raise GateMarkerError(
            f"{gate_name}: explicit profile marker is required on this policy-critical path; "
            "rerun with `--profile none` (or the required profile id)."
        )

    deprecation_messages: list[str] = []
    if not require_explicit and not snapshot_explicit:
        deprecation_messages.append(
            f"{gate_name}: DEPRECATION implicit snapshot marker fallback is active; "
            f"prefer `--snapshot-mode {expected_snapshot}`."
        )
    if not require_explicit and not profile_explicit:
        deprecation_messages.append(
            f"{gate_name}: DEPRECATION implicit profile marker fallback is active; "
            "prefer `--profile none` (or the required profile id)."
        )

    normalized_profile = "none" if profile.lower() == "none" else profile
    return GateMarkerResolution(
        snapshot_mode=snapshot_mode,
        profile=normalized_profile,
        snapshot_explicit=snapshot_explicit,
        profile_explicit=profile_explicit,
        deprecation_messages=tuple(deprecation_messages),
    )
