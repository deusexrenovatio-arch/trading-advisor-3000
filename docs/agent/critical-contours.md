# Critical Contour Policy

## Purpose
- Block false-green closure on expensive contours without adding a new lane or a manual approval workflow.
- Keep low-risk and docs-only tasks lightweight by applying the addendum only when a diff matches a configured critical contour.

## Solution Classes
- `target`: the patch claims the intended contour shape is closed and can show contour-specific evidence.
- `staged`: the patch is an explicit partial step that keeps the target shape intact and names what remains open.
- `fallback`: the patch intentionally uses a non-target route and must say so explicitly with a short waiver reason.

## Forbidden Shortcuts
- synthetic upstream boundary instead of the real previous-step output;
- scaffold-only closure presented as full closure;
- substitute technology or runtime without explicit declaration;
- hidden fallback path;
- smoke/manifests/sample artifacts used instead of required contour evidence.

## Critical Task Note Addendum
When the diff matches `configs/critical_contours.yaml`, add `## Solution Intent` to the active task note and record:
- `Solution Class: target|staged|fallback`
- `Critical Contour: <id>`
- `Forbidden Shortcuts: <comma list>|none`
- `Closure Evidence: <what proves closure>`
- `Shortcut Waiver: none|<one-line reason>`

Keep the design checkpoint inline in the same section:
- chosen path;
- why it is not a shortcut;
- what future shape is preserved.

## Pilot Contours
- `data-integration-closure`
- `runtime-publication-closure`

## Validation
- `python scripts/validate_solution_intent.py --from-git --git-ref HEAD`
- `python scripts/validate_critical_contour_closure.py --from-git --git-ref HEAD`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
