"""Observe JSON status; emit semantic changes and terminal outcome only."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path


def observe(
    read_status,
    *,
    fields,
    terminal,
    success,
    interval,
    timeout,
    emit,
    clock=time.monotonic,
    sleep=time.sleep,
):
    started = clock()
    previous = None
    while True:
        try:
            payload = read_status()
            if not isinstance(payload, dict) or not any(k in payload for k in fields):
                raise ValueError("status object has no selected fields")
            state = {k: payload.get(k) for k in fields}
        except FileNotFoundError:
            state = {"status": "source_pending"}
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            emit({"event": "observer_error", "error": type(exc).__name__})
            return 2
        if state != previous:
            emit({"event": "status_changed", "state": state})
            previous = state
        status = str(state.get("status") or "").lower()
        if status in terminal:
            if status == "completed" and not state.get("conclusion"):
                emit({"event": "observer_error", "error": "missing_conclusion"})
                return 2
            conclusion = str(state.get("conclusion") or status).lower()
            return 0 if conclusion in success else 1
        remaining = timeout - (clock() - started)
        if remaining <= 0:
            emit({"event": "observer_timeout"})
            return 3
        sleep(min(interval, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--status-file", type=Path)
    source.add_argument("--command", nargs=argparse.REMAINDER)
    parser.add_argument("--fields", default="status,conclusion,active_step")
    parser.add_argument("--interval-seconds", type=float, default=30)
    parser.add_argument("--max-wait-seconds", type=float, default=3600)
    parser.add_argument(
        "--terminal-statuses",
        default=("success,succeeded,completed,failed,failure,cancelled,canceled,timed_out"),
    )
    parser.add_argument("--success-statuses", default="success,succeeded,completed")
    args = parser.parse_args()
    if args.interval_seconds <= 0 or args.max_wait_seconds <= 0:
        parser.error("interval and maximum wait must be positive")
    if args.command == []:
        parser.error("--command needs an executable and arguments")
    fields = [v.strip() for v in args.fields.split(",") if v.strip()]
    if "status" not in fields:
        parser.error("--fields must include status")

    def read_status():
        if args.status_file:
            return json.loads(args.status_file.read_text(encoding="utf-8"))
        result = subprocess.run(
            args.command,
            check=True,
            capture_output=True,
            text=True,
            timeout=min(30, args.max_wait_seconds),
        )
        return json.loads(result.stdout)

    return observe(
        read_status,
        fields=fields,
        terminal={s.strip().lower() for s in args.terminal_statuses.split(",")},
        success={s.strip().lower() for s in args.success_statuses.split(",")},
        interval=args.interval_seconds,
        timeout=args.max_wait_seconds,
        emit=lambda value: print(json.dumps(value, sort_keys=True), flush=True),
    )


if __name__ == "__main__":
    raise SystemExit(main())
