from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


METRICS_FILE = Path(os.getenv("METRICS_FILE", "/runtime/observability.prometheus.metrics.txt"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9464"))


def _bridge_metric(file_exists: bool) -> str:
    return (
        "# HELP ta3000_observability_file_bridge_up Prometheus file bridge readiness.\n"
        "# TYPE ta3000_observability_file_bridge_up gauge\n"
        f"ta3000_observability_file_bridge_up {1 if file_exists else 0}\n"
    )


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path not in {"/metrics", "/metrics/"}:
            self.send_response(404)
            self.end_headers()
            return

        metrics_payload = ""
        file_exists = METRICS_FILE.exists()
        if file_exists:
            metrics_payload = METRICS_FILE.read_text(encoding="utf-8")
            if metrics_payload and not metrics_payload.endswith("\n"):
                metrics_payload += "\n"
        payload = (_bridge_metric(file_exists) + metrics_payload).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
