"""Host-only App authentication for the protected main merge controller; no credential output."""

from __future__ import annotations

import argparse
import base64
import importlib.util
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

REPOSITORY = "deusexrenovatio-arch/trading-advisor-3000"
API = "https://api.github.com"
OLD_RULE = 14483960
PERMISSIONS = {"contents": "write", "actions": "read", "metadata": "read"}


class Blocked(ValueError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def request(path, *, token=None, method="GET", body=None):
    if not path.startswith("/") or ".." in path or "://" in path:
        raise Blocked("invalid API path")
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2026-03-10"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.build_opener(NoRedirect()).open(
            urllib.request.Request(API + path, data=data, headers=headers, method=method),
            timeout=30,
        ) as response:
            raw = response.read(1048577)
            if len(raw) > 1048576:
                raise Blocked("oversized API response")
            return json.loads(raw) if raw else None
    except (urllib.error.URLError, OSError):
        raise Blocked("GitHub request rejected or unavailable") from None


def environment(token=None):
    clean = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("GH_", "GITHUB_", "GIT_", "PYTHON"))
    }
    clean.update(GH_HOST="github.com", GH_PROMPT_DISABLED="1", GIT_TERMINAL_PROMPT="0")
    if token:
        clean["GH_TOKEN"] = token
    return clean


def command(argv, *, env=None, cwd=None):
    result = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
        env=env or environment(),
        cwd=cwd,
    )
    if result.returncode:
        raise Blocked("protected command rejected")
    return result.stdout


def policy(document, app_id, rule_id):
    if (
        document.get("id") != rule_id
        or document.get("target") != "branch"
        or document.get("enforcement") != "active"
        or document.get("source") != REPOSITORY
        or document.get("conditions")
        != {"ref_name": {"include": ["refs/heads/main"], "exclude": []}}
    ):
        raise Blocked("unexpected main ruleset identity/scope")
    if rule_id != OLD_RULE:
        if document.get("bypass_actors") != [
            {"actor_type": "Integration", "actor_id": app_id, "bypass_mode": "pull_request"}
        ] or document.get("rules") != [
            {"type": "update", "parameters": {"update_allows_fetch_and_merge": False}}
        ]:
            raise Blocked("App-only main update restriction is missing")
    elif (
        document.get("bypass_actors") != []
        or not any(
            rule.get("type") == "required_status_checks"
            and rule.get("parameters", {}).get("strict_required_status_checks_policy") is True
            and any(
                check.get("context") == "pr-lane"
                for check in rule["parameters"].get("required_status_checks", [])
            )
            for rule in document.get("rules", [])
        )
        or not {"pull_request", "non_fast_forward"}.issubset({r["type"] for r in document["rules"]})
    ):
        raise Blocked("original strict PR policy changed or gained bypass")


def app_jwt(pem, client_id, *, now=None):
    now = int(time.time()) if now is None else now

    def encode(value):
        return base64.urlsafe_b64encode(value).rstrip(b"=")

    message = b".".join(
        encode(json.dumps(value, separators=(",", ":")).encode())
        for value in (
            {"alg": "RS256", "typ": "JWT"},
            {"iat": now - 60, "exp": now + 300, "iss": client_id},
        )
    )
    private = serialization.load_pem_private_key(pem.read_bytes(), password=None)
    signature = private.sign(message, padding.PKCS1v15(), hashes.SHA256())
    return (message + b"." + encode(signature)).decode()


def host_path(value):
    import re

    value = (
        re.sub(
            r"^/(?:run/desktop/mnt/host|host_mnt|mnt)/([a-z])/(.*)",
            lambda m: f"{m[1]}:/{m[2]}",
            value,
        )
        if os.name == "nt"
        else value
    )
    return Path(value).resolve()


def preflight(args, config):
    root, checkout, pem = (
        args.protected_root.resolve(),
        args.checkout.resolve(),
        Path(config["pem_path"]).resolve(),
    )
    authority = Path(__file__).resolve().parents[1]
    roots = [root, checkout, authority]
    ids = command(["docker", "ps", "-aq"]).split()
    if ids:
        for row in command(
            ["docker", "inspect", "--format", "{{json .Mounts}}", *ids]
        ).splitlines():
            roots.extend(host_path(item["Source"]) for item in json.loads(row))
    if not pem.is_file() or any(pem.is_relative_to(path) for path in roots):
        raise Blocked("App key must stay outside all code/data/container mounts")
    repo = request(f"/repos/{REPOSITORY}")
    remote = request(f"/repos/{REPOSITORY}/git/ref/heads/main")["object"]["sha"]
    if any(k.startswith("GIT_") for k in os.environ):
        raise Blocked("ambient Git overrides are not permitted")
    revisions = {}
    for directory in (authority, root):
        revisions[directory] = command(["git", "-C", str(directory), "rev-parse", "HEAD"]).strip()
        if command(
            ["git", "-C", str(directory), "status", "--porcelain", "--untracked-files=all"]
        ).strip():
            raise Blocked("reviewed wrapper/protected checkout is not clean")
    if revisions[root] != remote:
        raise Blocked("protected checkout is not actual remote main")
    spec = importlib.util.spec_from_file_location(
        "staging_candidate_helper", authority / "scripts/build_staging_candidate.py"
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    for directory, revision in revisions.items():
        helper.verify_deployed_files(directory, helper.execution_manifest(directory, revision))
    for identifier in (OLD_RULE, config["update_ruleset_id"]):
        # Existing owner gh login supplies read-only bypass-list visibility; no App admin grant.
        document = json.loads(
            command(
                [
                    "gh",
                    "api",
                    "--hostname",
                    "github.com",
                    f"repos/{REPOSITORY}/rulesets/{identifier}",
                ]
            )
        )
        policy(document, config["app_id"], identifier)
    return root, checkout, pem, repo["id"], remote


def invoke(args):
    config = json.loads(args.config.read_text("utf-8"))
    if (
        set(config) != {"client_id", "app_id", "installation_id", "pem_path", "update_ruleset_id"}
        or not isinstance(config["client_id"], str)
        or not config["client_id"]
        or any(
            type(config[k]) is not int or config[k] < 1
            for k in ("app_id", "installation_id", "update_ruleset_id")
        )
        or config["update_ruleset_id"] == OLD_RULE
    ):
        raise Blocked("operator App configuration is incomplete")
    root, checkout, pem, repo_id, remote = preflight(args, config)
    token, dispatched, outcome = None, False, {"status": "BLOCKED"}
    try:
        jwt = app_jwt(pem, config["client_id"])
        if request("/app", token=jwt)["id"] != config["app_id"]:
            raise Blocked("App key/client identity does not match approved App ID")
        minted = request(
            f"/app/installations/{config['installation_id']}/access_tokens",
            token=jwt,
            method="POST",
            body={
                "repository_ids": [repo_id],
                "permissions": {"contents": "write", "actions": "read"},
            },
        )
        token = minted["token"]
        if (
            not isinstance(token, str)
            or not token
            or minted["permissions"] != PERMISSIONS
            or datetime.fromisoformat(minted["expires_at"].replace("Z", "+00:00"))
            <= datetime.now(UTC)
        ):
            raise Blocked("installation token scope/expiry is invalid")
        accessible = request("/installation/repositories?per_page=100", token=token)
        if accessible["total_count"] != 1 or [r["id"] for r in accessible["repositories"]] != [
            repo_id
        ]:
            raise Blocked("installation token is not limited to the exact repository")
        if (
            request(f"/repos/{REPOSITORY}/git/ref/heads/main", token=token)["object"]["sha"]
            != remote
        ):
            raise Blocked("remote main moved during App admission")
        argv = [
            sys.executable,
            "-I",
            "-B",
            "-c",
            "import runpy,sys;"
            "sys.path[:0]=[sys.argv[1]+'/scripts',sys.argv[1]+'/src'];"
            "sys.argv=sys.argv[2:];runpy.run_path(sys.argv[0],run_name='__main__')",
            str(root),
            str(root / "scripts/merge_staging_candidate.py"),
            "--repository",
            REPOSITORY,
            "--checkout",
            str(checkout),
            "--pull",
            str(args.pull),
            "--run-id",
            str(args.run_id),
        ]
        if args.merge:
            argv.append("--merge")
        dispatched = True
        result = json.loads(command(argv, env=environment(token), cwd=root))
        if result.get("status") != ("MERGED" if args.merge else "ADMITTED"):
            raise Blocked("unexpected protected controller outcome")
        outcome = {"status": result["status"], "scope": "protected App controller"}
    except Exception:
        outcome = {"status": "MERGE_OUTCOME_UNCERTAIN" if dispatched and args.merge else "BLOCKED"}
    finally:
        if token:
            try:
                request("/installation/token", token=token, method="DELETE")
                outcome["token_revocation"] = "SUCCEEDED"
            except Exception:
                outcome["token_revocation"] = "FAILED"
    return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--protected-root", type=Path, required=True)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--pull", type=int, required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--merge", action="store_true")
    args = parser.parse_args()
    try:
        if min(args.pull, args.run_id) < 1:
            raise Blocked("positive pull/run IDs are required")
        outcome = invoke(args)
        print(json.dumps(outcome, sort_keys=True))
        return (
            0
            if outcome["status"] in {"ADMITTED", "MERGED"}
            and outcome.get("token_revocation") == "SUCCEEDED"
            else 2
        )
    except Exception:
        print("staging App merge: BLOCKED (protected preflight/controller/authentication rejected)")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
