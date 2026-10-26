"""Local App-auth protocol fixtures only; no real credentials or network calls."""

import base64
import copy
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import merge_with_staging_app as app  # noqa: E402


def test_jwt_is_rs256_with_client_issuer_and_bounded_lifetime(tmp_path):
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = tmp_path / "fixture.pem"
    pem.write_bytes(
        private.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    encoded = app.app_jwt(pem, "fixture-client", now=1000)
    header, payload, signature = encoded.split(".")

    def decode(value):
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))

    assert json.loads(decode(header))["alg"] == "RS256"
    assert json.loads(decode(payload)) == {"iat": 940, "exp": 1300, "iss": "fixture-client"}
    private.public_key().verify(
        decode(signature), f"{header}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256()
    )


def ruleset(identifier):
    common = {
        "id": identifier,
        "source": app.REPOSITORY,
        "target": "branch",
        "enforcement": "active",
        "conditions": {"ref_name": {"include": ["refs/heads/main"], "exclude": []}},
    }
    if identifier == app.OLD_RULE:
        return {
            **common,
            "bypass_actors": [],
            "rules": [
                {"type": "pull_request"},
                {"type": "non_fast_forward"},
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": True,
                        "required_status_checks": [{"context": "pr-lane"}],
                    },
                },
            ],
        }
    return {
        **common,
        "rules": [{"type": "update", "parameters": {"update_allows_fetch_and_merge": False}}],
        "bypass_actors": [
            {"actor_type": "Integration", "actor_id": 123, "bypass_mode": "pull_request"}
        ],
    }


def test_only_dedicated_app_bypasses_update_and_original_policy_stays_strict():
    for identifier in (app.OLD_RULE, 555):
        original = ruleset(identifier)
        app.policy(original, 123, identifier)
        for field, value in (
            ("enforcement", "disabled"),
            ("bypass_actors", [{"actor_id": 15368}]),
            ("rules", []),
        ):
            changed = {**copy.deepcopy(original), field: value}
            with pytest.raises(app.Blocked):
                app.policy(changed, 123, identifier)


@pytest.fixture
def session(tmp_path, monkeypatch):
    config = {
        "client_id": "fixture-client",
        "app_id": 123,
        "installation_id": 456,
        "pem_path": str(tmp_path / "unused.pem"),
        "update_ruleset_id": 555,
    }
    config_path = tmp_path / "operator.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    args = SimpleNamespace(
        config=config_path,
        protected_root=tmp_path / "protected",
        checkout=tmp_path / "candidate",
        pull=12,
        run_id=34,
        merge=True,
    )
    monkeypatch.setattr(
        app,
        "preflight",
        lambda *a: (args.protected_root, args.checkout, Path(config["pem_path"]), 42, "b" * 40),
    )
    monkeypatch.setattr(app, "app_jwt", lambda *a: "fixture-jwt")
    requests, commands = [], []

    def request(path, **kwargs):
        requests.append((path, kwargs))
        if path == "/app":
            return {"id": 123}
        if path.endswith("/access_tokens"):
            return {
                "token": "fixture-token",
                "permissions": app.PERMISSIONS,
                "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            }
        if path.startswith("/installation/repositories"):
            return {"total_count": 1, "repositories": [{"id": 42}]}
        if path.endswith("/git/ref/heads/main"):
            return {"object": {"sha": "b" * 40}}
        assert path == "/installation/token" and kwargs["method"] == "DELETE"

    monkeypatch.setattr(app, "request", request)
    monkeypatch.setattr(
        app,
        "command",
        lambda argv, **kwargs: commands.append((argv, kwargs)) or json.dumps({"status": "MERGED"}),
    )
    return args, requests, commands, request


def test_token_is_exact_repo_scoped_child_environment_only_and_revoked(session, monkeypatch):
    args, requests, commands, _ = session
    monkeypatch.setenv("GH_DEBUG", "api")
    monkeypatch.setenv("GITHUB_TOKEN", "inherited-fixture")
    before = dict(os.environ)
    assert app.invoke(args) == {
        "status": "MERGED",
        "scope": "protected App controller",
        "token_revocation": "SUCCEEDED",
    }
    mint = next(kwargs for path, kwargs in requests if path.endswith("/access_tokens"))
    assert mint["body"] == {
        "repository_ids": [42],
        "permissions": {"contents": "write", "actions": "read"},
    }
    argv, kwargs = commands[0]
    assert "-I" in argv and "-B" in argv and argv[-1] == "--merge"
    assert str(args.protected_root / "scripts/merge_staging_candidate.py") in argv
    assert "fixture-token" not in " ".join(argv)
    assert kwargs["env"]["GH_TOKEN"] == "fixture-token" and kwargs["env"]["GH_HOST"] == "github.com"
    assert "GH_DEBUG" not in kwargs["env"] and "GITHUB_TOKEN" not in kwargs["env"]
    assert dict(os.environ) == before
    assert requests[-1] == ("/installation/token", {"token": "fixture-token", "method": "DELETE"})


def test_controller_failure_after_merge_dispatch_is_uncertain_and_still_revokes(
    session, monkeypatch
):
    args, requests, _, _ = session
    monkeypatch.setattr(
        app, "command", lambda *a, **k: (_ for _ in ()).throw(app.Blocked("fixture"))
    )
    assert app.invoke(args) == {
        "status": "MERGE_OUTCOME_UNCERTAIN",
        "token_revocation": "SUCCEEDED",
    }
    assert requests[-1][0] == "/installation/token"


def test_revocation_failure_does_not_claim_merge_was_blocked(session, monkeypatch):
    args, _, _, request = session

    def revoke_failure(path, **kwargs):
        if path == "/installation/token":
            raise app.Blocked("fixture")
        return request(path, **kwargs)

    monkeypatch.setattr(app, "request", revoke_failure)
    outcome = app.invoke(args)
    assert outcome["status"] == "MERGED"
    assert outcome["token_revocation"] == "FAILED"


def test_authorization_redirect_is_never_followed():
    assert app.NoRedirect().redirect_request(None, None, None, None, None, None) is None
