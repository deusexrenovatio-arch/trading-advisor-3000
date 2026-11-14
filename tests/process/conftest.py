"""Keep the hosting PR's review inputs out of isolated process fixtures."""

import pytest


@pytest.fixture(autouse=True)
def isolated_review_inputs(monkeypatch):
    # Each test supplies its own event/export when that input is under test.
    for name in ("GITHUB_EVENT_PATH", "AI_SHELL_ARCHITECTURE_REVIEW_SOURCE"):
        monkeypatch.delenv(name, raising=False)
