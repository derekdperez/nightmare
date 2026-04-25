from __future__ import annotations

from server_app.store import _workflow_total_attempts


def test_workflow_retry_policy_defaults_to_three_retries(monkeypatch):
    monkeypatch.delenv("WORKFLOW_RETRY_LIMIT", raising=False)
    monkeypatch.delenv("WORKFLOW_MAX_RETRIES", raising=False)
    assert _workflow_total_attempts() == 4


def test_workflow_retry_policy_never_allows_zero_retries(monkeypatch):
    monkeypatch.setenv("WORKFLOW_RETRY_LIMIT", "0")
    assert _workflow_total_attempts() == 2
