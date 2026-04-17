from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import quote

from server import DashboardHandler


def _make_handler(*, coordinator_token: str, headers: dict[str, str]) -> DashboardHandler:
    handler = DashboardHandler.__new__(DashboardHandler)
    handler.server = SimpleNamespace(coordinator_token=coordinator_token)
    handler.headers = headers
    return handler


def test_is_coordinator_authorized_allows_when_token_is_not_configured():
    handler = _make_handler(coordinator_token="", headers={})
    assert handler._is_coordinator_authorized()


def test_is_coordinator_authorized_accepts_bearer_header():
    handler = _make_handler(
        coordinator_token="secret-token",
        headers={"Authorization": "Bearer secret-token"},
    )
    assert handler._is_coordinator_authorized()


def test_is_coordinator_authorized_accepts_x_coordinator_header():
    handler = _make_handler(
        coordinator_token="secret-token",
        headers={"X-Coordinator-Token": "secret-token"},
    )
    assert handler._is_coordinator_authorized()


def test_is_coordinator_authorized_accepts_cookie_token():
    handler = _make_handler(
        coordinator_token="secret-token",
        headers={"Cookie": "foo=bar; nightmare_coord_token=secret-token; baz=qux"},
    )
    assert handler._is_coordinator_authorized()


def test_is_coordinator_authorized_accepts_url_encoded_cookie_token():
    token = "secret token + value"
    handler = _make_handler(
        coordinator_token=token,
        headers={"Cookie": f"nightmare_coord_token={quote(token, safe='')}"},
    )
    assert handler._is_coordinator_authorized()


def test_is_coordinator_authorized_rejects_incorrect_cookie_token():
    handler = _make_handler(
        coordinator_token="secret-token",
        headers={"Cookie": "nightmare_coord_token=wrong-token"},
    )
    assert not handler._is_coordinator_authorized()
