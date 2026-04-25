from __future__ import annotations

import json

import httpx
import pytest

import http_client


@pytest.fixture(autouse=True)
def _cleanup_shared_clients():
    http_client.close_shared_clients()
    yield
    http_client.close_shared_clients()


def test_get_shared_client_reuses_same_key():
    c1 = http_client.get_shared_client(verify=True, timeout_seconds=10.0, follow_redirects=True, user_agent="ua")
    c2 = http_client.get_shared_client(verify=True, timeout_seconds=10.0, follow_redirects=True, user_agent="ua")
    c3 = http_client.get_shared_client(verify=False, timeout_seconds=10.0, follow_redirects=True, user_agent="ua")
    assert c1 is c2
    assert c1 is not c3


def test_request_json_success_returns_dict():
    transport = httpx.MockTransport(lambda req: httpx.Response(200, request=req, json={"ok": True, "n": 1}))
    client = httpx.Client(transport=transport)
    out = http_client.request_json("GET", "https://example.com/api", client=client)
    assert out == {"ok": True, "n": 1}


def test_request_json_non_dict_payload_returns_empty_dict():
    transport = httpx.MockTransport(lambda req: httpx.Response(200, request=req, content=json.dumps([1, 2, 3]).encode("utf-8")))
    client = httpx.Client(transport=transport)
    out = http_client.request_json("GET", "https://example.com/api", client=client)
    assert out == {}


def test_request_json_http_error_raises_runtime_error():
    transport = httpx.MockTransport(lambda req: httpx.Response(503, request=req, text="temporary failure"))
    client = httpx.Client(transport=transport)
    with pytest.raises(RuntimeError, match="HTTP 503 GET https://example.com/api"):
        http_client.request_json("GET", "https://example.com/api", client=client)


def test_request_json_network_error_raises_runtime_error():
    class FailingClient:
        def request(self, method, url, **kwargs):
            req = httpx.Request(method, url)
            raise httpx.ConnectError("boom", request=req)

    with pytest.raises(RuntimeError, match="Network error GET https://example.com/api"):
        http_client.request_json("GET", "https://example.com/api", client=FailingClient())  # type: ignore[arg-type]


def test_request_capped_respects_read_limit_and_sets_user_agent():
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["user-agent"] = request.headers.get("User-Agent", "")
        return httpx.Response(200, request=request, content=b"abcdef", headers={"X-Test": "1"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    out = http_client.request_capped(
        "GET",
        "https://example.com/path",
        read_limit=3,
        client=client,
        user_agent="nightmare-tests/1.0",
    )
    assert out.status_code == 200
    assert out.body == b"abc"
    assert captured["user-agent"] == "nightmare-tests/1.0"
    assert out.headers.get("x-test") == "1"


def test_request_json_logs_detailed_request_and_response_payloads():
    class CaptureLogger:
        def __init__(self):
            self.rows: list[tuple[str, str, dict[str, object]]] = []

        def info(self, event: str, **kwargs):
            self.rows.append(("info", event, kwargs))

        def error(self, event: str, **kwargs):
            self.rows.append(("error", event, kwargs))

    logger = CaptureLogger()
    transport = httpx.MockTransport(lambda req: httpx.Response(200, request=req, json={"ok": True, "n": 1}))
    client = httpx.Client(transport=transport)
    out = http_client.request_json(
        "POST",
        "https://example.com/api",
        client=client,
        headers={"Authorization": "Bearer secret", "X-Test": "1"},
        json_payload={"hello": "world"},
        logger=logger,
        log_details=True,
        include_payloads=True,
        redact_authorization_header=True,
    )
    assert out == {"ok": True, "n": 1}
    assert len(logger.rows) == 2
    assert logger.rows[0][1] == "http_request_outbound"
    assert logger.rows[1][1] == "http_response_inbound"
    assert logger.rows[0][2]["request_headers"] == {
        "Authorization": "<redacted>",
        "X-Test": "1",
        "User-Agent": http_client.DEFAULT_USER_AGENT,
    }
    assert logger.rows[0][2]["request_json_payload"] == {"hello": "world"}
    assert logger.rows[1][2]["http_status_code"] == 200
    assert logger.rows[1][2]["response_json_payload"] == {"ok": True, "n": 1}


def test_request_json_logs_http_error_event():
    class CaptureLogger:
        def __init__(self):
            self.rows: list[tuple[str, str, dict[str, object]]] = []

        def info(self, event: str, **kwargs):
            self.rows.append(("info", event, kwargs))

        def error(self, event: str, **kwargs):
            self.rows.append(("error", event, kwargs))

    logger = CaptureLogger()
    transport = httpx.MockTransport(lambda req: httpx.Response(503, request=req, text="temporary failure"))
    client = httpx.Client(transport=transport)
    with pytest.raises(RuntimeError, match="HTTP 503 GET https://example.com/api"):
        http_client.request_json(
            "GET",
            "https://example.com/api",
            client=client,
            logger=logger,
            log_details=True,
            include_payloads=True,
        )
    assert len(logger.rows) == 2
    assert logger.rows[0][1] == "http_request_outbound"
    assert logger.rows[1][0] == "error"
    assert logger.rows[1][1] == "http_response_error"


def test_request_json_rejects_non_http_urls():
    with pytest.raises(ValueError, match="absolute http or https URL"):
        http_client.request_json("GET", "file:///etc/passwd")


def test_request_json_redacts_cookie_like_headers_in_logs():
    class CaptureLogger:
        def __init__(self):
            self.rows: list[tuple[str, str, dict[str, object]]] = []

        def info(self, event: str, **kwargs):
            self.rows.append(("info", event, kwargs))

        def error(self, event: str, **kwargs):
            self.rows.append(("error", event, kwargs))

    logger = CaptureLogger()

    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=req, json={"ok": True}, headers={"Set-Cookie": "session=secret"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    http_client.request_json(
        "GET",
        "https://example.com/api",
        client=client,
        headers={"Cookie": "session=secret", "X-Api-Key": "secret-key"},
        logger=logger,
        log_details=True,
    )

    assert logger.rows[0][2]["request_headers"]["Cookie"] == "<redacted>"
    assert logger.rows[0][2]["request_headers"]["X-Api-Key"] == "<redacted>"
    assert logger.rows[1][2]["response_headers"]["set-cookie"] == "<redacted>"
