from __future__ import annotations

from shared.events import EventStream
from shared.models import EventRecord

import argparse
import base64
import io
import json
import sys
import threading
import zipfile
from pathlib import Path

import coordinator_app.runtime as runtime


def test_read_json_dict_handles_invalid_content(tmp_path: Path):
    path = tmp_path / "bad.json"
    path.write_text("{invalid", encoding="utf-8")
    assert runtime._read_json_dict(path) == {}


def test_session_uploader_uploads_updated_session(tmp_path: Path):
    class DummyClient:
        def __init__(self):
            self.payloads: list[dict[str, object]] = []

        def save_session(self, payload):
            self.payloads.append(dict(payload))
            return True

    session_path = tmp_path / "session.json"
    session_path.write_text(json.dumps({"saved_at_utc": "", "k": "v"}), encoding="utf-8")

    client = DummyClient()
    uploader = runtime.SessionUploader(
        client,
        root_domain="example.com",
        session_path=session_path,
        interval_seconds=15.0,
        stop_event=threading.Event(),
    )
    uploader.upload_once()
    assert len(client.payloads) == 1
    assert client.payloads[0]["root_domain"] == "example.com"
    assert client.payloads[0]["saved_at_utc"]


def test_zip_and_unzip_helpers_roundtrip(tmp_path: Path):
    source = tmp_path / "src"
    source.mkdir()
    (source / "a.txt").write_text("hello", encoding="utf-8")
    (source / "nested").mkdir()
    (source / "nested" / "b.bin").write_bytes(b"\x01\x02")

    zipped = runtime._zip_directory_bytes(source)
    out = tmp_path / "out"
    runtime._unzip_bytes_to_directory(zipped, out)

    assert (out / "a.txt").read_text(encoding="utf-8") == "hello"
    assert (out / "nested" / "b.bin").read_bytes() == b"\x01\x02"


def test_unzip_helper_blocks_path_traversal(tmp_path: Path):
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("../evil.txt", b"bad")
        zf.writestr("safe.txt", b"ok")

    target = tmp_path / "extract"
    runtime._unzip_bytes_to_directory(payload.getvalue(), target)

    assert (target / "safe.txt").read_bytes() == b"ok"
    assert not (tmp_path / "evil.txt").exists()


def test_run_subprocess_writes_log_and_returns_exit_code(tmp_path: Path):
    log_path = tmp_path / "logs" / "run.log"
    code = runtime.run_subprocess(
        [sys.executable, "-c", "import sys; print('hello'); sys.exit(7)"],
        cwd=tmp_path,
        log_path=log_path,
    )
    text = log_path.read_text(encoding="utf-8")
    assert code == 7
    assert "=== RUN" in text
    assert "hello" in text


def test_summarize_subprocess_failure_uses_error_line_from_log(tmp_path: Path):
    log_path = tmp_path / "logs" / "run.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        "\n".join(
            [
                "=== RUN 2026-01-01 00:00:00 ===",
                "$ python nightmare.py https://example.com/",
                "Traceback (most recent call last):",
                "  File \"/app/nightmare.py\", line 5620, in crawl_domain",
                "NameError: name 'verify_timeout' is not defined",
            ]
        ),
        encoding="utf-8",
    )

    message = runtime.summarize_subprocess_failure("nightmare", 1, log_path)
    assert message == "nightmare exit code 1; NameError: name 'verify_timeout' is not defined"


def test_summarize_subprocess_failure_falls_back_without_log(tmp_path: Path):
    missing_log_path = tmp_path / "logs" / "missing.log"
    message = runtime.summarize_subprocess_failure("nightmare", 1, missing_log_path)
    assert message == "nightmare exit code 1"


def test_coordinator_client_upload_and_download_artifact(monkeypatch):
    client = runtime.CoordinatorClient("https://coord.example.com", " token ")
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, payload=None):
        calls.append((method, path, payload))
        if path.startswith("/api/coord/artifact?"):
            return {
                "found": True,
                "artifact": {
                    "artifact_type": "x",
                    "content_base64": base64.b64encode(b"abc").decode("ascii"),
                },
            }
        return {"ok": True}

    monkeypatch.setattr(client, "_request_json", fake_request)

    assert client.upload_artifact("example.com", "x", b"abc", source_worker="w1")
    downloaded = client.download_artifact("example.com", "x")
    assert downloaded is not None
    assert downloaded["content"] == b"abc"
    assert client.token == "token"
    assert "Authorization" in client._headers()
    assert calls


def test_coordinator_client_worker_command_claim_and_complete(monkeypatch):
    client = runtime.CoordinatorClient("https://coord.example.com", "token")
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, payload=None):
        calls.append((method, path, payload))
        if path == "/api/coord/worker-command/claim":
            return {"ok": True, "command": {"id": 7, "command": "pause", "payload": {}}}
        if path == "/api/coord/worker-command/complete":
            return {"ok": True}
        return {"ok": False}

    monkeypatch.setattr(client, "_request_json", fake_request)

    command = client.claim_worker_command("worker-a", worker_state="running")
    assert command is not None
    assert command["id"] == 7
    assert command["command"] == "pause"
    assert client.complete_worker_command("worker-a", 7, success=True)
    assert calls[0][1] == "/api/coord/worker-command/claim"
    assert calls[1][1] == "/api/coord/worker-command/complete"


def test_coordinator_client_request_json_passes_logging_options(monkeypatch):
    captured: dict[str, object] = {}

    def fake_request_json(method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setenv("COORDINATOR_HTTP_LOG_DETAILS", "true")
    monkeypatch.setenv("COORDINATOR_HTTP_LOG_PAYLOADS", "true")
    monkeypatch.setenv("COORDINATOR_HTTP_LOG_MAX_CHARS", "1234")
    monkeypatch.setenv("COORDINATOR_HTTP_REDACT_AUTH_HEADER", "false")
    monkeypatch.setattr(runtime, "request_json", fake_request_json)

    client = runtime.CoordinatorClient("https://coord.example.com", "token")
    out = client._request_json("POST", "/api/coord/claim", {"worker_id": "w1", "lease_seconds": 30})

    assert out == {"ok": True}
    assert captured["method"] == "POST"
    assert captured["url"] == "https://coord.example.com/api/coord/claim"
    assert captured["log_details"] is True
    assert captured["include_payloads"] is True
    assert captured["max_logged_body_chars"] == 1234
    assert captured["redact_authorization_header"] is False
    assert captured["verify"] is True
    assert captured["headers"] == {"Content-Type": "application/json", "Authorization": "Bearer token"}


def test_load_config_uses_env_for_insecure_tls_and_applies_minimums(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "coordinator.json"
    config_path.write_text(
        json.dumps(
            {
                "output_root": str((tmp_path / "out").resolve()),
                "lease_seconds": 1,
                "nightmare_workers": 0,
                "fozzy_workers": 0,
                "extractor_workers": 0,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(runtime, "load_env_file_into_os", lambda *args, **kwargs: {})
    monkeypatch.setenv("COORDINATOR_BASE_URL", "server.internal")
    monkeypatch.setenv("COORDINATOR_API_TOKEN", "abc")
    monkeypatch.setenv("COORDINATOR_INSECURE_TLS", "true")

    args = argparse.Namespace(config=str(config_path), server_base_url=None, api_token=None, output_root=None)
    cfg = runtime.load_config(args)

    assert cfg.server_base_url == "https://server.internal"
    assert cfg.api_token == "abc"
    assert cfg.insecure_tls is True
    assert cfg.output_root == (tmp_path / "out").resolve()
    assert cfg.lease_seconds >= 30
    assert cfg.nightmare_workers >= 1
    assert cfg.fozzy_workers >= 1
    assert cfg.extractor_workers >= 1


def test_event_stream_reads_recent_rows_in_reverse_order(tmp_path: Path):
    stream = EventStream(tmp_path / "events.ndjson")
    stream.append(EventRecord(event_type="worker.started", aggregate_key="worker:a", payload={"source": "test", "message": "first"}))
    stream.append(EventRecord(event_type="worker.stopped", aggregate_key="worker:a", payload={"source": "test", "message": "second"}))
    rows = stream.read(limit=1, reverse=True)
    assert len(rows) == 1
    assert rows[0]["event_type"] == "worker.stopped"
