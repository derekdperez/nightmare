from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import pytest

import client
import http_queue_cli
import register_targets


def test_client_parse_compose_ps_stdout_variants():
    assert client._parse_compose_ps_stdout("") == "no output"
    assert "svc:ctr:running" in client._parse_compose_ps_stdout(
        json.dumps([{"Service": "svc", "Name": "ctr", "State": "running"}])
    )
    assert "no containers" == client._parse_compose_ps_stdout("[]")


def test_client_http_get_json_builds_auth_and_query(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}

    def fake_request_json(method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = kwargs.get("headers")
        captured["verify"] = kwargs.get("verify")
        return {"ok": True}

    monkeypatch.setattr(client, "request_json", fake_request_json)
    out = client._http_get_json(
        "https://coord.example.com",
        "/api/coord/workers",
        token="secret",
        query={"stale_after_seconds": 120},
        insecure_tls=True,
    )
    assert out == {"ok": True}
    assert captured["method"] == "GET"
    assert str(captured["url"]).startswith("https://coord.example.com/api/coord/workers?")
    assert captured["headers"] == {"Authorization": "Bearer secret"}
    assert captured["verify"] is False


def test_client_parse_args_reads_env_defaults(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        client,
        "_read_env_file",
        lambda _: {
            "COORDINATOR_BASE_URL": "https://coord.example.com",
            "COORDINATOR_API_TOKEN": "token-123",
            "AWS_REGION": "us-west-2",
        },
    )
    args = client.parse_args([])
    assert args.action == "status"
    assert args.server_base_url == "https://coord.example.com"
    assert args.api_token == "token-123"
    assert args.aws_region == "us-west-2"


def test_client_run_aws_json_raises_on_non_json(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(args=a, returncode=0, stdout="not-json", stderr=""),
    )
    with pytest.raises(RuntimeError, match="non-JSON"):
        client._run_aws_json(["aws", "ssm"])


def test_client_main_rejects_noop_skip_flags():
    with pytest.raises(ValueError, match="does nothing"):
        client.main(["status", "--skip-coordinator", "--skip-ssm"])


def test_client_main_progress_rejects_skip_coordinator():
    with pytest.raises(ValueError, match="does nothing"):
        client.main(["progress", "--skip-coordinator"])


def test_client_main_progress_fetches_crawl_progress(monkeypatch: pytest.MonkeyPatch):
    captured_paths: list[str] = []

    def fake_http_get_json(base_url, path, **kwargs):
        captured_paths.append(path)
        if path == "/api/coord/crawl-progress":
            return {
                "generated_at_utc": "2026-04-17T00:00:00Z",
                "counts": {"total_domains": 1, "running_domains": 1, "queued_domains": 0, "failed_domains": 0, "completed_domains": 0},
                "domains": [
                    {
                        "root_domain": "example.com",
                        "phase": "nightmare_running",
                        "discovered_urls_count": 12,
                        "visited_urls_count": 5,
                        "frontier_count": 7,
                        "seconds_since_activity": 3,
                        "active_workers": ["worker-a"],
                    }
                ],
            }
        raise AssertionError(f"unexpected path {path}")

    monkeypatch.setattr(client, "_http_get_json", fake_http_get_json)
    monkeypatch.setattr(client, "_print_crawl_progress", lambda payload: None)

    rc = client.main(["progress", "--server-base-url", "https://coord.example.com", "--api-token", "tok", "--skip-ssm"])
    assert rc == 0
    assert captured_paths == ["/api/coord/crawl-progress"]


def test_register_targets_read_targets_filters_blank_and_comments(tmp_path: Path):
    targets_file = tmp_path / "targets.txt"
    targets_file.write_text(
        "\n".join(
            [
                "# comment",
                "",
                "https://a.example.com",
                "b.example.com",
            ]
        ),
        encoding="utf-8",
    )
    out = register_targets._read_targets(targets_file)
    assert out == ["https://a.example.com", "b.example.com"]


def test_register_targets_post_json_uses_auth_header(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}

    def fake_request_json(method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = kwargs.get("headers")
        captured["verify"] = kwargs.get("verify")
        captured["payload"] = kwargs.get("json_payload")
        return {"ok": True}

    monkeypatch.setattr(register_targets, "request_json", fake_request_json)
    out = register_targets._post_json(
        "https://coord.example.com",
        "secret",
        "/api/coord/register-targets",
        {"targets": ["x"]},
    )
    assert out == {"ok": True}
    assert captured["method"] == "POST"
    assert captured["headers"] == {"Content-Type": "application/json", "Authorization": "Bearer secret"}
    assert captured["verify"] is False


def test_register_targets_parse_args_reads_deploy_env_defaults(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("COORDINATOR_BASE_URL", raising=False)
    monkeypatch.delenv("COORDINATOR_API_TOKEN", raising=False)

    def fake_load_env(path, override=False):
        monkeypatch.setenv("COORDINATOR_BASE_URL", "https://coord.example.com")
        monkeypatch.setenv("COORDINATOR_API_TOKEN", "token-123")
        return {
            "COORDINATOR_BASE_URL": "https://coord.example.com",
            "COORDINATOR_API_TOKEN": "token-123",
        }

    monkeypatch.setattr(register_targets, "load_env_file_into_os", fake_load_env)
    args = register_targets.parse_args([])
    assert args.server_base_url == "https://coord.example.com"
    assert args.api_token == "token-123"


def test_register_targets_main_requires_server_base_url(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        register_targets,
        "parse_args",
        lambda *_: argparse.Namespace(server_base_url="", api_token="x", targets_file="targets.txt"),
    )
    with pytest.raises(ValueError, match="server base URL is required"):
        register_targets.main()


def test_http_queue_cli_main_stats(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    class FakeQueue:
        def __init__(self, *args, **kwargs):
            pass

        def stats(self):
            return {"counts": {"queued": 1}}

        def requeue_expired_leases(self):
            return 2

    monkeypatch.setattr(http_queue_cli, "HttpRequestQueue", FakeQueue)
    monkeypatch.setattr(
        http_queue_cli,
        "parse_args",
        lambda: argparse.Namespace(db_path="q.db", spool_dir="spool", command="stats"),
    )
    rc = http_queue_cli.main()
    out = capsys.readouterr().out
    assert rc == 0
    assert '"queued": 1' in out


def test_http_queue_cli_main_requeue(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    class FakeQueue:
        def __init__(self, *args, **kwargs):
            pass

        def stats(self):
            return {"counts": {}}

        def requeue_expired_leases(self):
            return 5

    monkeypatch.setattr(http_queue_cli, "HttpRequestQueue", FakeQueue)
    monkeypatch.setattr(
        http_queue_cli,
        "parse_args",
        lambda: argparse.Namespace(db_path="q.db", spool_dir="spool", command="requeue-expired"),
    )
    rc = http_queue_cli.main()
    out = capsys.readouterr().out
    assert rc == 0
    assert '"requeued": 5' in out


def test_client_rollout_updates_worker_env_before_compose(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}

    def fake_run_aws_text(args: list[str]) -> str:
        captured["send_args"] = args
        return "cmd-123"

    def fake_run_aws_json(args: list[str]) -> dict[str, object]:
        return {
            "CommandInvocations": [
                {
                    "InstanceId": "i-abc",
                    "Status": "Success",
                    "CommandPlugins": [{"ResponseCode": 0, "Output": "[]"}],
                }
            ]
        }

    monkeypatch.setattr(client, "_run_aws_text", fake_run_aws_text)
    monkeypatch.setattr(client, "_run_aws_json", fake_run_aws_json)
    monkeypatch.setattr(client.shutil, "which", lambda _: "/usr/bin/aws")

    rc = client._run_ssm_rollout(
        region="us-east-1",
        target_key="tag:Name",
        target_values="nightmare-worker*",
        timeout_seconds=30,
        repo_dir="/opt/nightmare",
        branch="main",
        coordinator_base_url="https://coord.example.com",
        api_token="token-abc",
        coordinator_insecure_tls=True,
    )
    assert rc == 0
    send_args = captured.get("send_args")
    assert isinstance(send_args, list)
    assert "--parameters" in send_args
    payload = str(send_args[send_args.index("--parameters") + 1])
    parsed = json.loads(payload)
    script = str(parsed["commands"][0])
    assert "COORDINATOR_BASE_URL=https://coord.example.com" in script
    assert "COORDINATOR_API_TOKEN=token-abc" in script
    assert "COORDINATOR_INSECURE_TLS=true" in script
    assert " > .env;" in script
