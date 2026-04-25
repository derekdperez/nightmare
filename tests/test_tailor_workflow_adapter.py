from __future__ import annotations

from workflow_app.tailor_adapter import (
    is_tailor_workflow_spec,
    normalize_workflow_payload,
    resolve_workflow_runtime_payload,
)


def test_detects_tailor_workflow_shape():
    payload = {
        "workflow-type": "single_run",
        "variables": [],
        "steps": [{"type": "save_variable_value", "name": "seed", "value": "x"}],
    }
    assert is_tailor_workflow_spec(payload) is True


def test_normalize_workflow_payload_generates_plugins_from_tailor_steps():
    payload = {
        "workflow_id": "tailor-demo",
        "retry_limit": 3,
        "steps": [
            {"type": "save_variable_value", "name": "seed", "value": "abc"},
            {"type": "send_input_to_API", "name": "summarize", "input": "hello"},
        ],
    }
    normalized = normalize_workflow_payload(payload)
    plugins = normalized.get("plugins")
    assert isinstance(plugins, list)
    assert [item["plugin_name"] for item in plugins] == ["save_variable_value", "send_input_to_api"]
    assert plugins[0]["retry_limit"] == 3
    assert plugins[0]["max_attempts"] == 4


def test_normalize_workflow_payload_keeps_existing_plugins():
    payload = {"workflow_id": "run-recon", "plugins": [{"plugin_name": "recon_subdomain_enumeration"}]}
    normalized = normalize_workflow_payload(payload)
    assert normalized["plugins"] == payload["plugins"]


def test_resolve_workflow_runtime_payload_applies_templates_and_dependencies():
    payload = {
        "workflow-type": "single_run",
        "variables": [{"name": "wordlist_path", "value": "{{project_root}}/wordlist.txt"}],
        "steps": [
            {"type": "recon_subdomain_enumeration", "name": "seed"},
            {
                "type": "recon_spider_wordlist",
                "name": "crawl",
                "depends_on": ["seed"],
                "crawl_wordlist": "{{wordlist_path}}",
            },
        ],
    }
    normalized = resolve_workflow_runtime_payload(payload, {"project_root": "/tmp/recon"})
    plugins = normalized["plugins"]
    assert plugins[1]["preconditions"]["requires_plugins_all"] == ["recon_subdomain_enumeration"]
    assert plugins[1]["parameters"]["crawl_wordlist"] == "/tmp/recon/wordlist.txt"
