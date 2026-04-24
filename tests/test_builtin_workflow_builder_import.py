from pathlib import Path

from plugins.registry import list_registered_plugins
from workflow_app.store import load_builtin_workflow_definition, workflow_payload_from_scheduler_file


def test_run_recon_workflow_imports_to_builder_definition():
    payload = load_builtin_workflow_definition("run-recon")

    assert payload["workflow_key"] == "run-recon"
    assert payload["name"] == "Recon Workflow"
    assert payload["status"] == "published"
    assert payload["input_schema"]["required"] == ["root_domain"]
    assert len(payload["steps"]) == 7

    registered = set(list_registered_plugins())
    assert {step["plugin_key"] for step in payload["steps"]}.issubset(registered)

    takeover = next(step for step in payload["steps"] if step["step_key"] == "recon_subdomain_takeover")
    assert takeover["config_json"]["probe_timeout_seconds"] == 10
    assert "recon_subdomains_json" in takeover["preconditions_json"]["artifacts_all"]
    assert "recon_subdomain_enumeration_complete_flag" in takeover["preconditions_json"]["artifacts_all"]

    extractor = payload["steps"][-1]
    assert extractor["plugin_key"] == "recon_extractor_high_value"
    assert extractor["config_json"]["wordlist"].endswith("high_value_extractor_list.txt")
    assert "recon_spider_ai_complete_flag" in extractor["preconditions_json"]["artifacts_all"]
