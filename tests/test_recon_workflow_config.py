from __future__ import annotations

import json
from pathlib import Path


def test_run_recon_workflow_config_exists_and_has_expected_plugins() -> None:
    path = Path("workflows/run-recon.workflow.json")
    assert path.is_file()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["workflow_id"] == "run-recon"
    plugins = payload.get("plugins")
    assert isinstance(plugins, list)
    names = [str(item.get("plugin_name", "")).strip().lower() for item in plugins if isinstance(item, dict)]
    assert names == [
        "recon_subdomain_enumeration",
        "recon_spider_source_tags",
        "recon_spider_script_links",
        "recon_spider_wordlist",
        "recon_spider_ai",
        "recon_extractor_high_value",
    ]


def test_run_recon_extractor_depends_on_spider_completion_flags() -> None:
    payload = json.loads(Path("workflows/run-recon.workflow.json").read_text(encoding="utf-8"))
    plugins = payload.get("plugins") if isinstance(payload.get("plugins"), list) else []
    extractor = next((p for p in plugins if isinstance(p, dict) and p.get("plugin_name") == "recon_extractor_high_value"), None)
    assert isinstance(extractor, dict)
    prereq = extractor.get("preconditions")
    assert isinstance(prereq, dict)
    required_plugins = prereq.get("requires_plugins_all")
    required_artifacts = prereq.get("artifacts_all")
    assert required_plugins == [
        "recon_spider_source_tags",
        "recon_spider_script_links",
        "recon_spider_wordlist",
        "recon_spider_ai",
    ]
    assert required_artifacts == [
        "recon_spider_source_tags_complete_flag",
        "recon_spider_script_links_complete_flag",
        "recon_spider_wordlist_complete_flag",
        "recon_spider_ai_complete_flag",
    ]


def test_run_recon_spider_throttle_defaults_to_half_second() -> None:
    payload = json.loads(Path("workflows/run-recon.workflow.json").read_text(encoding="utf-8"))
    plugins = payload.get("plugins") if isinstance(payload.get("plugins"), list) else []
    spider_plugins = [
        p for p in plugins if isinstance(p, dict) and str(p.get("plugin_name", "")).startswith("recon_spider_")
    ]
    assert spider_plugins
    for plugin in spider_plugins:
        params = plugin.get("parameters")
        assert isinstance(params, dict)
        assert float(params.get("spider_throttle_seconds", 0.0)) == 0.5
