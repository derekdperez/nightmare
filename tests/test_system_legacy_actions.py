from __future__ import annotations

import json
from pathlib import Path

from plugins.system.legacy_actions import LEGACY_ACTIONS


def test_legacy_actions_cover_example_workflow_plugins() -> None:
    manifests_dir = Path("example/workflow_plugins")
    names: set[str] = set()
    for path in manifests_dir.glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        name = str(payload.get("name") or "").strip()
        if name:
            names.add(name)
    assert names, "expected example workflow plugin manifests"
    missing = sorted(name for name in names if name not in LEGACY_ACTIONS)
    assert not missing, f"missing LEGACY_ACTIONS entries: {missing}"

