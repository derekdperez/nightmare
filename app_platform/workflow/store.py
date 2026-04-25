#!/usr/bin/env python3
"""DB-backed workflow/plugin definition store.

This module intentionally wraps the existing CoordinatorStore connection pool instead of
introducing a second database configuration path.
"""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any

from app_platform.workflow.tailor_adapter import normalize_workflow_payload
from app_platform.workflow.bindings import resolve_bindings
from shared.schemas import WorkflowDefinitionSchema

_KEY_RE = re.compile(r"[^a-z0-9_\-]+")
DEFAULT_WORKFLOW_RETRY_LIMIT = 3
BOOTSTRAP_READY_STAGES = {"recon_subdomain_enumeration", "recon-subdomain-enumeration"}


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return max(1, int(default))
    return max(1, parsed)


def workflow_retry_limit(value: Any = None) -> int:
    if value is not None and str(value).strip() != "":
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return int(DEFAULT_WORKFLOW_RETRY_LIMIT)
        return max(0, parsed)
    return DEFAULT_WORKFLOW_RETRY_LIMIT


def workflow_total_attempts(value: Any = None) -> int:
    return workflow_retry_limit(value) + 1


def _normal_stage_name(stage: str) -> str:
    return str(stage or "").strip().lower().replace("-", "_")


def _is_bootstrap_ready_stage(stage: str) -> bool:
    return _normal_stage_name(stage) in BOOTSTRAP_READY_STAGES


def _preconditions_require_wait(preconditions: Any, *, plugin_key: str = "") -> bool:
    if _is_bootstrap_ready_stage(plugin_key):
        return False
    data = _json(preconditions, {})
    if not isinstance(data, dict) or not data:
        return False
    # Empty condition containers such as {"all": []} should not block.
    for value in data.values():
        if isinstance(value, list) and value:
            return True
        if isinstance(value, dict) and value:
            return True
        if isinstance(value, str) and value.strip():
            return True
        if isinstance(value, bool) and value:
            return True
    return False


def slugify_key(value: str, *, fallback: str = "workflow") -> str:
    raw = str(value or "").strip().lower().replace(" ", "-")
    slug = _KEY_RE.sub("-", raw).strip("-_")
    return slug or fallback


def _json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value) if value.strip() else default
        except Exception:
            return default
    return value


def ensure_workflow_schema(store: Any) -> None:
    ddl = """
CREATE TABLE IF NOT EXISTS plugin_definitions (
  id UUID PRIMARY KEY,
  plugin_key TEXT NOT NULL UNIQUE,
  python_module TEXT NOT NULL DEFAULT '',
  python_class TEXT NOT NULL DEFAULT '',
  category TEXT NOT NULL DEFAULT 'general',
  display_name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  contract_version TEXT NOT NULL DEFAULT '1.0.0',
  config_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  input_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  output_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  ui_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  examples_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  tags JSONB NOT NULL DEFAULT '[]'::jsonb,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  is_builtin BOOLEAN NOT NULL DEFAULT FALSE,
  source_path TEXT NOT NULL DEFAULT '',
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workflow_definitions (
  id UUID PRIMARY KEY,
  workflow_key TEXT NOT NULL UNIQUE,
  version INTEGER NOT NULL DEFAULT 1,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'draft',
  trigger_mode TEXT NOT NULL DEFAULT 'manual',
  input_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  ui_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  tags JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_by TEXT NOT NULL DEFAULT '',
  updated_by TEXT NOT NULL DEFAULT '',
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_definitions_key_version
  ON workflow_definitions(workflow_key, version);

CREATE TABLE IF NOT EXISTS workflow_steps (
  id UUID PRIMARY KEY,
  workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id) ON DELETE CASCADE,
  step_key TEXT NOT NULL,
  display_name TEXT NOT NULL,
  plugin_key TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  is_archived BOOLEAN NOT NULL DEFAULT FALSE,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  continue_on_error BOOLEAN NOT NULL DEFAULT FALSE,
  retry_failed BOOLEAN NOT NULL DEFAULT FALSE,
  max_attempts INTEGER NOT NULL DEFAULT 1,
  timeout_seconds INTEGER NOT NULL DEFAULT 0,
  input_bindings JSONB NOT NULL DEFAULT '{}'::jsonb,
  config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  preconditions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  outputs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(workflow_definition_id, step_key),
  UNIQUE(workflow_definition_id, ordinal)
);

CREATE TABLE IF NOT EXISTS workflow_schedules (
  id UUID PRIMARY KEY,
  workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  schedule_type TEXT NOT NULL DEFAULT 'manual',
  cron_expr TEXT NOT NULL DEFAULT '',
  interval_seconds INTEGER NOT NULL DEFAULT 0,
  run_at_utc TIMESTAMPTZ,
  default_input JSONB NOT NULL DEFAULT '{}'::jsonb,
  target_selector JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_enqueued_at_utc TIMESTAMPTZ,
  next_run_at_utc TIMESTAMPTZ,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workflow_runs (
  id UUID PRIMARY KEY,
  workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
  workflow_key TEXT NOT NULL,
  workflow_version INTEGER NOT NULL DEFAULT 1,
  root_domain TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'queued',
  trigger_source TEXT NOT NULL DEFAULT 'manual',
  trigger_reference TEXT NOT NULL DEFAULT '',
  input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at_utc TIMESTAMPTZ,
  completed_at_utc TIMESTAMPTZ,
  created_by TEXT NOT NULL DEFAULT '',
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_status_created ON workflow_runs(status, created_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_domain ON workflow_runs(root_domain, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS workflow_step_runs (
  id UUID PRIMARY KEY,
  workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
  step_definition_id UUID NOT NULL REFERENCES workflow_steps(id),
  step_key TEXT NOT NULL,
  plugin_key TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  blocked_reason TEXT NOT NULL DEFAULT '',
  worker_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 1,
  input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  resolved_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT NOT NULL DEFAULT '',
  lease_expires_at TIMESTAMPTZ,
  started_at_utc TIMESTAMPTZ,
  completed_at_utc TIMESTAMPTZ,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(workflow_run_id, step_key)
);
CREATE INDEX IF NOT EXISTS idx_workflow_step_runs_claim ON workflow_step_runs(status, lease_expires_at, ordinal, created_at_utc);

CREATE TABLE IF NOT EXISTS workflow_definition_audit (
  id BIGSERIAL PRIMARY KEY,
  entity_type TEXT NOT NULL,
  entity_key TEXT NOT NULL,
  entity_version INTEGER NOT NULL DEFAULT 1,
  action TEXT NOT NULL,
  actor TEXT NOT NULL DEFAULT '',
  before_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  after_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""
    with store._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            # Idempotent migrations for databases created by older versions of
            # the workflow builder. CREATE TABLE IF NOT EXISTS does not add
            # columns to existing tables, which caused saves to fail with the
            # generic "workflow API request failed" error after the builder grew
            # retry/config/status fields.
            cur.execute("""
ALTER TABLE plugin_definitions
  ADD COLUMN IF NOT EXISTS python_module TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS python_class TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'general',
  ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS contract_version TEXT NOT NULL DEFAULT '1.0.0',
  ADD COLUMN IF NOT EXISTS config_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS input_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS output_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS ui_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS examples_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS tags JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS is_builtin BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS source_path TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE workflow_definitions
  ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'draft',
  ADD COLUMN IF NOT EXISTS trigger_mode TEXT NOT NULL DEFAULT 'manual',
  ADD COLUMN IF NOT EXISTS input_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS ui_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS tags JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS created_by TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS updated_by TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE workflow_steps
  ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS is_archived BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS continue_on_error BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS retry_failed BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS timeout_seconds INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS input_bindings JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS preconditions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS outputs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE workflow_step_runs
  ADD COLUMN IF NOT EXISTS blocked_reason TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS worker_id TEXT,
  ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS resolved_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS error TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS started_at_utc TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS completed_at_utc TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
UPDATE workflow_steps SET max_attempts = 1 WHERE COALESCE(max_attempts, 0) < 1;
UPDATE workflow_step_runs SET max_attempts = 1 WHERE COALESCE(max_attempts, 0) < 1;
""")
        conn.commit()




_BUILTIN_WORKFLOW_DIR = Path(__file__).resolve().parents[2] / "workflows"



def _merge_requirement_lists(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    """Merge workflow-file preconditions and input artifact requirements.

    The legacy workflow file separated scheduling preconditions from declared
    inputs. The builder/runtime state machine needs both represented as
    preconditions so a stage is not released before its input artifacts exist.
    """
    merged: dict[str, Any] = dict(base or {})
    for key, value in (extra or {}).items():
        if key.endswith("_all") or key.endswith("_any") or key in {"artifacts", "required_artifacts", "requires_plugins_all", "requires_plugins_any"}:
            existing = merged.get(key)
            if isinstance(value, list):
                seen = []
                for item in (existing if isinstance(existing, list) else []):
                    if item not in seen:
                        seen.append(item)
                for item in value:
                    if item not in seen:
                        seen.append(item)
                merged[key] = seen
            elif value is not None and key not in merged:
                merged[key] = value
        elif key not in merged:
            merged[key] = value
    return merged


def workflow_payload_from_scheduler_file(path: str | Path) -> dict[str, Any]:
    """Convert a legacy coordinator scheduler workflow JSON file into the DB builder format."""
    wf_path = Path(path)
    raw = normalize_workflow_payload(json.loads(wf_path.read_text(encoding="utf-8-sig")))
    plugins = raw.get("plugins") if isinstance(raw.get("plugins"), list) else []
    workflow_retry = workflow_retry_limit(raw.get("retry_limit"))
    workflow_key = slugify_key(str(raw.get("workflow_id") or wf_path.stem.replace(".workflow", "")), fallback="workflow")
    steps: list[dict[str, Any]] = []
    for idx, item in enumerate(plugins, start=1):
        if not isinstance(item, dict):
            continue
        plugin_key = slugify_key(str(item.get("plugin_name") or item.get("handler") or item.get("name") or ""), fallback="")
        if not plugin_key:
            continue
        step_key = slugify_key(str(item.get("name") or plugin_key), fallback=f"step-{idx}")
        inputs = _json(item.get("inputs"), {})
        preconditions = _merge_requirement_lists(_json(item.get("preconditions", item.get("prerequisites")), {}), inputs if isinstance(inputs, dict) else {})
        parameters = _json(item.get("parameters"), {})
        config_json = parameters if isinstance(parameters, dict) else {}
        # Preserve legacy execution metadata next to the editable runtime parameters.
        for meta_key in ("handler", "resume_mode", "config_schema"):
            if meta_key in item and item.get(meta_key) not in (None, ""):
                config_json.setdefault(meta_key, item.get(meta_key))
        step_retry_limit = workflow_retry_limit(item.get("retry_limit"))
        steps.append(
            {
                "step_key": step_key,
                "display_name": str(item.get("display_name") or step_key.replace("-", " ").replace("_", " ").title()),
                "description": str(item.get("description") or ""),
                "plugin_key": plugin_key,
                "ordinal": int(item.get("ordinal") or idx),
                "enabled": bool(item.get("enabled", True)),
                "continue_on_error": bool(item.get("continue_on_error", False)),
                "retry_failed": bool(item.get("retry_failed", False)),
                "retry_limit": step_retry_limit,
                "max_attempts": max(1, int(item.get("max_attempts") or (step_retry_limit + 1))),
                "timeout_seconds": int(item.get("timeout_seconds") or 0),
                "input_bindings": inputs if isinstance(inputs, dict) else {},
                "config_json": config_json,
                "preconditions_json": preconditions,
                "outputs_json": _json(item.get("outputs"), {}),
            }
        )
    return {
        "workflow_key": workflow_key,
        "version": 1,
        "name": "Recon Workflow" if workflow_key in {"run-recon", "recon-workflow"} else workflow_key.replace("-", " ").replace("_", " ").title(),
        "description": str(raw.get("description") or "Imported workflow definition."),
        "status": "published",
        "trigger_mode": "manual",
        "input_schema": {
            "type": "object",
            "required": ["root_domain"],
            "properties": {
                "root_domain": {
                    "type": "string",
                    "title": "Root domain",
                    "description": "The root domain to run this recon workflow against.",
                }
            },
            "additionalProperties": True,
        },
        "ui_schema": {
            "source_file": str(wf_path.name),
            "legacy_schema_version": str(raw.get("schema_version") or ""),
            "workflow_type": str(raw.get("workflow_type") or ""),
        },
        "tags": ["builtin", "recon", "imported-from-workflow-file"],
        "steps": sorted(steps, key=lambda s: int(s.get("ordinal") or 0)),
    }


def load_builtin_workflow_definition(workflow_key: str = "run-recon") -> dict[str, Any]:
    key = slugify_key(workflow_key, fallback="run-recon")
    candidates = [
        _BUILTIN_WORKFLOW_DIR / f"{key}.workflow.json",
        _BUILTIN_WORKFLOW_DIR / f"{key}.json",
    ]
    if key == "recon-workflow":
        candidates.append(_BUILTIN_WORKFLOW_DIR / "run-recon.workflow.json")
    for candidate in candidates:
        if candidate.exists():
            return workflow_payload_from_scheduler_file(candidate)
    raise FileNotFoundError(f"built-in workflow file not found for {workflow_key!r}")


def seed_builtin_workflows(store: Any, *, overwrite: bool = False, actor: str = "system") -> list[dict[str, Any]]:
    """Ensure shipped workflow JSON files are available in the workflow builder.

    By default this is non-destructive: once a workflow exists, user edits are
    preserved. Passing overwrite=True intentionally re-imports from disk.
    """
    ensure_workflow_schema(store)
    seeded: list[dict[str, Any]] = []
    for wf_path in sorted(_BUILTIN_WORKFLOW_DIR.glob("*.workflow.json")):
        payload = workflow_payload_from_scheduler_file(wf_path)
        existing = get_workflow_definition(store, str(payload.get("workflow_key") or ""))
        if existing and not overwrite:
            seeded.append({"workflow_key": existing["workflow_key"], "status": "exists", "id": existing["id"]})
            continue
        item = save_workflow_definition(store, payload, actor=actor)
        seeded.append({"workflow_key": item["workflow_key"], "status": "imported", "id": item["id"]})
    return seeded


def seed_builtin_plugins(store: Any) -> None:
    ensure_workflow_schema(store)
    from plugins.registry import list_plugin_contracts

    contracts = list_plugin_contracts()
    with store._connect() as conn:
        with conn.cursor() as cur:
            for item in contracts:
                plugin_key = slugify_key(str(item.get("plugin_key") or item.get("name") or ""), fallback="")
                if not plugin_key:
                    continue
                cur.execute(
                    """
INSERT INTO plugin_definitions (
  id, plugin_key, python_module, python_class, category, display_name, description,
  contract_version, config_schema, input_schema, output_schema, ui_schema,
  examples_json, tags, enabled, is_builtin, source_path
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s,%s)
ON CONFLICT (plugin_key) DO UPDATE SET
  python_module = COALESCE(NULLIF(EXCLUDED.python_module, ''), plugin_definitions.python_module),
  python_class = COALESCE(NULLIF(EXCLUDED.python_class, ''), plugin_definitions.python_class),
  category = EXCLUDED.category,
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  contract_version = EXCLUDED.contract_version,
  config_schema = EXCLUDED.config_schema,
  input_schema = EXCLUDED.input_schema,
  output_schema = EXCLUDED.output_schema,
  ui_schema = EXCLUDED.ui_schema,
  examples_json = EXCLUDED.examples_json,
  tags = EXCLUDED.tags,
  enabled = TRUE,
  is_builtin = TRUE,
  source_path = EXCLUDED.source_path,
  updated_at_utc = NOW();
""",
                    (
                        str(uuid.uuid4()),
                        plugin_key,
                        str(item.get("python_module") or ""),
                        str(item.get("python_class") or ""),
                        str(item.get("category") or "general"),
                        str(item.get("display_name") or plugin_key),
                        str(item.get("description") or ""),
                        str(item.get("contract_version") or "1.0.0"),
                        json.dumps(_json(item.get("config_schema"), {})),
                        json.dumps(_json(item.get("input_schema"), {})),
                        json.dumps(_json(item.get("output_schema"), {})),
                        json.dumps(_json(item.get("ui_schema"), {})),
                        json.dumps(_json(item.get("examples"), [])),
                        json.dumps(_json(item.get("tags"), [])),
                        bool(item.get("enabled", True)),
                        True,
                        str(item.get("source_path") or ""),
                    ),
                )
        conn.commit()


def list_plugin_definitions(store: Any) -> list[dict[str, Any]]:
    ensure_workflow_schema(store)
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
SELECT id::text, plugin_key, python_module, python_class, category, display_name, description,
       contract_version, config_schema, input_schema, output_schema, ui_schema,
       examples_json, tags, enabled, is_builtin, source_path, updated_at_utc
FROM plugin_definitions
ORDER BY category ASC, plugin_key ASC;
"""
        )
        return [
            {
                "id": r[0], "plugin_key": r[1], "python_module": r[2], "python_class": r[3],
                "category": r[4], "display_name": r[5], "description": r[6],
                "contract_version": r[7], "config_schema": r[8] or {}, "input_schema": r[9] or {},
                "output_schema": r[10] or {}, "ui_schema": r[11] or {}, "examples_json": r[12] or [],
                "tags": r[13] or [], "enabled": bool(r[14]), "is_builtin": bool(r[15]),
                "source_path": r[16], "updated_at_utc": r[17].isoformat() if r[17] else None,
            }
            for r in cur.fetchall()
        ]


def upsert_plugin_definition(store: Any, payload: dict[str, Any], *, actor: str = "") -> dict[str, Any]:
    ensure_workflow_schema(store)
    plugin_key = slugify_key(str(payload.get("plugin_key") or payload.get("name") or payload.get("display_name") or ""), fallback="plugin")
    display_name = str(payload.get("display_name") or plugin_key).strip()
    row_id = str(payload.get("id") or uuid.uuid4())
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_jsonb(p.*) FROM plugin_definitions p WHERE plugin_key=%s", (plugin_key,))
        before = cur.fetchone()
        cur.execute(
            """
INSERT INTO plugin_definitions (
 id, plugin_key, python_module, python_class, category, display_name, description,
 contract_version, config_schema, input_schema, output_schema, ui_schema, examples_json,
 tags, enabled, is_builtin, source_path
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s,%s)
ON CONFLICT(plugin_key) DO UPDATE SET
 python_module=EXCLUDED.python_module, python_class=EXCLUDED.python_class,
 category=EXCLUDED.category, display_name=EXCLUDED.display_name, description=EXCLUDED.description,
 contract_version=EXCLUDED.contract_version, config_schema=EXCLUDED.config_schema,
 input_schema=EXCLUDED.input_schema, output_schema=EXCLUDED.output_schema, ui_schema=EXCLUDED.ui_schema,
 examples_json=EXCLUDED.examples_json, tags=EXCLUDED.tags, enabled=EXCLUDED.enabled,
 source_path=EXCLUDED.source_path, updated_at_utc=NOW()
RETURNING id::text;
""",
            (
                row_id, plugin_key, str(payload.get("python_module") or ""), str(payload.get("python_class") or ""),
                str(payload.get("category") or "general"), display_name, str(payload.get("description") or ""),
                str(payload.get("contract_version") or "1.0.0"),
                json.dumps(_json(payload.get("config_schema"), {})), json.dumps(_json(payload.get("input_schema"), {})),
                json.dumps(_json(payload.get("output_schema"), {})), json.dumps(_json(payload.get("ui_schema"), {})),
                json.dumps(_json(payload.get("examples_json", payload.get("examples")), [])),
                json.dumps(_json(payload.get("tags"), [])), bool(payload.get("enabled", True)),
                bool(payload.get("is_builtin", False)), str(payload.get("source_path") or ""),
            ),
        )
        saved_id = str(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO workflow_definition_audit(entity_type, entity_key, action, actor, before_json, after_json) VALUES('plugin',%s,%s,%s,%s::jsonb,%s::jsonb)",
            (plugin_key, "update" if before else "create", actor, json.dumps(before[0] if before else {}), json.dumps(payload)),
        )
        conn.commit()
    return {"id": saved_id, "plugin_key": plugin_key}


def list_workflow_definitions(store: Any) -> list[dict[str, Any]]:
    ensure_workflow_schema(store)
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
SELECT w.id::text, w.workflow_key, w.version, w.name, w.description, w.status, w.trigger_mode,
       w.input_schema, w.ui_schema, w.tags, w.updated_at_utc,
       COALESCE(COUNT(s.id),0) AS step_count
FROM workflow_definitions w
LEFT JOIN workflow_steps s ON s.workflow_definition_id = w.id AND COALESCE(s.is_archived, FALSE) = FALSE
GROUP BY w.id
ORDER BY w.updated_at_utc DESC, w.workflow_key ASC;
"""
        )
        return [
            {
                "id": r[0], "workflow_key": r[1], "version": int(r[2]), "name": r[3],
                "description": r[4], "status": r[5], "trigger_mode": r[6],
                "input_schema": r[7] or {}, "ui_schema": r[8] or {}, "tags": r[9] or [],
                "updated_at_utc": r[10].isoformat() if r[10] else None, "step_count": int(r[11] or 0),
            }
            for r in cur.fetchall()
        ]


def get_workflow_definition(store: Any, workflow_key: str) -> dict[str, Any] | None:
    ensure_workflow_schema(store)
    key = slugify_key(workflow_key, fallback="")
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
SELECT id::text, workflow_key, version, name, description, status, trigger_mode, input_schema, ui_schema, tags
FROM workflow_definitions WHERE workflow_key=%s;
""",
            (key,),
        )
        r = cur.fetchone()
        if not r:
            return None
        cur.execute(
            """
SELECT id::text, step_key, display_name, plugin_key, ordinal, enabled, continue_on_error,
       retry_failed, max_attempts, timeout_seconds, input_bindings, config_json,
       preconditions_json, outputs_json
FROM workflow_steps
WHERE workflow_definition_id=%s
  AND COALESCE(is_archived, FALSE) = FALSE
ORDER BY ordinal ASC;
""",
            (r[0],),
        )
        steps = [
            {
                "id": s[0], "step_key": s[1], "display_name": s[2], "plugin_key": s[3],
                "ordinal": int(s[4]), "enabled": bool(s[5]), "continue_on_error": bool(s[6]),
                "retry_failed": bool(s[7]), "max_attempts": max(1, int(s[8] or 1)), "timeout_seconds": int(s[9] or 0),
                "input_bindings": s[10] or {}, "config_json": s[11] or {},
                "preconditions_json": s[12] or {}, "outputs_json": s[13] or {},
            }
            for s in cur.fetchall()
        ]
        return {
            "id": r[0], "workflow_key": r[1], "version": int(r[2]), "name": r[3],
            "description": r[4], "status": r[5], "trigger_mode": r[6],
            "input_schema": r[7] or {}, "ui_schema": r[8] or {}, "tags": r[9] or [],
            "steps": steps,
        }


def save_workflow_definition(store: Any, payload: dict[str, Any], *, actor: str = "") -> dict[str, Any]:
    ensure_workflow_schema(store)
    draft_steps = payload.get("steps") if isinstance(payload.get("steps"), list) else []
    try:
        WorkflowDefinitionSchema.model_validate(
            {
                "workflow_key": str(payload.get("workflow_key") or payload.get("name") or "workflow"),
                "version": int(payload.get("version") or 1),
                "name": str(payload.get("name") or payload.get("workflow_key") or "Workflow"),
                "description": str(payload.get("description") or ""),
                "status": str(payload.get("status") or "draft"),
                "trigger_mode": str(payload.get("trigger_mode") or "manual"),
                "input_schema": _json(payload.get("input_schema"), {}),
                "ui_schema": _json(payload.get("ui_schema"), {}),
                "tags": _json(payload.get("tags"), []),
                "steps": [
                    {
                        "step_key": str(step.get("step_key") or step.get("display_name") or f"step-{idx}"),
                        "display_name": str(step.get("display_name") or ""),
                        "plugin_key": str(step.get("plugin_key") or step.get("plugin_name") or step.get("type") or ""),
                        "ordinal": int(step.get("ordinal") or idx),
                        "enabled": bool(step.get("enabled", True)),
                        "continue_on_error": bool(step.get("continue_on_error", False)),
                        "retry_failed": bool(step.get("retry_failed", True)),
                        "retry_limit": int(step.get("retry_limit") or 3),
                        "max_attempts": (int(step.get("max_attempts")) if step.get("max_attempts") not in (None, "") else None),
                        "timeout_seconds": int(step.get("timeout_seconds") or 0),
                        "input_bindings": _json(step.get("input_bindings"), {}),
                        "config_json": _json(step.get("config_json", step.get("config")), {}),
                        "preconditions_json": _json(step.get("preconditions_json", step.get("preconditions")), {}),
                        "outputs_json": _json(step.get("outputs_json", step.get("outputs")), {}),
                    }
                    for idx, step in enumerate(draft_steps, start=1)
                    if isinstance(step, dict)
                ],
            }
        )
    except Exception as exc:
        raise ValueError(f"invalid workflow definition payload: {exc}") from exc
    workflow_key = slugify_key(str(payload.get("workflow_key") or payload.get("name") or ""), fallback="workflow")
    name = str(payload.get("name") or workflow_key).strip()
    row_id = str(payload.get("id") or uuid.uuid4())
    steps = payload.get("steps") if isinstance(payload.get("steps"), list) else []
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_jsonb(w.*) FROM workflow_definitions w WHERE workflow_key=%s", (workflow_key,))
        before = cur.fetchone()
        cur.execute(
            """
INSERT INTO workflow_definitions(id, workflow_key, version, name, description, status, trigger_mode, input_schema, ui_schema, tags, created_by, updated_by)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s)
ON CONFLICT(workflow_key) DO UPDATE SET
  name=EXCLUDED.name, description=EXCLUDED.description, status=EXCLUDED.status,
  trigger_mode=EXCLUDED.trigger_mode, input_schema=EXCLUDED.input_schema,
  ui_schema=EXCLUDED.ui_schema, tags=EXCLUDED.tags, updated_by=EXCLUDED.updated_by,
  updated_at_utc=NOW()
RETURNING id::text, version;
""",
            (
                row_id, workflow_key, int(payload.get("version") or 1), name, str(payload.get("description") or ""),
                str(payload.get("status") or "draft"), str(payload.get("trigger_mode") or "manual"),
                json.dumps(_json(payload.get("input_schema"), {})), json.dumps(_json(payload.get("ui_schema"), {})),
                json.dumps(_json(payload.get("tags"), [])), actor, actor,
            ),
        )
        wf_id, version = cur.fetchone()
        cur.execute(
            """
SELECT id::text, step_key, ordinal, COALESCE(is_archived, FALSE)
FROM workflow_steps
WHERE workflow_definition_id=%s
ORDER BY ordinal ASC;
""",
            (wf_id,),
        )
        existing_steps = cur.fetchall()
        existing_by_id: dict[str, dict[str, Any]] = {}
        existing_by_step_key: dict[str, dict[str, Any]] = {}
        for row in existing_steps:
            row_id = str(row[0] or "").strip()
            row_step_key = str(row[1] or "").strip().lower()
            row_ordinal = int(row[2] or 0)
            row_archived = bool(row[3]) if len(row) > 3 else False
            if not row_id:
                continue
            item = {"id": row_id, "step_key": row_step_key, "ordinal": row_ordinal, "is_archived": row_archived}
            existing_by_id[row_id] = item
            if row_step_key and row_step_key not in existing_by_step_key:
                existing_by_step_key[row_step_key] = item

        normalized_steps: list[dict[str, Any]] = []
        seen_step_keys: set[str] = set()
        seen_ordinals: set[int] = set()
        for idx, step in enumerate(steps, start=1):
            step_key = slugify_key(str(step.get("step_key") or step.get("display_name") or f"step-{idx}"), fallback=f"step-{idx}")
            plugin_key = slugify_key(str(step.get("plugin_key") or step.get("plugin_name") or step.get("stage") or ""), fallback="")
            if not plugin_key:
                raise ValueError(f"step {idx} is missing plugin_key")
            ordinal = int(step.get("ordinal") or idx)
            if step_key in seen_step_keys:
                raise ValueError(f"duplicate step_key in workflow payload: {step_key}")
            if ordinal in seen_ordinals:
                raise ValueError(f"duplicate step ordinal in workflow payload: {ordinal}")
            seen_step_keys.add(step_key)
            seen_ordinals.add(ordinal)

            incoming_id = str(step.get("id") or "").strip()
            step_id = ""
            if incoming_id and incoming_id in existing_by_id:
                step_id = incoming_id
            elif step_key in existing_by_step_key:
                step_id = str(existing_by_step_key[step_key].get("id") or "").strip()
            if not step_id:
                step_id = str(uuid.uuid4())
            normalized_steps.append(
                {
                    "id": step_id,
                    "step_key": step_key,
                    "display_name": str(step.get("display_name") or step_key),
                    "plugin_key": plugin_key,
                    "ordinal": ordinal,
                    "enabled": bool(step.get("enabled", True)),
                    "continue_on_error": bool(step.get("continue_on_error", False)),
                    "retry_failed": bool(step.get("retry_failed", False)),
                    "max_attempts": workflow_total_attempts(step.get("retry_limit")),
                    "timeout_seconds": int(step.get("timeout_seconds") or 0),
                    "input_bindings": json.dumps(_json(step.get("input_bindings"), {})),
                    "config_json": json.dumps(_json(step.get("config_json", step.get("config")), {})),
                    "preconditions_json": json.dumps(_json(step.get("preconditions_json", step.get("preconditions")), {})),
                    "outputs_json": json.dumps(_json(step.get("outputs_json", step.get("outputs")), {})),
                }
            )

        retained_ids = {str(item["id"]) for item in normalized_steps}
        retired_ids = [row_id for row_id in existing_by_id.keys() if row_id not in retained_ids]
        max_incoming_ordinal = max((int(item["ordinal"]) for item in normalized_steps), default=0)
        max_existing_ordinal = max((int(item.get("ordinal") or 0) for item in existing_by_id.values()), default=0)
        retire_ordinal_base = max(max_incoming_ordinal, max_existing_ordinal, 0) + 1000
        for offset, step_id in enumerate(retired_ids, start=1):
            cur.execute(
                """
UPDATE workflow_steps
SET is_archived = TRUE,
    enabled = FALSE,
    ordinal = %s,
    updated_at_utc = NOW()
WHERE id = %s AND workflow_definition_id = %s;
""",
                (retire_ordinal_base + offset, step_id, wf_id),
            )

        for step in normalized_steps:
            cur.execute(
                """
INSERT INTO workflow_steps(
  id, workflow_definition_id, step_key, display_name, plugin_key, ordinal,
  is_archived, enabled, continue_on_error, retry_failed, max_attempts, timeout_seconds,
  input_bindings, config_json, preconditions_json, outputs_json
)
VALUES(%s,%s,%s,%s,%s,%s,FALSE,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb)
ON CONFLICT (id) DO UPDATE SET
  step_key = EXCLUDED.step_key,
  display_name = EXCLUDED.display_name,
  plugin_key = EXCLUDED.plugin_key,
  ordinal = EXCLUDED.ordinal,
  is_archived = FALSE,
  enabled = EXCLUDED.enabled,
  continue_on_error = EXCLUDED.continue_on_error,
  retry_failed = EXCLUDED.retry_failed,
  max_attempts = EXCLUDED.max_attempts,
  timeout_seconds = EXCLUDED.timeout_seconds,
  input_bindings = EXCLUDED.input_bindings,
  config_json = EXCLUDED.config_json,
  preconditions_json = EXCLUDED.preconditions_json,
  outputs_json = EXCLUDED.outputs_json,
  updated_at_utc = NOW();
""",
                (
                    str(step["id"]),
                    wf_id,
                    str(step["step_key"]),
                    str(step["display_name"]),
                    str(step["plugin_key"]),
                    int(step["ordinal"]),
                    bool(step["enabled"]),
                    bool(step["continue_on_error"]),
                    bool(step["retry_failed"]),
                    int(step["max_attempts"]),
                    int(step["timeout_seconds"]),
                    str(step["input_bindings"]),
                    str(step["config_json"]),
                    str(step["preconditions_json"]),
                    str(step["outputs_json"]),
                ),
            )
        cur.execute(
            "INSERT INTO workflow_definition_audit(entity_type, entity_key, entity_version, action, actor, before_json, after_json) VALUES('workflow',%s,%s,%s,%s,%s::jsonb,%s::jsonb)",
            (workflow_key, int(version or 1), "update" if before else "create", actor, json.dumps(before[0] if before else {}), json.dumps(payload)),
        )
        conn.commit()
    return {"id": wf_id, "workflow_key": workflow_key, "version": int(version or 1)}


def publish_workflow_definition(store: Any, workflow_key: str, *, actor: str = "") -> dict[str, Any]:
    ensure_workflow_schema(store)
    definition = get_workflow_definition(store, workflow_key)
    if not definition:
        raise KeyError("workflow definition not found")
    if not definition.get("steps"):
        raise ValueError("workflow must contain at least one step before publishing")
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE workflow_definitions SET status='published', version=version+1, updated_by=%s, updated_at_utc=NOW() WHERE workflow_key=%s RETURNING version",
            (actor, definition["workflow_key"]),
        )
        version = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO workflow_definition_audit(entity_type, entity_key, entity_version, action, actor, after_json) VALUES('workflow',%s,%s,'publish',%s,%s::jsonb)",
            (definition["workflow_key"], version, actor, json.dumps(definition)),
        )
        conn.commit()
    return {"workflow_key": definition["workflow_key"], "version": version, "status": "published"}


def create_workflow_run(store: Any, payload: dict[str, Any], *, actor: str = "") -> dict[str, Any]:
    ensure_workflow_schema(store)
    workflow_key = slugify_key(str(payload.get("workflow_key") or ""), fallback="")
    definition = get_workflow_definition(store, workflow_key)
    if not definition:
        raise KeyError("workflow definition not found")
    from app_platform.workflow.validation import validate_workflow_definition

    validation_errors = validate_workflow_definition(definition)
    if validation_errors:
        raise ValueError("invalid workflow definition: " + "; ".join(validation_errors))
    run_id = str(uuid.uuid4())
    root_domain = str(payload.get("root_domain") or payload.get("input", {}).get("root_domain") or "").strip().lower()
    if not root_domain:
        raise ValueError("root_domain is required")
    input_json = _json(payload.get("input_json", payload.get("input")), {})
    workflow_type = str((definition.get("ui_schema") or {}).get("workflow_type") or "single_run").strip().lower() or "single_run"
    runtime_variables = (definition.get("ui_schema") or {}).get("tailor_runtime_variables")
    runtime_variables = runtime_variables if isinstance(runtime_variables, dict) else {}
    iteration_items = input_json.get("iteration_items")
    is_iteration = workflow_type.startswith("iteration_over_") and isinstance(iteration_items, list) and bool(iteration_items)
    run_scope_ids: list[str] = []
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
INSERT INTO workflow_runs(id, workflow_definition_id, workflow_key, workflow_version, root_domain, status, trigger_source, input_json, created_by)
VALUES(%s,%s,%s,%s,%s,'queued',%s,%s::jsonb,%s);
""",
            (run_id, definition["id"], definition["workflow_key"], int(definition.get("version") or 1), root_domain, str(payload.get("trigger_source") or "manual"), json.dumps(input_json), actor),
        )
        for step in definition["steps"]:
            step_max_attempts = workflow_total_attempts(step.get("retry_limit"))
            preconditions = step.get("preconditions_json") or {}
            plugin_key = str(step.get("plugin_key") or "").strip().lower()
            has_prerequisites = _preconditions_require_wait(preconditions, plugin_key=plugin_key)
            initial_status = "pending" if has_prerequisites else "ready"
            initial_blocked_reason = "Waiting for Prerequisites..." if has_prerequisites else ""
            units: list[dict[str, Any]]
            if is_iteration:
                units = [
                    {"index": idx + 1, "item": item, "workflow_scope_id": f"{definition['workflow_key']}.run.{run_id}.iter.{idx + 1}"}
                    for idx, item in enumerate(iteration_items)
                ]
            else:
                units = [{"index": 0, "item": None, "workflow_scope_id": str(definition["workflow_key"])}]
            for unit in units:
                unit_index = int(unit["index"])
                unit_item = unit.get("item")
                workflow_scope_id = str(unit["workflow_scope_id"])
                if workflow_scope_id not in run_scope_ids:
                    run_scope_ids.append(workflow_scope_id)
                unit_input = dict(input_json)
                if unit_index > 0:
                    unit_input["iteration_index"] = unit_index
                    unit_input["iteration_item"] = unit_item
                resolved_inputs = resolve_bindings(step.get("input_bindings"), workflow_input=unit_input)
                step_config = step.get("config_json") if isinstance(step.get("config_json"), dict) else {}
                resolved_config = {**step_config, **resolved_inputs}
                step_run_key = str(step["step_key"]) if unit_index <= 0 else f"{step['step_key']}__iter_{unit_index}"
                cur.execute(
                """
INSERT INTO workflow_step_runs(
  id, workflow_run_id, workflow_definition_id, step_definition_id, step_key, plugin_key,
  ordinal, status, blocked_reason, max_attempts, input_json, resolved_config_json
)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb);
ON CONFLICT(workflow_run_id, step_key) DO UPDATE SET
  status = EXCLUDED.status,
  blocked_reason = EXCLUDED.blocked_reason,
  max_attempts = EXCLUDED.max_attempts,
  input_json = EXCLUDED.input_json,
  resolved_config_json = EXCLUDED.resolved_config_json,
  updated_at_utc = NOW();
""",
                (
                    str(uuid.uuid4()), run_id, definition["id"], step["id"], step_run_key, step["plugin_key"],
                    int(step["ordinal"]), initial_status, initial_blocked_reason, step_max_attempts,
                    json.dumps(unit_input), json.dumps(resolved_config),
                ),
            )
                # Compatibility bridge: enqueue coordinator stage tasks so existing
                # workers can process DB-backed workflow runs. "pending" means
                # waiting for prerequisites; only "ready" tasks are claimable.
                if root_domain:
                    checkpoint = {
                        "workflow_run_id": run_id,
                        "step_key": step_run_key,
                        "base_step_key": step["step_key"],
                        "preconditions_json": preconditions if isinstance(preconditions, dict) else {},
                        "max_attempts": step_max_attempts,
                        "retry_failed": bool(step.get("retry_failed", False)),
                        "workflow_type": workflow_type,
                        "runtime_variables": runtime_variables,
                        "workflow_definition_key": definition["workflow_key"],
                    }
                    if unit_index > 0:
                        checkpoint["iteration_mode"] = workflow_type
                        checkpoint["iteration_index"] = unit_index
                        checkpoint["iteration_item"] = unit_item
                    cur.execute(
                    """
INSERT INTO coordinator_stage_tasks(workflow_id, root_domain, stage, status, checkpoint_json, error, max_attempts)
VALUES(%s,%s,%s,%s,%s::jsonb,%s,%s)
ON CONFLICT(workflow_id, root_domain, stage) DO UPDATE SET
  status=EXCLUDED.status,
  checkpoint_json=EXCLUDED.checkpoint_json,
  worker_id=NULL,
  lease_expires_at=NULL,
  heartbeat_at_utc=NULL,
  completed_at_utc=NULL,
  error=EXCLUDED.error,
  max_attempts=EXCLUDED.max_attempts,
  updated_at_utc=NOW();
""",
                    (
                        workflow_scope_id,
                        root_domain,
                        step["plugin_key"],
                        initial_status,
                        json.dumps(checkpoint),
                        initial_blocked_reason,
                        step_max_attempts,
                    ),
                    )
        conn.commit()
    # File/artifact and plugin prerequisite evaluation happens here and on
    # every worker poll/completion. This makes newly-created runs immediately
    # claimable when their first ready step has no unmet prerequisite.
    try:
        for wid in (run_scope_ids or [str(definition["workflow_key"])]):
            store.refresh_stage_task_readiness(root_domain=root_domain, workflow_id=wid, limit=5000)
    except Exception:
        pass

    persisted_stage_task_rows = 0
    if hasattr(store, "count_stage_tasks"):
        try:
            for wid in (run_scope_ids or [str(definition["workflow_key"])]):
                persisted_stage_task_rows += int(
                    store.count_stage_tasks(
                        workflow_id=wid,
                        root_domains=[root_domain],
                        plugins=[],
                    )
                    or 0
                )
        except Exception:
            persisted_stage_task_rows = 0
    else:
        persisted_stage_task_rows = len(definition.get("steps") or [])

    if persisted_stage_task_rows <= 0 and bool(definition.get("steps")):
        raise RuntimeError("workflow run created but no rows were persisted to coordinator_stage_tasks")

    result = {
        "id": run_id,
        "workflow_key": definition["workflow_key"],
        "root_domain": root_domain,
        "status": "queued",
        "persisted_stage_task_rows": persisted_stage_task_rows,
    }
    if hasattr(store, "record_system_event"):
        try:
            store.record_system_event(
                "workflow.definition.validated",
                f"workflow_definition:{definition['workflow_key']}",
                {
                    "source": "workflow_app.store.create_workflow_run",
                    "workflow_id": definition["workflow_key"],
                    "workflow_run_id": run_id,
                    "status": "validated",
                },
            )
            store.record_system_event(
                "workflow.run.created",
                f"workflow_run:{run_id}",
                {
                    "source": "workflow_app.store.create_workflow_run",
                    "workflow_id": definition["workflow_key"],
                    "workflow_run_id": run_id,
                    "root_domain": root_domain,
                    "status": "queued",
                },
            )
            if bool(definition.get("steps")):
                store.record_system_event(
                    "workflow.run.started",
                    f"workflow_run:{run_id}",
                    {
                        "source": "workflow_app.store.create_workflow_run",
                        "workflow_id": definition["workflow_key"],
                        "workflow_run_id": run_id,
                        "root_domain": root_domain,
                        "status": "queued",
                        "message": "Run initialized and tasks enqueued.",
                    },
                )
        except Exception:
            pass
    return result


def list_workflow_runs(store: Any, *, limit: int = 100) -> list[dict[str, Any]]:
    ensure_workflow_schema(store)
    safe_limit = max(1, min(500, int(limit or 100)))
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
SELECT r.id::text, r.workflow_key, r.workflow_version, r.root_domain, r.status, r.trigger_source,
       r.created_at_utc, r.updated_at_utc,
       COUNT(s.id) FILTER (WHERE s.status='completed') AS completed_steps,
       COUNT(s.id) AS total_steps
FROM workflow_runs r
LEFT JOIN workflow_step_runs s ON s.workflow_run_id = r.id
GROUP BY r.id
ORDER BY r.created_at_utc DESC
LIMIT %s;
""",
            (safe_limit,),
        )
        return [
            {
                "id": r[0], "workflow_key": r[1], "workflow_version": int(r[2]), "root_domain": r[3],
                "status": r[4], "trigger_source": r[5],
                "created_at_utc": r[6].isoformat() if r[6] else None,
                "updated_at_utc": r[7].isoformat() if r[7] else None,
                "completed_steps": int(r[8] or 0), "total_steps": int(r[9] or 0),
            }
            for r in cur.fetchall()
        ]


def get_workflow_run(store: Any, run_id: str) -> dict[str, Any] | None:
    ensure_workflow_schema(store)
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT id::text, workflow_key, workflow_version, root_domain, status, input_json, created_at_utc FROM workflow_runs WHERE id=%s", (run_id,))
        r = cur.fetchone()
        if not r:
            return None
        cur.execute(
            """
SELECT id::text, step_key, plugin_key, ordinal, status, blocked_reason, worker_id, attempt_count, max_attempts,
       input_json, resolved_config_json, output_json, error, started_at_utc, completed_at_utc
FROM workflow_step_runs WHERE workflow_run_id=%s ORDER BY ordinal ASC;
""",
            (run_id,),
        )
        steps = [
            {
                "id": s[0], "step_key": s[1], "plugin_key": s[2], "ordinal": int(s[3]),
                "status": s[4], "blocked_reason": s[5], "worker_id": s[6],
                "attempt_count": int(s[7] or 0), "max_attempts": max(1, int(s[8] or 1)),
                "input_json": s[9] or {}, "resolved_config_json": s[10] or {}, "output_json": s[11] or {},
                "error": s[12], "started_at_utc": s[13].isoformat() if s[13] else None,
                "completed_at_utc": s[14].isoformat() if s[14] else None,
            }
            for s in cur.fetchall()
        ]
    return {"id": r[0], "workflow_key": r[1], "workflow_version": int(r[2]), "root_domain": r[3], "status": r[4], "input_json": r[5] or {}, "created_at_utc": r[6].isoformat() if r[6] else None, "steps": steps}
