
from __future__ import annotations

import json
import os
import uuid
from typing import Any

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

from auth0r.crypto import decrypt_text, encrypt_text
from auth0r.policy import ReplayPolicy
from auth0r.types import AuthIdentity, AuthVerificationMarker


def _json_load(value: Any, default: Any):
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        parsed = json.loads(value)
    except Exception:
        return default
    return parsed if isinstance(parsed, type(default)) else default


def _split_lines(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [line.strip() for line in str(value or "").replace("\r", "\n").split("\n") if line.strip()]


def _normalize_markers(value: Any) -> list[dict[str, str]]:
    if isinstance(value, list):
        out: list[dict[str, str]] = []
        for item in value:
            if isinstance(item, dict):
                marker_value = str(item.get("value", "") or "").strip()
                if not marker_value:
                    continue
                out.append({"kind": str(item.get("kind", "text") or "text"), "value": marker_value})
            else:
                marker_value = str(item or "").strip()
                if marker_value:
                    out.append({"kind": "text", "value": marker_value})
        return out
    return [{"kind": "text", "value": item} for item in _split_lines(value)]


def _jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {})


class Auth0rProfileStore:
    def __init__(self, database_url: str | None = None):
        if psycopg is None:
            raise RuntimeError("psycopg is required")
        self.database_url = (
            str(database_url or "").strip()
            or os.getenv("AUTH0R_DATABASE_URL", "").strip()
            or os.getenv("DATABASE_URL", "").strip()
            or os.getenv("COORDINATOR_DATABASE_URL", "").strip()
        )
        if not self.database_url:
            raise ValueError("database_url is required")
        self._ensure_schema()

    def _connect(self):
        return psycopg.connect(self.database_url, autocommit=False)

    def _ensure_schema(self) -> None:
        ddl = """
CREATE TABLE IF NOT EXISTS auth0r_profiles (
  id UUID PRIMARY KEY,
  root_domain TEXT NOT NULL,
  profile_label TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  allowed_hosts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  default_headers_encrypted BYTEA,
  replay_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_auth0r_profiles_domain_enabled ON auth0r_profiles(root_domain, enabled);

CREATE TABLE IF NOT EXISTS auth0r_identities (
  id UUID PRIMARY KEY,
  profile_id UUID NOT NULL REFERENCES auth0r_profiles(id) ON DELETE CASCADE,
  identity_label TEXT NOT NULL,
  role_label TEXT NOT NULL DEFAULT '',
  tenant_label TEXT NOT NULL DEFAULT '',
  login_strategy TEXT NOT NULL DEFAULT 'cookie_import',
  username_encrypted BYTEA,
  password_encrypted BYTEA,
  login_config_json_encrypted BYTEA,
  custom_headers_json_encrypted BYTEA,
  success_markers_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  denial_markers_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_auth0r_identities_profile_enabled ON auth0r_identities(profile_id, enabled);

CREATE TABLE IF NOT EXISTS auth0r_cookie_jars (
  id UUID PRIMARY KEY,
  identity_id UUID NOT NULL REFERENCES auth0r_identities(id) ON DELETE CASCADE,
  jar_label TEXT NOT NULL DEFAULT 'default',
  cookies_json_encrypted BYTEA NOT NULL,
  host_scope_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  path_scope_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS auth0r_runtime_sessions (
  id UUID PRIMARY KEY,
  root_domain TEXT NOT NULL,
  identity_id UUID NOT NULL,
  source_type TEXT NOT NULL,
  generation_number INTEGER NOT NULL DEFAULT 1,
  verified BOOLEAN NOT NULL DEFAULT FALSE,
  verification_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  cookies_encrypted BYTEA,
  auth_headers_encrypted BYTEA,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_auth0r_runtime_sessions_domain ON auth0r_runtime_sessions(root_domain, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS auth0r_recorded_actions (
  id UUID PRIMARY KEY,
  root_domain TEXT NOT NULL,
  identity_id UUID NOT NULL,
  runtime_session_id UUID NOT NULL,
  url TEXT NOT NULL,
  method TEXT NOT NULL,
  source TEXT NOT NULL,
  content_type TEXT NOT NULL DEFAULT '',
  likely_state_changing BOOLEAN NOT NULL DEFAULT FALSE,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_auth0r_recorded_actions_domain ON auth0r_recorded_actions(root_domain, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS auth0r_replay_attempts (
  id UUID PRIMARY KEY,
  root_domain TEXT NOT NULL,
  identity_id UUID NOT NULL,
  recorded_action_id UUID NOT NULL,
  replay_variant TEXT NOT NULL,
  status_code INTEGER,
  suspicious BOOLEAN NOT NULL DEFAULT FALSE,
  summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_auth0r_replay_attempts_domain ON auth0r_replay_attempts(root_domain, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS auth0r_findings (
  id UUID PRIMARY KEY,
  root_domain TEXT NOT NULL,
  identity_id UUID,
  finding_type TEXT NOT NULL,
  severity TEXT NOT NULL DEFAULT 'medium',
  title TEXT NOT NULL,
  endpoint TEXT NOT NULL DEFAULT '',
  replay_variant TEXT NOT NULL DEFAULT '',
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
  summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_auth0r_findings_domain ON auth0r_findings(root_domain, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS auth0r_evidence (
  id UUID PRIMARY KEY,
  root_domain TEXT NOT NULL,
  identity_id UUID,
  category TEXT NOT NULL,
  ref_id TEXT NOT NULL DEFAULT '',
  payload_encrypted BYTEA NOT NULL,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_auth0r_evidence_domain ON auth0r_evidence(root_domain, created_at_utc DESC);
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def list_domains_overview(self, limit: int = 250) -> list[dict[str, Any]]:
        sql = """
WITH profile_counts AS (
  SELECT root_domain, COUNT(*)::int AS profile_count
  FROM auth0r_profiles
  GROUP BY root_domain
),
identity_counts AS (
  SELECT p.root_domain, COUNT(i.id)::int AS identity_count
  FROM auth0r_profiles p
  LEFT JOIN auth0r_identities i ON i.profile_id = p.id
  GROUP BY p.root_domain
),
result_counts AS (
  SELECT root_domain,
         COUNT(*) FILTER (WHERE finding_type = 'authorization')::int AS authorization_findings,
         COUNT(*) FILTER (WHERE finding_type = 'session_lifecycle')::int AS lifecycle_findings,
         MAX(created_at_utc) AS last_finding_at_utc
  FROM auth0r_findings
  GROUP BY root_domain
),
session_counts AS (
  SELECT root_domain,
         COUNT(*)::int AS session_count,
         MAX(created_at_utc) AS last_session_at_utc
  FROM auth0r_runtime_sessions
  GROUP BY root_domain
),
action_counts AS (
  SELECT root_domain, COUNT(*)::int AS action_count
  FROM auth0r_recorded_actions
  GROUP BY root_domain
),
replay_counts AS (
  SELECT root_domain,
         COUNT(*)::int AS replay_count,
         COUNT(*) FILTER (WHERE suspicious = TRUE)::int AS suspicious_replay_count
  FROM auth0r_replay_attempts
  GROUP BY root_domain
)
SELECT
  p.root_domain,
  p.profile_count,
  COALESCE(i.identity_count, 0) AS identity_count,
  COALESCE(s.session_count, 0) AS session_count,
  COALESCE(a.action_count, 0) AS action_count,
  COALESCE(r.replay_count, 0) AS replay_count,
  COALESCE(r.suspicious_replay_count, 0) AS suspicious_replay_count,
  COALESCE(f.authorization_findings, 0) AS authorization_findings,
  COALESCE(f.lifecycle_findings, 0) AS lifecycle_findings,
  GREATEST(
    COALESCE(f.last_finding_at_utc, TIMESTAMPTZ 'epoch'),
    COALESCE(s.last_session_at_utc, TIMESTAMPTZ 'epoch')
  ) AS last_activity_at_utc
FROM profile_counts p
LEFT JOIN identity_counts i ON i.root_domain = p.root_domain
LEFT JOIN session_counts s ON s.root_domain = p.root_domain
LEFT JOIN action_counts a ON a.root_domain = p.root_domain
LEFT JOIN replay_counts r ON r.root_domain = p.root_domain
LEFT JOIN result_counts f ON f.root_domain = p.root_domain
ORDER BY last_activity_at_utc DESC NULLS LAST, p.root_domain ASC
LIMIT %s
"""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, (max(1, min(int(limit or 250), 2000)),))
            rows = cur.fetchall()
        out = []
        for row in rows:
            out.append({
                "root_domain": row[0],
                "profile_count": int(row[1] or 0),
                "identity_count": int(row[2] or 0),
                "session_count": int(row[3] or 0),
                "action_count": int(row[4] or 0),
                "replay_count": int(row[5] or 0),
                "suspicious_replay_count": int(row[6] or 0),
                "authorization_findings": int(row[7] or 0),
                "lifecycle_findings": int(row[8] or 0),
                "last_activity_at_utc": (row[9].isoformat() if row[9] else ""),
            })
        return out

    def list_profile_domains(self, limit: int = 1000) -> list[str]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT DISTINCT root_domain FROM auth0r_profiles ORDER BY root_domain ASC LIMIT %s", (max(1, min(limit, 5000)),))
            return [str(row[0]) for row in cur.fetchall()]

    def get_domain_profiles(self, root_domain: str) -> dict[str, Any]:
        root_domain = str(root_domain or "").strip().lower()
        profiles_sql = """
SELECT id::text, profile_label, enabled, allowed_hosts_json, replay_policy_json, default_headers_encrypted, created_at_utc, updated_at_utc
FROM auth0r_profiles
WHERE root_domain = %s
ORDER BY created_at_utc ASC
"""
        identities_sql = """
SELECT
  i.id::text,
  i.profile_id::text,
  i.identity_label,
  i.role_label,
  i.tenant_label,
  i.login_strategy,
  i.username_encrypted,
  i.password_encrypted,
  i.login_config_json_encrypted,
  i.custom_headers_json_encrypted,
  i.success_markers_json,
  i.denial_markers_json,
  i.enabled,
  i.created_at_utc,
  i.updated_at_utc
FROM auth0r_identities i
JOIN auth0r_profiles p ON p.id = i.profile_id
WHERE p.root_domain = %s
ORDER BY i.created_at_utc ASC
"""
        profiles: dict[str, Any] = {}
        identities: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(profiles_sql, (root_domain,))
                for row in cur.fetchall():
                    profiles[row[0]] = {
                        "id": row[0],
                        "root_domain": root_domain,
                        "profile_label": row[1],
                        "enabled": bool(row[2]),
                        "allowed_hosts": _json_load(row[3], []),
                        "replay_policy": _json_load(row[4], {}),
                        "default_headers": _json_load(decrypt_text(row[5]), {}) if row[5] else {},
                        "created_at_utc": row[6].isoformat() if row[6] else "",
                        "updated_at_utc": row[7].isoformat() if row[7] else "",
                        "identities": [],
                    }
                cur.execute(identities_sql, (root_domain,))
                rows = cur.fetchall()
            for row in rows:
                login_cfg = _json_load(decrypt_text(row[8]), {}) if row[8] else {}
                custom_headers = _json_load(decrypt_text(row[9]), {}) if row[9] else {}
                cookies = self._load_cookie_jar(row[0])
                identity = {
                    "id": row[0],
                    "profile_id": row[1],
                    "identity_label": row[2],
                    "role_label": row[3] or "",
                    "tenant_label": row[4] or "",
                    "login_strategy": row[5] or "cookie_import",
                    "username": decrypt_text(row[6]) if row[6] else "",
                    "has_password": bool(row[7]),
                    "login_url": str(login_cfg.get("login_url", "") or ""),
                    "login_method": str(login_cfg.get("login_method", "POST") or "POST").upper(),
                    "login_username_field": str(login_cfg.get("username_field", "username") or "username"),
                    "login_password_field": str(login_cfg.get("password_field", "password") or "password"),
                    "login_extra_fields": (login_cfg.get("extra_fields", {}) if isinstance(login_cfg.get("extra_fields", {}), dict) else {}),
                    "authenticated_probe_url": str(login_cfg.get("authenticated_probe_url", "") or ""),
                    "logout_url": str(login_cfg.get("logout_url", "") or ""),
                    "custom_headers": custom_headers if isinstance(custom_headers, dict) else {},
                    "success_markers": _json_load(row[10], []),
                    "denial_markers": _json_load(row[11], []),
                    "enabled": bool(row[12]),
                    "imported_cookie_count": len(cookies),
                    "imported_cookies": cookies,
                    "created_at_utc": row[13].isoformat() if row[13] else "",
                    "updated_at_utc": row[14].isoformat() if row[14] else "",
                }
                identities.append(identity)
                if row[1] in profiles:
                    profiles[row[1]]["identities"].append(identity)
        return {"root_domain": root_domain, "profiles": list(profiles.values()), "identity_count": len(identities)}

    def get_domain_results(self, root_domain: str, limit: int = 200) -> dict[str, Any]:
        root_domain = str(root_domain or "").strip().lower()
        limit = max(1, min(int(limit or 200), 1000))
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
SELECT COUNT(*)::int,
       COUNT(*) FILTER (WHERE verified = TRUE)::int,
       MAX(created_at_utc)
FROM auth0r_runtime_sessions
WHERE root_domain = %s
""",
                (root_domain,),
            )
            session_row = cur.fetchone() or (0, 0, None)
            cur.execute("SELECT COUNT(*)::int FROM auth0r_recorded_actions WHERE root_domain = %s", (root_domain,))
            action_count = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                "SELECT COUNT(*)::int, COUNT(*) FILTER (WHERE suspicious = TRUE)::int FROM auth0r_replay_attempts WHERE root_domain = %s",
                (root_domain,),
            )
            replay_row = cur.fetchone() or (0, 0)
            cur.execute(
                "SELECT COUNT(*)::int, COUNT(*) FILTER (WHERE finding_type = 'authorization')::int, COUNT(*) FILTER (WHERE finding_type = 'session_lifecycle')::int FROM auth0r_findings WHERE root_domain = %s",
                (root_domain,),
            )
            finding_row = cur.fetchone() or (0, 0, 0)
            cur.execute(
                """
SELECT f.id::text, COALESCE(i.identity_label, ''), f.finding_type, f.severity, f.title, f.endpoint, f.replay_variant, f.confidence, f.summary_json, f.created_at_utc
FROM auth0r_findings f
LEFT JOIN auth0r_identities i ON i.id = f.identity_id
WHERE f.root_domain = %s
ORDER BY f.created_at_utc DESC
LIMIT %s
""",
                (root_domain, limit),
            )
            findings = [{
                "id": row[0],
                "identity_label": row[1],
                "finding_type": row[2],
                "severity": row[3],
                "title": row[4],
                "endpoint": row[5],
                "replay_variant": row[6],
                "confidence": float(row[7] or 0),
                "summary": _json_load(row[8], {}),
                "created_at_utc": row[9].isoformat() if row[9] else "",
            } for row in cur.fetchall()]
            cur.execute(
                """
SELECT r.id::text, COALESCE(i.identity_label, ''), a.method, a.url, r.replay_variant, r.status_code, r.suspicious, r.summary_json, r.created_at_utc
FROM auth0r_replay_attempts r
JOIN auth0r_recorded_actions a ON a.id = r.recorded_action_id
LEFT JOIN auth0r_identities i ON i.id = r.identity_id
WHERE r.root_domain = %s
ORDER BY r.created_at_utc DESC
LIMIT %s
""",
                (root_domain, limit),
            )
            replays = [{
                "id": row[0],
                "identity_label": row[1],
                "method": row[2],
                "url": row[3],
                "replay_variant": row[4],
                "status_code": row[5],
                "suspicious": bool(row[6]),
                "summary": _json_load(row[7], {}),
                "created_at_utc": row[8].isoformat() if row[8] else "",
            } for row in cur.fetchall()]
            cur.execute(
                """
SELECT s.id::text, COALESCE(i.identity_label, ''), s.source_type, s.generation_number, s.verified, s.verification_summary_json, s.created_at_utc
FROM auth0r_runtime_sessions s
LEFT JOIN auth0r_identities i ON i.id = s.identity_id
WHERE s.root_domain = %s
ORDER BY s.created_at_utc DESC
LIMIT %s
""",
                (root_domain, limit),
            )
            sessions = [{
                "id": row[0],
                "identity_label": row[1],
                "source_type": row[2],
                "generation_number": int(row[3] or 0),
                "verified": bool(row[4]),
                "verification_summary": _json_load(row[5], {}),
                "created_at_utc": row[6].isoformat() if row[6] else "",
            } for row in cur.fetchall()]
        return {
            "root_domain": root_domain,
            "counts": {
                "session_count": int(session_row[0] or 0),
                "verified_session_count": int(session_row[1] or 0),
                "action_count": action_count,
                "replay_count": int(replay_row[0] or 0),
                "suspicious_replay_count": int(replay_row[1] or 0),
                "finding_count": int(finding_row[0] or 0),
                "authorization_finding_count": int(finding_row[1] or 0),
                "lifecycle_finding_count": int(finding_row[2] or 0),
                "last_session_at_utc": session_row[2].isoformat() if session_row[2] else "",
            },
            "sessions": sessions,
            "replays": replays,
            "findings": findings,
        }

    def upsert_profile(
        self,
        *,
        profile_id: str | None,
        root_domain: str,
        profile_label: str,
        enabled: bool,
        allowed_hosts: list[str] | Any,
        default_headers: dict[str, Any] | Any = None,
        replay_policy: dict[str, Any] | Any = None,
    ) -> str:
        profile_id = str(profile_id or "").strip() or str(uuid.uuid4())
        allowed_hosts_list = [str(item).strip() for item in (allowed_hosts if isinstance(allowed_hosts, list) else _split_lines(allowed_hosts)) if str(item).strip()]
        default_headers_dict = default_headers if isinstance(default_headers, dict) else _json_load(default_headers, {})
        replay_policy_dict = replay_policy if isinstance(replay_policy, dict) else _json_load(replay_policy, {})
        sql = """
INSERT INTO auth0r_profiles
(id, root_domain, profile_label, enabled, allowed_hosts_json, default_headers_encrypted, replay_policy_json, updated_at_utc)
VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, NOW())
ON CONFLICT (id) DO UPDATE SET
  root_domain = EXCLUDED.root_domain,
  profile_label = EXCLUDED.profile_label,
  enabled = EXCLUDED.enabled,
  allowed_hosts_json = EXCLUDED.allowed_hosts_json,
  default_headers_encrypted = EXCLUDED.default_headers_encrypted,
  replay_policy_json = EXCLUDED.replay_policy_json,
  updated_at_utc = NOW()
"""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    profile_id,
                    str(root_domain or "").strip().lower(),
                    str(profile_label or "").strip() or "Default",
                    bool(enabled),
                    _jsonb(allowed_hosts_list),
                    encrypt_text(json.dumps(default_headers_dict or {})),
                    _jsonb(replay_policy_dict or {}),
                ),
            )
            conn.commit()
        return profile_id

    def upsert_identity(
        self,
        *,
        identity_id: str | None,
        profile_id: str,
        identity_label: str,
        role_label: str = "",
        tenant_label: str = "",
        login_strategy: str = "cookie_import",
        username: str = "",
        password: str = "",
        login_url: str = "",
        login_method: str = "POST",
        login_username_field: str = "username",
        login_password_field: str = "password",
        login_extra_fields: dict[str, Any] | Any = None,
        authenticated_probe_url: str = "",
        logout_url: str = "",
        custom_headers: dict[str, Any] | Any = None,
        success_markers: list[dict[str, str]] | Any = None,
        denial_markers: list[dict[str, str]] | Any = None,
        imported_cookies: list[dict[str, Any]] | Any = None,
        enabled: bool = True,
    ) -> str:
        identity_id = str(identity_id or "").strip() or str(uuid.uuid4())
        custom_headers_dict = custom_headers if isinstance(custom_headers, dict) else _json_load(custom_headers, {})
        login_extra_fields_dict = login_extra_fields if isinstance(login_extra_fields, dict) else _json_load(login_extra_fields, {})
        markers_success = _normalize_markers(success_markers)
        markers_denial = _normalize_markers(denial_markers)
        login_cfg = {
            "login_url": str(login_url or "").strip(),
            "login_method": str(login_method or "POST").strip().upper(),
            "username_field": str(login_username_field or "username").strip() or "username",
            "password_field": str(login_password_field or "password").strip() or "password",
            "extra_fields": login_extra_fields_dict if isinstance(login_extra_fields_dict, dict) else {},
            "authenticated_probe_url": str(authenticated_probe_url or "").strip(),
            "logout_url": str(logout_url or "").strip(),
        }
        password_to_store = None
        with self._connect() as conn, conn.cursor() as cur:
            if password:
                password_to_store = encrypt_text(str(password))
            else:
                cur.execute("SELECT password_encrypted FROM auth0r_identities WHERE id = %s", (identity_id,))
                row = cur.fetchone()
                password_to_store = row[0] if row and row[0] else encrypt_text("")
            cur.execute(
                """
INSERT INTO auth0r_identities
(id, profile_id, identity_label, role_label, tenant_label, login_strategy, username_encrypted, password_encrypted, login_config_json_encrypted, custom_headers_json_encrypted, success_markers_json, denial_markers_json, enabled, updated_at_utc)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, NOW())
ON CONFLICT (id) DO UPDATE SET
  profile_id = EXCLUDED.profile_id,
  identity_label = EXCLUDED.identity_label,
  role_label = EXCLUDED.role_label,
  tenant_label = EXCLUDED.tenant_label,
  login_strategy = EXCLUDED.login_strategy,
  username_encrypted = EXCLUDED.username_encrypted,
  password_encrypted = EXCLUDED.password_encrypted,
  login_config_json_encrypted = EXCLUDED.login_config_json_encrypted,
  custom_headers_json_encrypted = EXCLUDED.custom_headers_json_encrypted,
  success_markers_json = EXCLUDED.success_markers_json,
  denial_markers_json = EXCLUDED.denial_markers_json,
  enabled = EXCLUDED.enabled,
  updated_at_utc = NOW()
""",
                (
                    identity_id,
                    profile_id,
                    str(identity_label or "").strip() or "Identity",
                    str(role_label or "").strip(),
                    str(tenant_label or "").strip(),
                    str(login_strategy or "cookie_import").strip(),
                    encrypt_text(str(username or "")),
                    password_to_store,
                    encrypt_text(json.dumps(login_cfg)),
                    encrypt_text(json.dumps(custom_headers_dict or {})),
                    _jsonb(markers_success),
                    _jsonb(markers_denial),
                    bool(enabled),
                ),
            )
            imported_cookie_list = imported_cookies if isinstance(imported_cookies, list) else _json_load(imported_cookies, [])
            if isinstance(imported_cookie_list, list) and imported_cookie_list:
                cur.execute("DELETE FROM auth0r_cookie_jars WHERE identity_id = %s", (identity_id,))
                host_scope = sorted({str(item.get("domain", "") or "").strip() for item in imported_cookie_list if isinstance(item, dict) and str(item.get("domain", "") or "").strip()})
                path_scope = sorted({str(item.get("path", "/") or "/").strip() for item in imported_cookie_list if isinstance(item, dict)})
                cur.execute(
                    """
INSERT INTO auth0r_cookie_jars
(id, identity_id, jar_label, cookies_json_encrypted, host_scope_json, path_scope_json, updated_at_utc)
VALUES (%s, %s, 'default', %s, %s::jsonb, %s::jsonb, NOW())
""",
                    (
                        str(uuid.uuid4()),
                        identity_id,
                        encrypt_text(json.dumps(imported_cookie_list)),
                        _jsonb(host_scope),
                        _jsonb(path_scope),
                    ),
                )
            conn.commit()
        return identity_id

    def delete_profile(self, profile_id: str) -> bool:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM auth0r_profiles WHERE id = %s", (str(profile_id or "").strip(),))
            deleted = cur.rowcount > 0
            conn.commit()
        return deleted

    def delete_identity(self, identity_id: str) -> bool:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM auth0r_identities WHERE id = %s", (str(identity_id or "").strip(),))
            deleted = cur.rowcount > 0
            conn.commit()
        return deleted

    def list_enabled_identities(self, root_domain: str) -> list[AuthIdentity]:
        sql = """
SELECT
  i.id::text,
  i.profile_id::text,
  i.identity_label,
  i.role_label,
  i.tenant_label,
  i.login_strategy,
  i.username_encrypted,
  i.password_encrypted,
  i.login_config_json_encrypted,
  i.custom_headers_json_encrypted,
  i.success_markers_json,
  i.denial_markers_json,
  p.allowed_hosts_json,
  p.replay_policy_json
FROM auth0r_profiles p
JOIN auth0r_identities i ON i.profile_id = p.id
WHERE p.root_domain = %s
  AND p.enabled = TRUE
  AND i.enabled = TRUE
ORDER BY i.created_at_utc ASC
"""
        identities: list[AuthIdentity] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (root_domain,))
                rows = cur.fetchall()
                for row in rows:
                    login_cfg = _json_load(decrypt_text(row[8]), {}) if row[8] else {}
                    custom_headers = _json_load(decrypt_text(row[9]), {}) if row[9] else {}
                    success_markers = [
                        AuthVerificationMarker(kind=str(item.get("kind", "text")), value=str(item.get("value", "")))
                        for item in _json_load(row[10], [])
                        if isinstance(item, dict)
                    ]
                    denial_markers = [
                        AuthVerificationMarker(kind=str(item.get("kind", "text")), value=str(item.get("value", "")))
                        for item in _json_load(row[11], [])
                        if isinstance(item, dict)
                    ]
                    identity = AuthIdentity(
                        id=row[0],
                        profile_id=row[1],
                        identity_label=row[2],
                        role_label=row[3] or "",
                        tenant_label=row[4] or "",
                        login_strategy=row[5] or "cookie_import",
                        username=decrypt_text(row[6]) if row[6] else "",
                        password=decrypt_text(row[7]) if row[7] else "",
                        login_url=str(login_cfg.get("login_url", "") or ""),
                        login_method=str(login_cfg.get("login_method", "POST") or "POST").upper(),
                        login_username_field=str(login_cfg.get("username_field", "username") or "username"),
                        login_password_field=str(login_cfg.get("password_field", "password") or "password"),
                        login_extra_fields=(login_cfg.get("extra_fields", {}) if isinstance(login_cfg.get("extra_fields", {}), dict) else {}),
                        custom_headers={str(k): str(v) for k, v in custom_headers.items()} if isinstance(custom_headers, dict) else {},
                        allowed_hosts=[str(v) for v in _json_load(row[12], []) if str(v).strip()],
                        success_markers=success_markers,
                        denial_markers=denial_markers,
                        authenticated_probe_url=str(login_cfg.get("authenticated_probe_url", "") or ""),
                        logout_url=str(login_cfg.get("logout_url", "") or ""),
                        replay_policy=ReplayPolicy.from_dict(_json_load(row[13], {})),
                    )
                    identity.imported_cookies = self._load_cookie_jar(identity.id)
                    identities.append(identity)
        return identities

    def _load_cookie_jar(self, identity_id: str) -> list[dict[str, Any]]:
        sql = """
SELECT cookies_json_encrypted
FROM auth0r_cookie_jars
WHERE identity_id = %s
ORDER BY created_at_utc DESC
LIMIT 1
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (identity_id,))
                row = cur.fetchone()
                if not row:
                    return []
                return _json_load(decrypt_text(row[0]), []) if row[0] else []

    def save_runtime_session(
        self,
        root_domain: str,
        identity_id: str,
        source_type: str,
        generation_number: int,
        *,
        verified: bool,
        verification_summary: dict[str, Any],
        cookies: list[dict[str, Any]],
        auth_headers: dict[str, Any],
    ) -> str:
        session_id = str(uuid.uuid4())
        sql = """
INSERT INTO auth0r_runtime_sessions
(id, root_domain, identity_id, source_type, generation_number, verified, verification_summary_json, cookies_encrypted, auth_headers_encrypted)
VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        session_id,
                        root_domain,
                        identity_id,
                        source_type,
                        int(generation_number),
                        bool(verified),
                        json.dumps(verification_summary or {}),
                        encrypt_text(json.dumps(cookies or [])),
                        encrypt_text(json.dumps(auth_headers or {})),
                    ),
                )
            conn.commit()
        return session_id

    def save_recorded_action(self, root_domain: str, identity_id: str, runtime_session_id: str, action: dict[str, Any]) -> str:
        action_id = str(uuid.uuid4())
        sql = """
INSERT INTO auth0r_recorded_actions
(id, root_domain, identity_id, runtime_session_id, url, method, source, content_type, likely_state_changing, metadata_json)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        action_id,
                        root_domain,
                        identity_id,
                        runtime_session_id,
                        str(action.get("url", "")),
                        str(action.get("method", "GET")).upper(),
                        str(action.get("source", "nightmare_seed")),
                        str(action.get("content_type", "") or ""),
                        bool(action.get("likely_state_changing")),
                        json.dumps(action.get("metadata", {}) or {}),
                    ),
                )
            conn.commit()
        return action_id

    def save_replay_attempt(
        self,
        root_domain: str,
        identity_id: str,
        recorded_action_id: str,
        replay_variant: str,
        *,
        status_code: int | None,
        suspicious: bool,
        summary: dict[str, Any],
    ) -> str:
        replay_id = str(uuid.uuid4())
        sql = """
INSERT INTO auth0r_replay_attempts
(id, root_domain, identity_id, recorded_action_id, replay_variant, status_code, suspicious, summary_json)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        replay_id,
                        root_domain,
                        identity_id,
                        recorded_action_id,
                        replay_variant,
                        status_code,
                        bool(suspicious),
                        json.dumps(summary or {}),
                    ),
                )
            conn.commit()
        return replay_id

    def save_finding(
        self,
        root_domain: str,
        identity_id: str | None,
        finding_type: str,
        severity: str,
        title: str,
        endpoint: str,
        replay_variant: str,
        confidence: float,
        summary: dict[str, Any],
    ) -> str:
        finding_id = str(uuid.uuid4())
        sql = """
INSERT INTO auth0r_findings
(id, root_domain, identity_id, finding_type, severity, title, endpoint, replay_variant, confidence, summary_json)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        finding_id,
                        root_domain,
                        identity_id,
                        finding_type,
                        severity,
                        title,
                        endpoint,
                        replay_variant,
                        float(confidence),
                        json.dumps(summary or {}),
                    ),
                )
            conn.commit()
        return finding_id

    def save_evidence(self, root_domain: str, identity_id: str | None, category: str, ref_id: str, payload: dict[str, Any]) -> str:
        evidence_id = str(uuid.uuid4())
        sql = """
INSERT INTO auth0r_evidence
(id, root_domain, identity_id, category, ref_id, payload_encrypted)
VALUES (%s, %s, %s, %s, %s, %s)
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        evidence_id,
                        root_domain,
                        identity_id,
                        category,
                        ref_id,
                        encrypt_text(json.dumps(payload or {})),
                    ),
                )
            conn.commit()
        return evidence_id
