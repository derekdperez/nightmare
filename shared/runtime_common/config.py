#!/usr/bin/env python3
"""Shared configuration helpers and strongly-typed app settings."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Mapping, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


def read_json_dict(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
    except Exception:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def read_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    out: dict[str, str] = {}
    try:
        raw_text = path.read_text(encoding="utf-8-sig")
    except Exception:
        return {}
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def load_env_file_into_os(path: Path, *, override: bool = False) -> dict[str, str]:
    values = read_env_file(path)
    for key, value in values.items():
        if override or key not in os.environ:
            os.environ[key] = value
    return values


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def merged_value(cli_value: Any, cfg: Mapping[str, Any], key: str, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if key in cfg:
        value = cfg[key]
        if value is not None and value != "":
            return value
    return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def resolve_config_path(base_dir: Path, raw_path: Optional[str], default_name: str) -> Path:
    value = str(raw_path or "").strip() or default_name
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    if path.parts and path.parts[0].lower() == "config":
        return (base_dir / path).resolve()
    return (base_dir / "config" / path).resolve()


def normalize_server_base_url(raw: str) -> str:
    s = str(raw or "").strip().rstrip("/")
    if not s:
        return ""
    if "://" not in s:
        s = f"https://{s}"
    return s


class ServerSettings(BaseModel):
    host: str = "127.0.0.1"
    output_root: Path = Path("output")
    http_port: int = 80
    https_port: int = 443
    cert_file: Optional[Path] = None
    key_file: Optional[Path] = None
    database_url: str = ""
    coordinator_api_token: str = ""
    master_report_regen_token: str = ""

    @field_validator("host", mode="before")
    @classmethod
    def _normalize_host(cls, value: Any) -> str:
        text = str(value or "").strip()
        return text or "127.0.0.1"

    @field_validator("database_url", "coordinator_api_token", "master_report_regen_token", mode="before")
    @classmethod
    def _normalize_str(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("http_port", "https_port", mode="before")
    @classmethod
    def _normalize_port(cls, value: Any) -> int:
        port = safe_int(value, 0)
        if port and not (1 <= port <= 65535):
            raise ValueError("port must be in range 1..65535")
        return port


class CoordinatorSettings(BaseModel):
    server_base_url: str
    api_token: str = ""
    insecure_tls: bool = False
    output_root: Path = Path("output")
    heartbeat_interval_seconds: float = 20.0
    lease_seconds: int = 180
    poll_interval_seconds: float = 5.0
    nightmare_workers: int = 2
    fozzy_workers: int = 2
    extractor_workers: int = 2
    auth0r_workers: int = 2
    python_executable: str = "python3"
    nightmare_config: Path = Path("config/nightmare.json")
    fozzy_config: Path = Path("config/fozzy.json")
    extractor_config: Path = Path("config/extractor.json")
    auth0r_config: Path = Path("config/auth0r.json")
    upload_session_every_seconds: float = 15.0
    enable_nightmare: bool = True
    enable_fozzy: bool = True
    enable_extractor: bool = True
    enable_auth0r: bool = True
    fozzy_process_workers: int = 1
    extractor_process_workers: int = 1
    workflow_config: Path = Path("workflows")
    workflow_scheduler_enabled: bool = True
    workflow_scheduler_interval_seconds: float = 15.0
    plugin_workers: int = 1
    plugin_allowlist: list[str] = Field(default_factory=list)

    @field_validator("server_base_url", mode="before")
    @classmethod
    def _normalize_base_url(cls, value: Any) -> str:
        normalized = normalize_server_base_url(str(value or ""))
        if not normalized:
            raise ValueError("server_base_url is required")
        return normalized

    @field_validator(
        "heartbeat_interval_seconds",
        "poll_interval_seconds",
        "upload_session_every_seconds",
        "workflow_scheduler_interval_seconds",
        mode="before",
    )
    @classmethod
    def _normalize_floatish(cls, value: Any) -> float:
        return safe_float(value, 0.0)

    @field_validator(
        "lease_seconds",
        "nightmare_workers",
        "fozzy_workers",
        "extractor_workers",
        "auth0r_workers",
        "fozzy_process_workers",
        "extractor_process_workers",
        "plugin_workers",
        mode="before",
    )
    @classmethod
    def _normalize_intish(cls, value: Any) -> int:
        return safe_int(value, 0)

    @field_validator("plugin_allowlist", mode="before")
    @classmethod
    def _normalize_plugin_allowlist(cls, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [
            str(item or "").strip().lower()
            for item in value
            if str(item or "").strip()
        ]

    @model_validator(mode="after")
    def _apply_minimums(self) -> "CoordinatorSettings":
        self.heartbeat_interval_seconds = max(5.0, self.heartbeat_interval_seconds)
        self.lease_seconds = max(30, self.lease_seconds)
        self.poll_interval_seconds = max(1.0, self.poll_interval_seconds)
        self.nightmare_workers = max(1, self.nightmare_workers)
        self.fozzy_workers = max(1, self.fozzy_workers)
        self.extractor_workers = max(1, self.extractor_workers)
        self.auth0r_workers = max(1, self.auth0r_workers)
        self.upload_session_every_seconds = max(5.0, self.upload_session_every_seconds)
        self.workflow_scheduler_interval_seconds = max(5.0, self.workflow_scheduler_interval_seconds)
        self.fozzy_process_workers = max(1, self.fozzy_process_workers)
        self.extractor_process_workers = max(1, self.extractor_process_workers)
        self.plugin_workers = max(1, self.plugin_workers)
        self.api_token = str(self.api_token or "").strip()
        self.python_executable = str(self.python_executable or "python3").strip() or "python3"
        return self


class ClientSettings(BaseModel):
    server_base_url: str = Field(default="")
    api_token: str = Field(default="")
    insecure_tls: bool = Field(default=False)

    @field_validator("server_base_url", mode="before")
    @classmethod
    def _normalize_client_base_url(cls, value: Any) -> str:
        return normalize_server_base_url(str(value or ""))

    @field_validator("api_token", mode="before")
    @classmethod
    def _normalize_api_token(cls, value: Any) -> str:
        return str(value or "").strip()
