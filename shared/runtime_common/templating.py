#!/usr/bin/env python3
"""Jinja2 template helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE_DIR = Path(__file__).resolve().parent.parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"

_env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(enabled_extensions=("html", "xml", "j2"), default=True),
    trim_blocks=False,
    lstrip_blocks=False,
)

def render_template(name: str, **context: Any) -> str:
    return _env.get_template(name).render(**context)
