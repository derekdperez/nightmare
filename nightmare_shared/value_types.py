#!/usr/bin/env python3
"""Shared value/type inference helpers."""

from __future__ import annotations

import re


def infer_observed_value_type(value: str) -> str:
    text = (value or "").strip()
    if text == "":
        return "empty"

    lowered = text.lower()
    if lowered in {"true", "false"}:
        return "bool"

    if re.fullmatch(r"[+-]?\d+", text):
        return "int"

    if re.fullmatch(r"[+-]?\d+\.\d+", text):
        return "float"

    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}", text):
        return "uuid"

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return "date"

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T[^ ]+", text):
        return "datetime"

    if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", text):
        return "email"

    if re.fullmatch(r"https?://\S+", text, flags=re.IGNORECASE):
        return "url"

    if re.fullmatch(r"[0-9a-fA-F]+", text) and len(text) % 2 == 0:
        return "hex"

    if re.fullmatch(r"[A-Za-z0-9_-]{8,}={0,2}", text):
        return "token"

    return "string"

