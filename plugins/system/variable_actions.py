"""Variable and template helper actions for system workflows."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def merge_dicts(*values: dict[str, Any]) -> dict[str, Any]:
    """Merge dictionaries left-to-right and return a new object."""
    out: dict[str, Any] = {}
    for value in values:
        if isinstance(value, dict):
            out.update(value)
    return out


def coerce_bool(value: Any, default: bool = False) -> bool:
    """Coerce common truthy/falsy values into a boolean."""
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "y"}:
        return True
    if text in {"0", "false", "no", "off", "n"}:
        return False
    return bool(default)


def coerce_int(value: Any, default: int = 0) -> int:
    """Coerce a value to int with a default fallback."""
    try:
        return int(value)
    except Exception:
        return int(default)


def get_nested(data: dict[str, Any], path: str, default: Any = None) -> Any:
    """Read dotted-path value from a nested dictionary."""
    if not isinstance(data, dict):
        return default
    parts = [p for p in str(path or "").split(".") if p]
    current: Any = data
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def set_nested(data: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    """Set dotted-path value in a nested dictionary and return the object."""
    if not isinstance(data, dict):
        raise ValueError("data must be a dict")
    parts = [p for p in str(path or "").split(".") if p]
    if not parts:
        raise ValueError("path is required")
    current = data
    for part in parts[:-1]:
        child = current.get(part)
        if not isinstance(child, dict):
            child = {}
            current[part] = child
        current = child
    current[parts[-1]] = value
    return data


def format_template(template: str, variables: dict[str, Any] | None = None) -> str:
    """Expand ``{{name}}`` placeholders with variable values."""
    out = str(template or "")
    context = variables if isinstance(variables, dict) else {}
    for key, raw in context.items():
        token = "{{" + str(key) + "}}"
        out = out.replace(token, str(raw if raw is not None else ""))
    return out


def save_variable_value(variables: dict[str, Any], key: str, value: Any) -> dict[str, Any]:
    """Return a copied variable map with ``key`` assigned to ``value``."""
    out = deepcopy(variables) if isinstance(variables, dict) else {}
    out[str(key or "").strip()] = value
    return out
