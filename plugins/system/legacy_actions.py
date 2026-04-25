"""Reimplemented baseline actions inspired by ``example/workflow_plugins``."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable

from plugins.system.file_actions import append_text, read_text, write_text
from plugins.system.variable_actions import format_template


def output(value: Any) -> dict[str, Any]:
    """Return a normalized output payload."""
    return {"value": value}


def path_exists(path: str | Path) -> dict[str, Any]:
    """Return existence metadata for a path."""
    resolved = Path(path).expanduser().resolve()
    return {
        "exists": resolved.exists(),
        "is_file": resolved.is_file(),
        "is_dir": resolved.is_dir(),
        "path": str(resolved),
    }


def append_unique_lines(path: str | Path, lines: list[str]) -> dict[str, Any]:
    """Append only new lines to a text file while preserving order."""
    resolved = Path(path).expanduser().resolve()
    existing = set()
    if resolved.is_file():
        existing = {line.rstrip("\n") for line in read_text(resolved).splitlines()}
    appended = 0
    for raw in lines if isinstance(lines, list) else []:
        line = str(raw or "")
        if line in existing:
            continue
        append_text(resolved, line + "\n")
        existing.add(line)
        appended += 1
    return {"path": str(resolved), "appended_count": appended}


def replace_variables_in_file(path: str | Path, variables: dict[str, Any]) -> dict[str, Any]:
    """Replace ``{{var}}`` placeholders in a file and persist the result."""
    resolved = Path(path).expanduser().resolve()
    original = read_text(resolved)
    rendered = format_template(original, variables if isinstance(variables, dict) else {})
    write_text(resolved, rendered)
    return {"path": str(resolved), "changed": original != rendered}


def regex_match(value: Any, pattern: str) -> dict[str, Any]:
    """Run a regex search and return matched groups."""
    text = str(value or "")
    compiled = re.compile(str(pattern or ""))
    match = compiled.search(text)
    return {
        "matched": bool(match),
        "match": (match.group(0) if match else ""),
        "groups": (list(match.groups()) if match else []),
    }


def count_items(items: list[Any]) -> dict[str, Any]:
    """Count the number of items in a list."""
    return {"count": len(items if isinstance(items, list) else [])}


def any_true(items: list[Any]) -> dict[str, Any]:
    """Return whether any list value is truthy."""
    return {"result": any(bool(v) for v in (items if isinstance(items, list) else []))}


def compare_number(left: Any, right: Any, operator: str) -> dict[str, Any]:
    """Compare two numeric values with a named operator."""
    l = float(left)
    r = float(right)
    op = str(operator or "eq").strip().lower()
    if op in {"eq", "=="}:
        result = l == r
    elif op in {"ne", "!="}:
        result = l != r
    elif op in {"gt", ">"}:
        result = l > r
    elif op in {"gte", ">="}:
        result = l >= r
    elif op in {"lt", "<"}:
        result = l < r
    elif op in {"lte", "<="}:
        result = l <= r
    else:
        raise ValueError(f"unsupported operator: {operator}")
    return {"result": bool(result), "left": l, "right": r, "operator": op}


LEGACY_ACTIONS: dict[str, Callable[..., dict[str, Any]]] = {
    "output": output,
    "path_exists": path_exists,
    "append_unique_lines": append_unique_lines,
    "replace_variables_in_file": replace_variables_in_file,
    "regex_match": regex_match,
    "count_items": count_items,
    "any_true": any_true,
    "compare_number": compare_number,
}
