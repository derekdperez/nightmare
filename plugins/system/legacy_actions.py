"""Reimplemented baseline actions inspired by ``example/workflow_plugins``."""

from __future__ import annotations

import difflib
import json
import re
from pathlib import Path
from typing import Any, Callable

import httpx

from plugins.system.file_actions import append_text, read_text, write_text
from plugins.system.variable_actions import format_template, save_variable_value as _save_variable_value


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


def append_to_file(path: str | Path, text: str) -> dict[str, Any]:
    """Append text to a file."""
    resolved = append_text(path, str(text or ""))
    return {"path": str(resolved), "appended": True}


def file_contains_line(path: str | Path, line: str) -> dict[str, Any]:
    """Return whether a file contains an exact line."""
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        return {"path": str(resolved), "contains": False}
    target = str(line or "").rstrip("\n")
    contains = any(target == row.rstrip("\n") for row in read_text(resolved).splitlines())
    return {"path": str(resolved), "contains": contains}


def list_files_in_folder(folder: str | Path, pattern: str = "*", recursive: bool = False) -> dict[str, Any]:
    """List files in a folder with optional recursion."""
    root = Path(folder).expanduser().resolve()
    if not root.is_dir():
        return {"folder": str(root), "files": []}
    iterator = root.rglob(pattern) if recursive else root.glob(pattern)
    files = [str(p.resolve()) for p in iterator if p.is_file()]
    files.sort()
    return {"folder": str(root), "files": files}


def combine_files(paths: list[str], output_path: str | Path, separator: str = "\n") -> dict[str, Any]:
    """Combine multiple files into one output file."""
    chunks: list[str] = []
    for raw in paths if isinstance(paths, list) else []:
        p = Path(str(raw or "")).expanduser().resolve()
        if p.is_file():
            chunks.append(read_text(p))
    resolved = write_text(output_path, str(separator or "\n").join(chunks))
    return {"output_path": str(resolved), "file_count": len(chunks)}


def combine_listed_files(files: list[str], output_path: str | Path, separator: str = "\n") -> dict[str, Any]:
    """Alias for combine_files using a pre-listed file array."""
    return combine_files(files, output_path, separator=separator)


def combine_all_files_in_folder(folder: str | Path, output_path: str | Path, pattern: str = "*") -> dict[str, Any]:
    """Combine all matching files in a folder."""
    listed = list_files_in_folder(folder, pattern=pattern, recursive=False)
    return combine_files(listed.get("files", []), output_path)


def extract_section_from_file(path: str | Path, start_marker: str = "", end_marker: str = "") -> dict[str, Any]:
    """Extract text between optional start/end markers."""
    text = read_text(path)
    start = 0
    end = len(text)
    if start_marker:
        idx = text.find(str(start_marker))
        if idx >= 0:
            start = idx + len(str(start_marker))
    if end_marker:
        idx = text.find(str(end_marker), start)
        if idx >= 0:
            end = idx
    section = text[start:end]
    return {"section": section, "length": len(section)}


def file_to_context(path: str | Path, key: str = "file_content") -> dict[str, Any]:
    """Load file content into a context key."""
    return {"context": {str(key or "file_content"): read_text(path)}}


def load_agent_context(path: str | Path) -> dict[str, Any]:
    """Load JSON context for agent/system prompts."""
    payload = json.loads(read_text(path))
    return {"context": payload if isinstance(payload, dict) else {"value": payload}}


def build_agent_system_prompt(template: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """Render a system prompt from template + variables."""
    return {"system_prompt": format_template(template, variables or {})}


def save_variable_value(variables: dict[str, Any], key: str, value: Any) -> dict[str, Any]:
    """Store one variable value and return updated map."""
    return {"variables": _save_variable_value(variables, key, value)}


def get_next_item_in_list(items: list[Any], index: int = 0) -> dict[str, Any]:
    """Return item at index and next index pointer."""
    values = items if isinstance(items, list) else []
    idx = max(0, int(index or 0))
    if idx >= len(values):
        return {"has_next": False, "item": None, "next_index": idx}
    return {"has_next": True, "item": values[idx], "next_index": idx + 1}


def slice_items(items: list[Any], start: int = 0, stop: int | None = None, step: int = 1) -> dict[str, Any]:
    """Slice a list and return sliced items."""
    values = items if isinstance(items, list) else []
    s = int(start or 0)
    e = int(stop) if stop is not None else None
    st = max(1, int(step or 1))
    return {"items": values[s:e:st]}


def extract_emails_from_value(value: Any) -> dict[str, Any]:
    """Extract email-like tokens from text."""
    text = str(value or "")
    emails = sorted(set(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)))
    return {"emails": emails, "count": len(emails)}


def unified_text_diff(before: str, after: str, from_label: str = "before", to_label: str = "after") -> dict[str, Any]:
    """Return a unified text diff."""
    diff = difflib.unified_diff(
        str(before or "").splitlines(),
        str(after or "").splitlines(),
        fromfile=str(from_label),
        tofile=str(to_label),
        lineterm="",
    )
    return {"diff": "\n".join(diff)}


def skip_remaining_steps_if(condition: Any) -> dict[str, Any]:
    """Signal workflow engine to skip remaining steps when condition is truthy."""
    return {"skip_remaining_steps": bool(condition)}


def skip_remaining_steps_unless(condition: Any) -> dict[str, Any]:
    """Signal workflow engine to skip remaining steps when condition is falsy."""
    return {"skip_remaining_steps": not bool(condition)}


def send_input_to_API(url: str, payload: dict[str, Any] | None = None, method: str = "POST") -> dict[str, Any]:
    """Send JSON payload to an HTTP endpoint."""
    response = httpx.request(str(method or "POST").upper(), str(url), json=(payload or {}), timeout=30.0)
    return {
        "ok": response.status_code < 400,
        "status_code": int(response.status_code),
        "response_text": response.text,
    }


def generate_list_from_API(url: str, method: str = "GET") -> dict[str, Any]:
    """Fetch list payload from an HTTP endpoint."""
    response = httpx.request(str(method or "GET").upper(), str(url), timeout=30.0)
    data: Any
    try:
        data = response.json()
    except Exception:
        data = []
    items = data if isinstance(data, list) else (data.get("items", []) if isinstance(data, dict) else [])
    return {"items": items if isinstance(items, list) else []}


def get_all_messages(messages: list[Any] | None = None) -> dict[str, Any]:
    """Return messages passed in-memory (placeholder action)."""
    out = messages if isinstance(messages, list) else []
    return {"messages": out, "count": len(out)}


def get_all_messages_from_client(messages: list[Any] | None = None) -> dict[str, Any]:
    """Alias for message list retrieval action."""
    return get_all_messages(messages)


def send_file_to_AI(path: str | Path) -> dict[str, Any]:
    """Return file payload for downstream AI stage."""
    resolved = Path(path).expanduser().resolve()
    return {"path": str(resolved), "content": read_text(resolved)}


def send_all_files_in_folder_to_AI(folder: str | Path, pattern: str = "*") -> dict[str, Any]:
    """Return all file contents in a folder for downstream AI stage."""
    listed = list_files_in_folder(folder, pattern=pattern, recursive=False).get("files", [])
    files = [{"path": item, "content": read_text(item)} for item in listed]
    return {"files": files, "count": len(files)}


def ai_chat(prompt: str, model: str = "", system_prompt: str = "") -> dict[str, Any]:
    """Placeholder AI chat action returning deterministic payload."""
    return {
        "model": str(model or ""),
        "system_prompt": str(system_prompt or ""),
        "prompt": str(prompt or ""),
        "response": "",
    }


def send_email(to: str, subject: str, body: str) -> dict[str, Any]:
    """Placeholder email action; returns envelope for external sender."""
    return {"queued": True, "to": str(to or ""), "subject": str(subject or ""), "body": str(body or "")}


def send_email_list(recipients: list[str], subject: str, body: str) -> dict[str, Any]:
    """Placeholder bulk-email action."""
    sent = [str(item or "") for item in (recipients if isinstance(recipients, list) else []) if str(item or "")]
    return {"queued": True, "recipients": sent, "count": len(sent), "subject": str(subject or ""), "body": str(body or "")}


def send_fetlife_message(handle: str, subject: str, body: str) -> dict[str, Any]:
    """Placeholder action for external FetLife messaging integration."""
    return {"queued": True, "handle": str(handle or ""), "subject": str(subject or ""), "body": str(body or "")}


def select_business_research_target(candidates: list[dict[str, Any]] | list[str]) -> dict[str, Any]:
    """Select first candidate target from a list."""
    values = candidates if isinstance(candidates, list) else []
    target = values[0] if values else None
    return {"target": target, "selected": target is not None}


LEGACY_ACTIONS: dict[str, Callable[..., dict[str, Any]]] = {
    "ai_chat": ai_chat,
    "append_to_file": append_to_file,
    "output": output,
    "path_exists": path_exists,
    "append_unique_lines": append_unique_lines,
    "build_agent_system_prompt": build_agent_system_prompt,
    "combine_all_files_in_folder": combine_all_files_in_folder,
    "combine_files": combine_files,
    "combine_listed_files": combine_listed_files,
    "extract_emails_from_value": extract_emails_from_value,
    "extract_section_from_file": extract_section_from_file,
    "file_contains_line": file_contains_line,
    "file_to_context": file_to_context,
    "generate_list_from_API": generate_list_from_API,
    "get_all_messages": get_all_messages,
    "get_all_messages_from_client": get_all_messages_from_client,
    "get_next_item_in_list": get_next_item_in_list,
    "list_files_in_folder": list_files_in_folder,
    "load_agent_context": load_agent_context,
    "replace_variables_in_file": replace_variables_in_file,
    "regex_match": regex_match,
    "count_items": count_items,
    "any_true": any_true,
    "compare_number": compare_number,
    "save_variable_value": save_variable_value,
    "select_business_research_target": select_business_research_target,
    "send_all_files_in_folder_to_AI": send_all_files_in_folder_to_AI,
    "send_email": send_email,
    "send_email_list": send_email_list,
    "send_fetlife_message": send_fetlife_message,
    "send_file_to_AI": send_file_to_AI,
    "send_input_to_API": send_input_to_API,
    "skip_remaining_steps_if": skip_remaining_steps_if,
    "skip_remaining_steps_unless": skip_remaining_steps_unless,
    "slice_items": slice_items,
    "unified_text_diff": unified_text_diff,
}
