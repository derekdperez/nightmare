"""Reusable system-level plugin actions.

This package provides small, composable helpers for common operations that
plugins should share instead of re-implementing ad hoc logic.
"""

from plugins.system.command_actions import run_command
from plugins.system.file_actions import append_text, delete_path, ensure_directory, read_text, write_text
from plugins.system.legacy_actions import LEGACY_ACTIONS
from plugins.system.variable_actions import (
    coerce_bool,
    coerce_int,
    format_template,
    get_nested,
    merge_dicts,
    set_nested,
)

__all__ = [
    "append_text",
    "coerce_bool",
    "coerce_int",
    "delete_path",
    "ensure_directory",
    "format_template",
    "get_nested",
    "LEGACY_ACTIONS",
    "merge_dicts",
    "read_text",
    "run_command",
    "set_nested",
    "write_text",
]
