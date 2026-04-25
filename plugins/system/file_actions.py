"""Basic filesystem actions for plugin steps."""

from __future__ import annotations

from pathlib import Path


def ensure_directory(path: str | Path) -> Path:
    """Create a directory (including parents) if it does not exist."""
    resolved = Path(path).expanduser().resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def read_text(path: str | Path, *, encoding: str = "utf-8") -> str:
    """Read text content from a file path."""
    return Path(path).expanduser().resolve().read_text(encoding=encoding)


def write_text(path: str | Path, content: str, *, encoding: str = "utf-8") -> Path:
    """Write text content to a file, creating parent directories as needed."""
    resolved = Path(path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(str(content), encoding=encoding)
    return resolved


def append_text(path: str | Path, content: str, *, encoding: str = "utf-8") -> Path:
    """Append text content to a file, creating parent directories as needed."""
    resolved = Path(path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with resolved.open("a", encoding=encoding) as handle:
        handle.write(str(content))
    return resolved


def delete_path(path: str | Path, *, missing_ok: bool = True) -> bool:
    """Delete a file or empty directory path."""
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        return bool(missing_ok)
    if resolved.is_dir():
        resolved.rmdir()
        return True
    resolved.unlink()
    return True
