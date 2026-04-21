#!/usr/bin/env python3
"""Structured logging setup for the Nightmare project."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Optional

try:
    import structlog
except Exception:  # pragma: no cover - optional runtime dependency
    structlog = None

from nightmare_shared.error_reporting import install_error_reporting


class _FallbackBoundLogger:
    def __init__(self, logger: logging.Logger, **bound: Any):
        self._logger = logger
        self._bound = dict(bound)

    def bind(self, **extra: Any) -> "_FallbackBoundLogger":
        merged = dict(self._bound)
        merged.update(extra)
        return _FallbackBoundLogger(self._logger, **merged)

    def _emit(self, level: int, event: str, **kwargs: Any) -> None:
        payload = dict(self._bound)
        payload.update(kwargs)
        if payload:
            self._logger.log(level, "%s | %s", event, payload)
        else:
            self._logger.log(level, "%s", event)

    def info(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.INFO, event, **kwargs)

    def warning(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.WARNING, event, **kwargs)

    def error(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.ERROR, event, **kwargs)

    def debug(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.DEBUG, event, **kwargs)

    def exception(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.ERROR, event, exc_info=True, **kwargs)


def configure_logging(level: Optional[str] = None) -> None:
    resolved_level = str(level or os.getenv("NIGHTMARE_LOG_LEVEL", "INFO")).upper()
    resolved_level_value = getattr(logging, resolved_level, logging.INFO)
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=resolved_level_value)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    if structlog is not None:
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(resolved_level_value),
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
    try:
        install_error_reporting(
            program_name=str(os.getenv("NIGHTMARE_PROGRAM_NAME", "python_process") or "python_process"),
            component_name=str(os.getenv("NIGHTMARE_COMPONENT_NAME", "") or ""),
            source_type=str(os.getenv("NIGHTMARE_SOURCE_TYPE", "python") or "python"),
        )
    except Exception:
        pass


def get_logger(name: str, **bound: Any):
    if structlog is not None:
        logger = structlog.get_logger(name)
        if bound:
            logger = logger.bind(**bound)
        return logger
    logger = logging.getLogger(name)
    return _FallbackBoundLogger(logger, **bound)
