#!/usr/bin/env python3
"""Structured logging setup for the Nightmare project."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Optional

import structlog


def configure_logging(level: Optional[str] = None) -> None:
    resolved_level = str(level or os.getenv("NIGHTMARE_LOG_LEVEL", "INFO")).upper()
    resolved_level_value = getattr(logging, resolved_level, logging.INFO)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=resolved_level_value,
    )
    # Prevent duplicated one-line HTTP client chatter; structured request/response
    # logs are emitted explicitly from our http client wrapper.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
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


def get_logger(name: str, **bound: Any) -> structlog.stdlib.BoundLogger:
    logger = structlog.get_logger(name)
    if bound:
        logger = logger.bind(**bound)
    return logger
