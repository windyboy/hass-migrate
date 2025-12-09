"""Structured logging utilities."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict


class StructuredLogger:
    """Structured logger for migration events."""

    def __init__(self, name: str, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Remove existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Create console handler with JSON-like formatting
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def log_migration_event(
        self,
        event: str,
        table: str,
        rows_migrated: int = 0,
        duration: float = 0.0,
        **kwargs: Any,
    ) -> None:
        """Log a migration event with structured data."""
        log_data = {
            "event": event,
            "table": table,
            "rows_migrated": rows_migrated,
            "duration_seconds": round(duration, 2),
            **kwargs,
        }
        self.logger.info(json.dumps(log_data))

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        if kwargs:
            self.logger.info(f"{message} {json.dumps(kwargs)}")
        else:
            self.logger.info(message)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        if kwargs:
            self.logger.warning(f"{message} {json.dumps(kwargs)}")
        else:
            self.logger.warning(message)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        if kwargs:
            self.logger.error(f"{message} {json.dumps(kwargs)}")
        else:
            self.logger.error(message)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        if kwargs:
            self.logger.debug(f"{message} {json.dumps(kwargs)}")
        else:
            self.logger.debug(message)


class SafeLogger:
    """Logger that automatically sanitizes sensitive information."""

    SENSITIVE_KEYS = {"password", "pwd", "secret", "token", "api_key", "apikey"}

    @staticmethod
    def sanitize(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize sensitive information from log data.

        Args:
            data: Dictionary to sanitize

        Returns:
            Sanitized dictionary with sensitive values redacted
        """
        sanitized: Dict[str, Any] = {}
        for key, value in data.items():
            if any(sensitive in key.lower() for sensitive in SafeLogger.SENSITIVE_KEYS):
                sanitized[key] = "***REDACTED***"
            elif isinstance(value, dict):
                sanitized[key] = SafeLogger.sanitize(value)
            else:
                sanitized[key] = value
        return sanitized

