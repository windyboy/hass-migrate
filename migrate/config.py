from __future__ import annotations

import os
import sys
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


class ConfigError(Exception):
    """Configuration validation error."""

    pass


class DBConfig:
    """Database configuration with validation."""

    def __init__(self):
        try:
            # MySQL Configuration
            self.mysql_host = self._require_env("MYSQL_HOST")
            self.mysql_port = self._validate_port("MYSQL_PORT", 3306)
            self.mysql_user = self._require_env("MYSQL_USER")
            self.mysql_password = self._require_env("MYSQL_PASSWORD")
            self.mysql_db = self._require_env("MYSQL_DB")

            # PostgreSQL Configuration
            self.pg_host = self._require_env("PG_HOST")
            self.pg_port = self._validate_port("PG_PORT", 5432)
            self.pg_user = self._require_env("PG_USER")
            self.pg_password = self._require_env("PG_PASSWORD")
            self.pg_db = self._require_env("PG_DB")
            self.pg_schema = os.getenv(
                "PG_SCHEMA", "hass"
            )  # Default to 'hass' for backward compatibility
        except ConfigError as e:
            sys.stderr.write(f"❌ Configuration Error: {e}\n")
            sys.stderr.write("\nPlease ensure your .env file is properly configured.\n")
            sys.stderr.write("You can copy .env.example as a template:\n\n")
            sys.stderr.write("  cp .env.example .env\n")
            sys.stderr.write("  # Then edit .env with your database credentials\n\n")
            sys.exit(1)

    def _require_env(self, key: str) -> str:
        """Get required environment variable or raise ConfigError."""
        value = os.getenv(key)
        if not value:
            raise ConfigError(f"Missing required environment variable: {key}")
        return value

    def _validate_port(self, key: str, default: int) -> int:
        """Validate port number is in valid range (1-65535)."""
        value = os.getenv(key, str(default))
        try:
            port = int(value)
            if not (1 <= port <= 65535):
                raise ConfigError(
                    f"{key}={port} is not a valid port number (must be 1-65535)"
                )
            return port
        except ValueError:
            raise ConfigError(f"{key}={value} is not a valid integer")
