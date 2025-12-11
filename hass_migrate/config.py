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
        # MySQL Configuration
        self.mysql_host = self._require_env("MYSQL_HOST")
        self.mysql_port = self._validate_port("MYSQL_PORT", 3306)
        self.mysql_user = self._require_env("MYSQL_USER")
        self.mysql_password = self._require_env("MYSQL_PASSWORD")
        self.mysql_db = self._require_env("MYSQL_DB")
        self.mysql_pool_minsize = int(os.getenv("MYSQL_POOL_MINSIZE", "1"))
        self.mysql_pool_maxsize = int(os.getenv("MYSQL_POOL_MAXSIZE", "10"))
        self.mysql_pool_timeout = float(os.getenv("MYSQL_POOL_TIMEOUT", "30.0"))

        # PostgreSQL Configuration
        self.pg_host = self._require_env("PG_HOST")
        self.pg_port = self._validate_port("PG_PORT", 5432)
        self.pg_user = self._require_env("PG_USER")
        self.pg_password = self._require_env("PG_PASSWORD")
        self.pg_db = self._require_env("PG_DB")
        self.pg_schema = os.getenv(
            "PG_SCHEMA", "public"
        )  # Default to 'public' for Home Assistant compatibility
        self.pg_pool_minsize = int(os.getenv("PG_POOL_MINSIZE", "1"))
        self.pg_pool_maxsize = int(os.getenv("PG_POOL_MAXSIZE", "10"))
        self.pg_pool_timeout = float(os.getenv("PG_POOL_TIMEOUT", "30.0"))

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
