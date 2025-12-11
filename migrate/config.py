from __future__ import annotations

import os
import sys
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ConfigError(Exception):
    """Configuration validation error."""

    pass


class DBConfig:
    """Database configuration with validation and multi-environment support."""

    def __init__(self):
        try:
            # Environment
            self.environment = os.getenv("ENVIRONMENT", "development")

            # MySQL Configuration
            self.mysql_host = self._require_env("MYSQL_HOST")
            self.mysql_port = self._validate_port("MYSQL_PORT", 3306)
            self.mysql_user = self._require_env("MYSQL_USER")
            self.mysql_password = self._require_env("MYSQL_PASSWORD")
            self.mysql_db = self._require_env("MYSQL_DB")

            # MySQL Connection Pool Configuration
            self.mysql_pool_minsize = self._validate_positive_int(
                "MYSQL_POOL_MINSIZE", 1
            )
            self.mysql_pool_maxsize = self._validate_positive_int(
                "MYSQL_POOL_MAXSIZE", 10
            )
            self.mysql_pool_timeout = self._validate_positive_int(
                "MYSQL_POOL_TIMEOUT", 30
            )

            # PostgreSQL Configuration
            self.pg_host = self._require_env("PG_HOST")
            self.pg_port = self._validate_port("PG_PORT", 5432)
            self.pg_user = self._require_env("PG_USER")
            self.pg_password = self._require_env("PG_PASSWORD")
            self.pg_db = self._require_env("PG_DB")
            self.pg_schema = os.getenv("PG_SCHEMA", "hass")

            # PostgreSQL Connection Pool Configuration
            self.pg_pool_minsize = self._validate_positive_int("PG_POOL_MINSIZE", 2)
            self.pg_pool_maxsize = self._validate_positive_int("PG_POOL_MAXSIZE", 10)

            # Migration Configuration
            self.batch_size = self._validate_positive_int("BATCH_SIZE", 10000)
            self.max_chunk_size = self._validate_positive_int("MAX_CHUNK_SIZE", 5000)
            self.progress_update_interval = self._validate_positive_int(
                "PROGRESS_UPDATE_INTERVAL", 10
            )
            self.use_copy = self._validate_bool("USE_COPY", True)

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

    def _parse_int(self, value: str, min_val: int, max_val: int, error_msg: str) -> int:
        """Parse and validate an integer value within range."""
        try:
            num = int(value)
            if not (min_val <= num <= max_val):
                raise ConfigError(error_msg)
            return num
        except ValueError:
            raise ConfigError(f"{value} is not a valid integer")

    def _parse_bool(self, value: str) -> bool:
        """Parse a boolean value from string."""
        if value.lower() in ("true", "1", "yes", "y"):
            return True
        elif value.lower() in ("false", "0", "no", "n"):
            return False
        else:
            raise ConfigError(f"{value} is not a valid boolean value")

    def _validate_port(self, key: str, default: int) -> int:
        """Validate port number is in valid range (1-65535)."""
        value = os.getenv(key, str(default))
        return self._parse_int(
            value,
            1,
            65535,
            f"{key}={value} is not a valid port number (must be 1-65535)",
        )

    def _validate_positive_int(self, key: str, default: int) -> int:
        """Validate that a value is a positive integer."""
        value = os.getenv(key, str(default))
        return self._parse_int(
            value, 1, sys.maxsize, f"{key}={value} must be a positive integer"
        )

    def _validate_bool(self, key: str, default: bool) -> bool:
        """Validate that a value is a boolean."""
        value = os.getenv(key, str(default))
        return self._parse_bool(value)
