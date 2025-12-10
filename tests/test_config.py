"""Unit tests for config module."""

from __future__ import annotations

import os
import sys
from unittest.mock import patch

import pytest

from hass_migrate.config import ConfigError, DBConfig


class TestDBConfig:
    """Test cases for DBConfig class."""

    def test_init_with_valid_env(self, mock_env_vars):
        """Test DBConfig initialization with valid environment variables."""
        config = DBConfig()
        assert config.mysql_host == "localhost"
        assert config.mysql_port == 3306
        assert config.mysql_user == "test_user"
        assert config.mysql_password == "test_password"
        assert config.mysql_db == "test_db"
        assert config.pg_host == "localhost"
        assert config.pg_port == 5432
        assert config.pg_user == "test_user"
        assert config.pg_password == "test_password"
        assert config.pg_db == "test_db"
        assert config.pg_schema == "hass"

    def test_init_with_missing_mysql_host(self, mock_empty_env):
        """Test DBConfig initialization with missing MYSQL_HOST."""
        with pytest.raises(ConfigError, match="Missing required environment variable: MYSQL_HOST"):
            DBConfig()

    def test_init_with_missing_pg_password(self, mock_env_vars):
        """Test DBConfig initialization with missing PG_PASSWORD."""
        del os.environ["PG_PASSWORD"]
        with pytest.raises(ConfigError, match="Missing required environment variable: PG_PASSWORD"):
            DBConfig()

    def test_pg_schema_default(self, mock_env_vars):
        """Test that pg_schema defaults to 'hass' when not set."""
        del os.environ["PG_SCHEMA"]
        config = DBConfig()
        assert config.pg_schema == "hass"

    def test_pg_schema_custom(self, mock_env_vars):
        """Test that pg_schema uses custom value when set."""
        os.environ["PG_SCHEMA"] = "custom_schema"
        config = DBConfig()
        assert config.pg_schema == "custom_schema"

    def test_validate_port_valid(self, mock_env_vars):
        """Test port validation with valid ports."""
        os.environ["MYSQL_PORT"] = "3306"
        os.environ["PG_PORT"] = "5432"
        config = DBConfig()
        assert config.mysql_port == 3306
        assert config.pg_port == 5432

    def test_validate_port_default(self, mock_env_vars):
        """Test port validation uses defaults when not set."""
        del os.environ["MYSQL_PORT"]
        del os.environ["PG_PORT"]
        config = DBConfig()
        assert config.mysql_port == 3306
        assert config.pg_port == 5432

    def test_validate_port_invalid_too_high(self, mock_env_vars):
        """Test port validation with port number too high."""
        os.environ["MYSQL_PORT"] = "70000"
        with pytest.raises(ConfigError, match="is not a valid port number"):
            DBConfig()

    def test_validate_port_invalid_too_low(self, mock_env_vars):
        """Test port validation with port number too low."""
        os.environ["PG_PORT"] = "0"
        with pytest.raises(ConfigError, match="is not a valid port number"):
            DBConfig()

    def test_validate_port_invalid_non_integer(self, mock_env_vars):
        """Test port validation with non-integer value."""
        os.environ["MYSQL_PORT"] = "not_a_number"
        with pytest.raises(ConfigError, match="is not a valid integer"):
            DBConfig()
