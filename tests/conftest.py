"""Pytest configuration and fixtures."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_env_vars():
    """Fixture to provide mock environment variables for testing."""
    env_vars = {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DB": "test_db",
        "PG_HOST": "localhost",
        "PG_PORT": "5432",
        "PG_USER": "test_user",
        "PG_PASSWORD": "test_password",
        "PG_DB": "test_db",
        "PG_SCHEMA": "hass",
    }
    with patch.dict(os.environ, env_vars, clear=False):
        yield env_vars


@pytest.fixture
def mock_empty_env():
    """Fixture to provide empty environment variables for testing."""
    with patch.dict(os.environ, {}, clear=True):
        yield

