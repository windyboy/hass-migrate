"""Unit tests for exceptions module."""

from __future__ import annotations

import pytest

from migrate.exceptions import (
    BackupError,
    DatabaseConnectionError,
    DataValidationError,
    DependencyError,
    MigrationError,
    SchemaError,
)


class TestExceptions:
    """Test cases for custom exceptions."""

    def test_migration_error_base(self):
        """Test that MigrationError is the base exception."""
        assert issubclass(DatabaseConnectionError, MigrationError)
        assert issubclass(SchemaError, MigrationError)
        assert issubclass(DataValidationError, MigrationError)
        assert issubclass(BackupError, MigrationError)
        assert issubclass(DependencyError, MigrationError)

    def test_migration_error_raise(self):
        """Test raising MigrationError."""
        with pytest.raises(MigrationError):
            raise MigrationError("Test error")

    def test_database_connection_error(self):
        """Test DatabaseConnectionError."""
        with pytest.raises(DatabaseConnectionError) as exc_info:
            raise DatabaseConnectionError("Connection failed")
        assert str(exc_info.value) == "Connection failed"
        assert isinstance(exc_info.value, MigrationError)

    def test_schema_error(self):
        """Test SchemaError."""
        with pytest.raises(SchemaError) as exc_info:
            raise SchemaError("Schema mismatch")
        assert str(exc_info.value) == "Schema mismatch"
        assert isinstance(exc_info.value, MigrationError)

    def test_data_validation_error(self):
        """Test DataValidationError."""
        with pytest.raises(DataValidationError) as exc_info:
            raise DataValidationError("Validation failed")
        assert str(exc_info.value) == "Validation failed"
        assert isinstance(exc_info.value, MigrationError)

    def test_backup_error(self):
        """Test BackupError."""
        with pytest.raises(BackupError) as exc_info:
            raise BackupError("Backup failed")
        assert str(exc_info.value) == "Backup failed"
        assert isinstance(exc_info.value, MigrationError)

    def test_dependency_error(self):
        """Test DependencyError."""
        with pytest.raises(DependencyError) as exc_info:
            raise DependencyError("Dependency resolution failed")
        assert str(exc_info.value) == "Dependency resolution failed"
        assert isinstance(exc_info.value, MigrationError)

