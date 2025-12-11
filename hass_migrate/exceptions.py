"""Custom exceptions for migration tool."""

from __future__ import annotations


class MigrationError(Exception):
    """Base exception for migration errors."""

    pass


class DatabaseConnectionError(MigrationError):
    """Error connecting to database."""

    pass


class SchemaError(MigrationError):
    """Error with database schema."""

    pass


class DataValidationError(MigrationError):
    """Error validating migrated data."""

    pass


class BackupError(MigrationError):
    """Error creating or restoring backup."""

    pass


class DependencyError(MigrationError):
    """Error analyzing or resolving table dependencies."""

    pass

