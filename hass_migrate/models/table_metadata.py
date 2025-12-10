"""Table metadata models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class TableMetadata:
    """Table metadata information."""

    name: str
    columns: List[str]
    primary_key: str
    foreign_keys: List[str] = None
    unique_constraints: List[List[str]] = None
    indexes: List[str] = None

    def __post_init__(self):
        """Initialize default values."""
        if self.foreign_keys is None:
            self.foreign_keys = []
        if self.unique_constraints is None:
            self.unique_constraints = []
        if self.indexes is None:
            self.indexes = []


@dataclass
class MigrationConfig:
    """Migration configuration."""

    batch_size: int = 20000
    max_concurrent_tables: int = 4
    progress_update_interval: int = 10
    use_copy: bool = True
    enable_transactions: bool = True
    transaction_batch_size: int = 10  # Number of batches per transaction
    schema: str = "hass"  # PostgreSQL schema name


@dataclass
class MigrationResult:
    """Result of a migration operation."""

    table: str
    rows_migrated: int
    success: bool
    duration: float
    errors: List[str] = None

    def __post_init__(self):
        """Initialize default values."""
        if self.errors is None:
            self.errors = []


@dataclass
class ValidationResult:
    """Result of a validation operation."""

    table: str
    row_count_match: bool
    mysql_count: int
    pg_count: int
    checksum_match: bool = True
    sample_match: bool = True
    errors: List[str] = None

    def __post_init__(self):
        """Initialize default values."""
        if self.errors is None:
            self.errors = []

    @property
    def all_match(self) -> bool:
        """Check if all validations passed."""
        return self.row_count_match and self.checksum_match and self.sample_match

