"""Unit tests for table_metadata module."""

from __future__ import annotations

from hass_migrate.models.table_metadata import TableMetadata, ValidationResult


class TestTableMetadata:
    """Test cases for TableMetadata dataclass."""

    def test_table_metadata_defaults(self):
        """Test TableMetadata default values initialization."""
        metadata = TableMetadata(
            name="events",
            columns=["event_id"],
            primary_key="event_id",
        )
        # Verify __post_init__ sets defaults correctly
        assert metadata.foreign_keys == []
        assert metadata.unique_constraints == []
        assert metadata.indexes == []


class TestValidationResult:
    """Test cases for ValidationResult dataclass."""

    def test_validation_result_all_match(self):
        """Test all_match property logic."""
        # Case 1: All true
        result = ValidationResult(
            table="events",
            row_count_match=True,
            mysql_count=100,
            pg_count=100,
            checksum_match=True,
            sample_match=True,
        )
        assert result.all_match is True

        # Case 2: Row count mismatch
        result = ValidationResult(
            table="events",
            row_count_match=False,
            mysql_count=100,
            pg_count=99,
        )
        assert result.all_match is False

