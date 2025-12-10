"""Unit tests for table_metadata module."""

from __future__ import annotations

from hass_migrate.models.table_metadata import (
    MigrationConfig,
    MigrationResult,
    TableMetadata,
    ValidationResult,
)


class TestTableMetadata:
    """Test cases for TableMetadata dataclass."""

    def test_table_metadata_init(self):
        """Test TableMetadata initialization."""
        metadata = TableMetadata(
            name="events",
            columns=["event_id", "event_type", "time_fired"],
            primary_key="event_id",
        )
        assert metadata.name == "events"
        assert metadata.columns == ["event_id", "event_type", "time_fired"]
        assert metadata.primary_key == "event_id"
        assert metadata.foreign_keys == []
        assert metadata.unique_constraints == []
        assert metadata.indexes == []

    def test_table_metadata_with_optional_fields(self):
        """Test TableMetadata with optional fields."""
        metadata = TableMetadata(
            name="events",
            columns=["event_id", "event_type"],
            primary_key="event_id",
            foreign_keys=["event_type_id"],
            unique_constraints=[["event_type"]],
            indexes=["idx_time_fired"],
        )
        assert metadata.foreign_keys == ["event_type_id"]
        assert metadata.unique_constraints == [["event_type"]]
        assert metadata.indexes == ["idx_time_fired"]


class TestMigrationConfig:
    """Test cases for MigrationConfig dataclass."""

    def test_migration_config_defaults(self):
        """Test MigrationConfig with default values."""
        config = MigrationConfig()
        assert config.batch_size == 20000
        assert config.max_concurrent_tables == 4
        assert config.progress_update_interval == 10
        assert config.use_copy is True
        assert config.enable_transactions is True
        assert config.transaction_batch_size == 10
        assert config.schema == "hass"

    def test_migration_config_custom(self):
        """Test MigrationConfig with custom values."""
        config = MigrationConfig(
            batch_size=50000,
            max_concurrent_tables=8,
            schema="custom_schema",
        )
        assert config.batch_size == 50000
        assert config.max_concurrent_tables == 8
        assert config.schema == "custom_schema"


class TestMigrationResult:
    """Test cases for MigrationResult dataclass."""

    def test_migration_result_init(self):
        """Test MigrationResult initialization."""
        result = MigrationResult(
            table="events",
            rows_migrated=1000,
            success=True,
            duration=5.5,
        )
        assert result.table == "events"
        assert result.rows_migrated == 1000
        assert result.success is True
        assert result.duration == 5.5
        assert result.errors == []

    def test_migration_result_with_errors(self):
        """Test MigrationResult with errors."""
        result = MigrationResult(
            table="events",
            rows_migrated=500,
            success=False,
            duration=2.5,
            errors=["Error 1", "Error 2"],
        )
        assert result.errors == ["Error 1", "Error 2"]
        assert result.success is False


class TestValidationResult:
    """Test cases for ValidationResult dataclass."""

    def test_validation_result_init(self):
        """Test ValidationResult initialization."""
        result = ValidationResult(
            table="events",
            row_count_match=True,
            mysql_count=1000,
            pg_count=1000,
        )
        assert result.table == "events"
        assert result.row_count_match is True
        assert result.mysql_count == 1000
        assert result.pg_count == 1000
        assert result.checksum_match is True
        assert result.sample_match is True
        assert result.errors == []

    def test_validation_result_all_match_true(self):
        """Test all_match property when all validations pass."""
        result = ValidationResult(
            table="events",
            row_count_match=True,
            mysql_count=1000,
            pg_count=1000,
            checksum_match=True,
            sample_match=True,
        )
        assert result.all_match is True

    def test_validation_result_all_match_false_row_count(self):
        """Test all_match property when row count doesn't match."""
        result = ValidationResult(
            table="events",
            row_count_match=False,
            mysql_count=1000,
            pg_count=999,
            checksum_match=True,
            sample_match=True,
        )
        assert result.all_match is False

    def test_validation_result_all_match_false_checksum(self):
        """Test all_match property when checksum doesn't match."""
        result = ValidationResult(
            table="events",
            row_count_match=True,
            mysql_count=1000,
            pg_count=1000,
            checksum_match=False,
            sample_match=True,
        )
        assert result.all_match is False

    def test_validation_result_all_match_false_sample(self):
        """Test all_match property when sample doesn't match."""
        result = ValidationResult(
            table="events",
            row_count_match=True,
            mysql_count=1000,
            pg_count=1000,
            checksum_match=True,
            sample_match=False,
        )
        assert result.all_match is False

    def test_validation_result_with_errors(self):
        """Test ValidationResult with errors."""
        result = ValidationResult(
            table="events",
            row_count_match=False,
            mysql_count=1000,
            pg_count=999,
            errors=["Count mismatch"],
        )
        assert result.errors == ["Count mismatch"]

