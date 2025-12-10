"""Unit tests for migration_service module."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hass_migrate.models.table_metadata import MigrationConfig, MigrationResult
from hass_migrate.services.migration_service import MigrationService
from hass_migrate.utils.dependency import DependencyAnalyzer
from hass_migrate.utils.logger import StructuredLogger


@pytest.fixture
def mock_mysql_client():
    """Mock MySQL client."""
    client = MagicMock()
    client.connection = MagicMock()
    client.create_connection = MagicMock(return_value=MagicMock())
    return client


@pytest.fixture
def mock_pg_client():
    """Mock PostgreSQL client."""
    client = MagicMock()
    client.pool = MagicMock()
    # Mock the async context manager
    mock_conn = AsyncMock()
    client.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    client.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    client.fix_sequence = AsyncMock()
    return client


@pytest.fixture
def mock_dependency_analyzer():
    """Mock dependency analyzer."""
    analyzer = MagicMock()
    analyzer.analyze_dependencies = AsyncMock(return_value={})
    analyzer.topological_sort = MagicMock(return_value=[["table1"], ["table2"]])
    return analyzer


@pytest.fixture
def mock_logger():
    """Mock logger."""
    return MagicMock(spec=StructuredLogger)


@pytest.fixture
def migration_service(mock_mysql_client, mock_pg_client, mock_dependency_analyzer, mock_logger):
    """Migration service instance with mocked dependencies."""
    return MigrationService(
        mysql_client=mock_mysql_client,
        pg_client=mock_pg_client,
        dependency_analyzer=mock_dependency_analyzer,
        logger=mock_logger,
    )


@pytest.fixture
def migration_config():
    """Sample migration config."""
    return MigrationConfig(batch_size=1000, schema="public")


class TestMigrationService:
    """Test cases for MigrationService class."""

    def test_init(self, migration_service, mock_mysql_client, mock_pg_client, mock_dependency_analyzer, mock_logger):
        """Test MigrationService initialization."""
        assert migration_service.mysql_client == mock_mysql_client
        assert migration_service.pg_client == mock_pg_client
        assert migration_service.dependency_analyzer == mock_dependency_analyzer
        assert migration_service.logger == mock_logger
        assert migration_service.progress == {}

    async def test_migrate_table_success(self, migration_service, migration_config, mock_mysql_client, mock_pg_client, mock_logger):
        """Test successful table migration."""
        # Mock the MySQL connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchmany.return_value = []  # No rows to migrate
        mock_connection.cursor.return_value = mock_cursor
        mock_mysql_client.connection = mock_connection

        # Mock PG connection
        mock_pg_conn = AsyncMock()
        mock_pg_client.pool.acquire.return_value.__aenter__.return_value = mock_pg_conn

        result = await migration_service.migrate_table(
            table="event_types",
            columns=["event_type_id", "event_type"],
            config=migration_config
        )

        assert isinstance(result, MigrationResult)
        assert result.table == "event_types"
        assert result.success is True
        assert result.rows_migrated == 0  # No rows

    async def test_migrate_table_invalid_table(self, migration_service, migration_config):
        """Test migration with invalid table name."""
        result = await migration_service.migrate_table(
            table="invalid_table",
            columns=["col1"],
            config=migration_config
        )

        assert result.success is False
        assert "Invalid table name" in result.errors[0]

    async def test_migrate_table_insert_error(self, migration_service, migration_config, mock_mysql_client, mock_pg_client, mock_logger):
        """Test migration with insert error."""
        # Mock the MySQL connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [[(1, "test")], []]  # First call returns row, second returns empty
        mock_connection.cursor.return_value = mock_cursor
        mock_mysql_client.connection = mock_connection

        # Mock PG connection to raise error on copy and executemany
        mock_pg_conn = AsyncMock()
        mock_pg_conn.copy_records_to_table.side_effect = Exception("Copy failed")
        mock_pg_conn.executemany.side_effect = Exception("Insert failed")
        mock_pg_client.pool.acquire.return_value.__aenter__.return_value = mock_pg_conn

        with patch('hass_migrate.services.migration_service.clean_batch_values', return_value=[(1, "test")]), \
             patch('hass_migrate.services.migration_service.ProgressTracker') as mock_tracker:
            mock_tracker_instance = MagicMock()
            mock_tracker_instance.should_update.return_value = False
            mock_tracker.return_value = mock_tracker_instance

            result = await migration_service.migrate_table(
                table="event_types",  # Valid table
                columns=["event_type_id", "event_type"],
                config=migration_config
            )

        assert isinstance(result, MigrationResult)
        assert result.success is False
        assert len(result.errors) > 0

    async def test_migrate_all_tables_success(self, migration_service, migration_config, mock_dependency_analyzer, mock_logger):
        """Test successful migration of all tables."""
        # Mock migrate_table to return success
        migration_service.migrate_table = AsyncMock(return_value=MigrationResult(
            table="table1", rows_migrated=100, success=True, duration=1.0, errors=[]
        ))

        all_tables = [("table1", ["col1"]), ("table2", ["col1"])]

        results = await migration_service.migrate_all_tables(all_tables, migration_config)

        assert len(results) == 2
        assert all(r.success for r in results)
        mock_dependency_analyzer.analyze_dependencies.assert_called_once()
        mock_dependency_analyzer.topological_sort.assert_called_once()

    async def test_migrate_all_tables_with_backup(self, migration_service, migration_config, mock_dependency_analyzer):
        """Test migration with backup service."""
        from hass_migrate.services.backup_service import BackupService

        mock_backup = MagicMock(spec=BackupService)
        mock_backup.create_backup = AsyncMock(return_value="/path/to/backup")

        db_config = MagicMock()

        migration_service.migrate_table = AsyncMock(return_value=MigrationResult(
            table="table1", rows_migrated=100, success=True, duration=1.0, errors=[]
        ))

        all_tables = [("table1", ["col1"])]

        results = await migration_service.migrate_all_tables(
            all_tables, migration_config, backup_service=mock_backup, db_config=db_config
        )

        assert len(results) == 1
        mock_backup.create_backup.assert_called_once_with(db_config)

    async def test_load_progress(self, migration_service):
        """Test loading migration progress."""
        progress_data = {"table1": {"last_id": 100, "total": 200}}
        migration_service.load_progress(progress_data)
        assert migration_service.progress == progress_data

    def test_get_progress(self, migration_service):
        """Test getting migration progress."""
        progress_data = {"table1": {"last_id": 100, "total": 200}}
        migration_service.progress = progress_data
        assert migration_service.get_progress() == progress_data