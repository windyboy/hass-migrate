from __future__ import annotations

import asyncio
from typing import Any, Iterable, Sequence
from unittest.mock import AsyncMock, MagicMock

import pytest

from migrate.models.table_metadata import MigrationConfig, MigrationResult
from migrate.services.migration_service import MigrationService


class DummyTransaction:
    async def __aenter__(self) -> DummyTransaction:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class DummyAcquire:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    async def __aenter__(self) -> Any:
        return self._connection

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class DummyPool:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def acquire(self) -> DummyAcquire:
        return DummyAcquire(self._connection)


class RecordingConnection:
    def __init__(self) -> None:
        self.executed_batches: list[tuple[str, Iterable[Sequence[Any]]]] = []
        self.copy_batches: list[tuple[str, list[list[Any]], list[str], str | None]] = []

    def transaction(self) -> DummyTransaction:
        return DummyTransaction()

    async def executemany(self, query: str, values: Iterable[Sequence[Any]]) -> None:
        self.executed_batches.append((query, list(values)))

    async def copy_records_to_table(
        self,
        table: str,
        records: Iterable[Sequence[Any]],
        columns: Sequence[str],
        schema_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        materialized = [list(row) for row in records]
        self.copy_batches.append((table, materialized, list(columns), schema_name))


class CopyFailingConnection(RecordingConnection):
    def __init__(self) -> None:
        super().__init__()
        self.copy_attempts = 0

    async def copy_records_to_table(
        self,
        table: str,
        records: Iterable[Sequence[Any]],
        columns: Sequence[str],
        schema_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.copy_attempts += 1
        await super().copy_records_to_table(
            table, records, columns, schema_name, **kwargs
        )
        raise RuntimeError("copy failed")


class ExecutemanyFailingConnection(RecordingConnection):
    def __init__(self) -> None:
        super().__init__()
        self.executemany_calls = 0

    async def executemany(self, query: str, values: Iterable[Sequence[Any]]) -> None:
        self.executemany_calls += 1
        raise RuntimeError("executemany failed")


class StubPGClient:
    def __init__(self, connection: Any) -> None:
        self.pool = DummyPool(connection)


@pytest.mark.asyncio
async def test_migrate_table_updates_progress_and_returns_success() -> None:
    mysql_client = MagicMock()
    mysql_client.fetch_batch_with_resume = AsyncMock(
        side_effect=[
            ([(1, "alpha"), (2, "beta")], 2),
            ([], None),
        ]
    )

    dependency_analyzer = MagicMock()
    logger = MagicMock()
    pg_connection = RecordingConnection()
    pg_client = StubPGClient(pg_connection)

    service = MigrationService(
        mysql_client=mysql_client,
        pg_client=pg_client,
        dependency_analyzer=dependency_analyzer,
        logger=logger,
    )

    config = MigrationConfig(batch_size=2, max_chunk_size=1, use_copy=True)

    result = await service.migrate_table(
        table="test_table",
        columns=["id", "value"],
        config=config,
    )

    assert result.success is True
    assert result.rows_migrated == 2
    assert service.progress["test_table"] == {
        "last_id": 2,
        "total": 2,
        "status": "completed",
    }
    assert len(pg_connection.executed_batches) == 0
    assert len(pg_connection.copy_batches) == 2
    copied_tables = [entry[0] for entry in pg_connection.copy_batches]
    assert copied_tables == ["test_table", "test_table"]
    copied_rows = [rows for _, rows, _, _ in pg_connection.copy_batches]
    assert copied_rows == [[[1, "alpha"]], [[2, "beta"]]]
    copied_schemas = [schema for _, _, _, schema in pg_connection.copy_batches]
    assert all(schema == config.schema for schema in copied_schemas)
    mysql_client.fetch_batch_with_resume.assert_awaited()
    logger.log_migration_event.assert_called_once_with(
        "migration_complete",
        "test_table",
        rows_migrated=2,
        duration=pytest.approx(result.duration, rel=1e-6),
    )


@pytest.mark.asyncio
async def test_migrate_table_retries_copy_with_executemany() -> None:
    mysql_client = MagicMock()
    mysql_client.fetch_batch_with_resume = AsyncMock(
        side_effect=[([(1, "value")], 1), ([], None)]
    )

    dependency_analyzer = MagicMock()
    logger = MagicMock()
    pg_connection = CopyFailingConnection()
    pg_client = StubPGClient(pg_connection)

    service = MigrationService(
        mysql_client=mysql_client,
        pg_client=pg_client,
        dependency_analyzer=dependency_analyzer,
        logger=logger,
    )

    config = MigrationConfig(batch_size=1, max_chunk_size=1, use_copy=True)

    result = await service.migrate_table(
        table="retry_table",
        columns=["id", "value"],
        config=config,
    )

    assert result.success is True
    assert result.rows_migrated == 1
    assert service.progress["retry_table"]["status"] == "completed"
    assert pg_connection.copy_attempts == 1
    assert len(pg_connection.executed_batches) == 1
    assert len(pg_connection.copy_batches) == 1
    warning_message = logger.warning.call_args[0][0]
    assert "COPY failed for retry_table" in warning_message


@pytest.mark.asyncio
async def test_migrate_table_executemany_failure_sets_failed_status() -> None:
    mysql_client = MagicMock()
    mysql_client.fetch_batch_with_resume = AsyncMock(
        side_effect=[([(1, "value")], 1), ([], None)]
    )

    dependency_analyzer = MagicMock()
    logger = MagicMock()
    pg_connection = ExecutemanyFailingConnection()
    pg_client = StubPGClient(pg_connection)

    service = MigrationService(
        mysql_client=mysql_client,
        pg_client=pg_client,
        dependency_analyzer=dependency_analyzer,
        logger=logger,
    )

    config = MigrationConfig(batch_size=1, max_chunk_size=1, use_copy=False)

    result = await service.migrate_table(
        table="failed_table",
        columns=["id", "value"],
        config=config,
    )

    assert result.success is False
    assert any(
        "Error inserting batch for failed_table" in call.args[0]
        for call in logger.error.call_args_list
    )
    assert (
        result.errors[0] == "Error inserting batch for failed_table: executemany failed"
    )
    assert result.errors[1].startswith(
        "Unexpected error during migration of failed_table"
    )
    assert service.progress["failed_table"] == {
        "last_id": None,
        "total": 0,
        "status": "failed",
    }
    assert pg_connection.executemany_calls == 1


@pytest.mark.asyncio
async def test_migrate_all_tables_respects_concurrency_and_handles_failures() -> None:
    mysql_client = MagicMock()
    logger = MagicMock()

    dependency_analyzer = MagicMock()
    dependency_analyzer.analyze_dependencies = AsyncMock(return_value={})
    dependency_analyzer.topological_sort = MagicMock(
        return_value=[["first_table", "second_table"]]
    )

    pg_client = MagicMock()
    pg_client.pool = MagicMock()

    service = MigrationService(
        mysql_client=mysql_client,
        pg_client=pg_client,
        dependency_analyzer=dependency_analyzer,
        logger=logger,
    )

    config = MigrationConfig(max_concurrent_tables=1)

    ongoing = 0
    max_seen = 0
    lock = asyncio.Lock()

    async def fake_migrate_table(table: str, columns, config, progress_tracker=None):
        nonlocal ongoing, max_seen
        async with lock:
            ongoing += 1
            max_seen = max(max_seen, ongoing)
        try:
            await asyncio.sleep(0)
            if table == "second_table":
                raise RuntimeError("boom")
            service.progress[table] = {"total": 1, "status": "completed"}
            return MigrationResult(
                table=table,
                rows_migrated=1,
                success=True,
                duration=0.01,
            )
        finally:
            async with lock:
                ongoing -= 1

    service.migrate_table = fake_migrate_table  # type: ignore[assignment]

    all_tables = [
        ("first_table", ["id"]),
        ("second_table", ["id"]),
    ]

    results = await service.migrate_all_tables(all_tables, config)

    assert max_seen == 1
    assert len(results) == 2
    assert results[0].table == "first_table"
    assert results[0].success is True
    assert results[1].table == "second_table"
    assert results[1].success is False
    assert results[1].rows_migrated == 0
    dependency_analyzer.analyze_dependencies.assert_awaited_once()
    dependency_analyzer.topological_sort.assert_called_once_with(
        ["first_table", "second_table"], {}
    )
