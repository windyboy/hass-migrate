"""Migration service for orchestrating data migration."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

import asyncpg

from migrate.database.mysql_client import MySQLClient
from migrate.database.pg_client import PGClient
from migrate.models.table_metadata import MigrationConfig, MigrationResult
from migrate.utils.data_cleaner import clean_batch_values
from migrate.utils.dependency import DependencyAnalyzer
from migrate.utils.logger import StructuredLogger
from migrate.utils.progress_tracker import ProgressTracker

# Tables with unique constraints
UNIQUE_CONSTRAINTS: Dict[str, List[List[str]]] = {
    "event_types": [["event_type"]],
    "states_meta": [["entity_id"]],
    "statistics_meta": [["statistic_id"]],
    "statistics": [["metadata_id", "start_ts"]],
    "statistics_short_term": [["metadata_id", "start_ts"]],
}


class MigrationService:
    """Service for orchestrating table migrations with async support."""

    def __init__(
        self,
        mysql_client: MySQLClient,
        pg_client: PGClient,
        dependency_analyzer: DependencyAnalyzer,
        logger: StructuredLogger,
    ):
        """
        Initialize migration service.

        Args:
            mysql_client: Async MySQL client instance
            pg_client: Async PostgreSQL client instance
            dependency_analyzer: Dependency analyzer for table relationships
            logger: Structured logger for migration events
        """
        self.mysql_client = mysql_client
        self.pg_client = pg_client
        self.dependency_analyzer = dependency_analyzer
        self.logger = logger
        self.progress: Dict[str, Dict[str, Any]] = {}

    def load_progress(self, progress_data: Dict[str, Dict[str, Any]]) -> None:
        """
        Load migration progress.

        Args:
            progress_data: Progress data dictionary
        """
        self.progress = progress_data

    def get_progress(self) -> Dict[str, Dict[str, Any]]:
        """
        Get current progress.

        Returns:
            Progress dictionary
        """
        return self.progress

    async def migrate_table(
        self,
        table: str,
        columns: List[str],
        config: MigrationConfig,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> MigrationResult:
        """
        Migrate a single table.

        Args:
            table: Table name
            columns: Column names
            config: Migration configuration
            progress_tracker: Optional progress tracker

        Returns:
            Migration result
        """
        start_time = time.time()
        errors: List[str] = []

        if progress_tracker is None:
            progress_tracker = ProgressTracker(
                update_interval=config.progress_update_interval
            )

        pk_col = columns[0]  # Assume first column is primary key

        last_id = self.progress.get(table, {}).get("last_id", None)

        total_migrated = self.progress.get(table, {}).get("total", 0)

        unique_constraints = UNIQUE_CONSTRAINTS.get(table)

        try:
            total = total_migrated

            pk_index = columns.index(pk_col) if pk_col in columns else 0
            self.progress.setdefault(
                table, {"last_id": last_id, "total": total, "status": "in_progress"}
            )

            if self.pg_client.pool is None:
                raise RuntimeError("PostgreSQL pool not established")

            async with self.pg_client.pool.acquire() as conn:
                while True:
                    rows, _ = await self.mysql_client.fetch_batch_with_resume(
                        table=table,
                        columns=columns,
                        batch_size=config.batch_size,
                        last_id=last_id,
                        primary_key=pk_col,
                    )

                    if not rows:
                        break

                    raw_chunk_size = getattr(config, "max_chunk_size", None)

                    max_chunk_size = (
                        raw_chunk_size
                        if isinstance(raw_chunk_size, int) and raw_chunk_size > 0
                        else len(rows)
                    )
                    if len(rows) > max_chunk_size:
                        chunks = [
                            rows[i : i + max_chunk_size]
                            for i in range(0, len(rows), max_chunk_size)
                        ]

                    else:
                        chunks = [rows]

                    for chunk in chunks:
                        cleaned_batch = clean_batch_values(table, columns, list(chunk))

                        if not cleaned_batch:
                            continue

                        inserted_count = 0

                        try:
                            async with conn.transaction():
                                if config.use_copy:
                                    await conn.copy_records_to_table(
                                        table,
                                        records=cleaned_batch,
                                        columns=columns,
                                        schema_name=config.schema,
                                    )

                                    inserted_count = len(cleaned_batch)

                                else:
                                    inserted_count = await self._insert_executemany(
                                        conn,
                                        table,
                                        columns,
                                        cleaned_batch,
                                        unique_constraints,
                                        schema=config.schema,
                                    )

                        except Exception as primary_error:
                            if config.use_copy:
                                self.logger.warning(
                                    f"COPY failed for {table}, retrying with executemany",
                                    error=str(primary_error),
                                )
                                try:
                                    async with conn.transaction():
                                        inserted_count = await self._insert_executemany(
                                            conn,
                                            table,
                                            columns,
                                            cleaned_batch,
                                            unique_constraints,
                                            schema=config.schema,
                                        )
                                except Exception as fallback_error:
                                    error_msg = f"Error inserting batch for {table} using executemany: {fallback_error}"

                                    errors.append(error_msg)

                                    self.logger.error(error_msg)

                                    raise RuntimeError(error_msg) from fallback_error
                            else:
                                error_msg = f"Error inserting batch for {table}: {primary_error}"
                                errors.append(error_msg)
                                self.logger.error(error_msg)
                                raise RuntimeError(error_msg) from primary_error

                        total += inserted_count
                        last_id = chunk[-1][pk_index]
                        self.progress[table].update(
                            {
                                "last_id": last_id,
                                "total": total,
                            }
                        )

                        if progress_tracker.should_update():
                            self.logger.info(f"{table}: {total:,} rows migrated...")

                    del rows

            self.progress[table].update(
                {
                    "status": "completed",
                }
            )

            duration = time.time() - start_time
            self.logger.log_migration_event(
                "migration_complete",
                table,
                rows_migrated=total,
                duration=duration,
            )

            return MigrationResult(
                table=table,
                rows_migrated=total,
                success=len(errors) == 0,
                duration=duration,
                errors=errors,
            )

        except Exception as e:
            error_msg = f"Unexpected error during migration of {table}: {e}"
            errors.append(error_msg)
            self.logger.error(error_msg)
            self.progress[table].update(
                {
                    "status": "failed",
                }
            )
            self.logger.warning(
                f"Partial progress recorded for {table}: last_id={last_id}, rows_migrated={total}"
            )
            return MigrationResult(
                table=table,
                rows_migrated=total,
                success=False,
                duration=time.time() - start_time,
                errors=errors,
            )

    async def _insert_executemany(
        self,
        conn: asyncpg.Connection,
        table: str,
        columns: List[str],
        cleaned_batch: List[List[Any]],
        unique_constraints: Optional[List[List[str]]],
        schema: str = None,
    ) -> int:
        """
        Insert using executemany (fallback method).

        Args:
            conn: PostgreSQL connection
            table: Table name
            columns: Column names
            cleaned_batch: Cleaned batch of rows
            unique_constraints: Unique constraint columns for ON CONFLICT
            schema: Schema name (required)

        Returns:
            Number of inserted rows
        """
        if schema is None:
            raise ValueError("Schema name is required")
        pg_columns = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))

        if unique_constraints:
            conflict_cols = unique_constraints[0]
            conflict_cols_qualified = ", ".join(f'"{col}"' for col in conflict_cols)
            insert_sql = f'INSERT INTO "{schema}"."{table}" ({pg_columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_cols_qualified}) DO NOTHING'
        else:
            insert_sql = f'INSERT INTO "{schema}"."{table}" ({pg_columns}) VALUES ({placeholders})'

        await conn.executemany(insert_sql, cleaned_batch)
        return len(cleaned_batch)

    async def migrate_all_tables(
        self,
        all_tables: List[tuple[str, List[str]]],
        config: MigrationConfig,
    ) -> List[MigrationResult]:
        """
        Migrate all tables in dependency order.

        Args:
            all_tables: List of (table_name, columns) tuples
            config: Migration configuration

        Returns:
            List of migration results
        """
        # Analyze dependencies
        deps = await self.dependency_analyzer.analyze_dependencies(
            self.pg_client.pool, schema=config.schema
        )
        table_names = [t[0] for t in all_tables]
        table_levels = self.dependency_analyzer.topological_sort(table_names, deps)

        self.logger.info(f"Analyzed dependencies: {len(table_levels)} levels")

        results: List[MigrationResult] = []

        # Migrate by level
        for level_idx, level_tables in enumerate(table_levels):
            self.logger.info(
                f"Migrating level {level_idx + 1}: {', '.join(level_tables)}"
            )

            # Create tasks for this level with concurrency limit
            max_concurrent = getattr(config, "max_concurrent_tables", 1) or 1
            semaphore = asyncio.Semaphore(max(1, max_concurrent))
            tasks = []

            async def run_table(
                table_name: str, table_columns: List[str]
            ) -> MigrationResult:
                async with semaphore:
                    start = time.time()
                    self.logger.info(f"Starting migration for {table_name}")
                    try:
                        result = await self.migrate_table(
                            table_name,
                            table_columns,
                            config,
                        )
                    except Exception as exc:
                        duration = time.time() - start
                        error_msg = f"Migration failed for {table_name}: {exc}"
                        self.logger.error(error_msg)
                        progress_snapshot = self.progress.get(table_name, {})
                        partial_rows = progress_snapshot.get("total", 0)
                        if partial_rows:
                            self.logger.warning(
                                f"Partial progress retained for {table_name}: {partial_rows} rows migrated so far"
                            )
                        return MigrationResult(
                            table=table_name,
                            rows_migrated=partial_rows,
                            success=False,
                            duration=duration,
                            errors=[str(exc)],
                        )
                    if result.success:
                        self.logger.info(
                            f"Completed migration for {table_name} in {result.duration:.2f}s"
                        )
                    else:
                        self.logger.error(
                            f"Migration for {table_name} completed with errors after {result.duration:.2f}s"
                        )
                    return result

            for table_name in level_tables:
                # Find columns for this table
                columns = next(
                    (cols for t, cols in all_tables if t == table_name), None
                )
                if columns is None:
                    self.logger.warning(f"Columns not found for table {table_name}")
                    continue

                tasks.append(asyncio.create_task(run_table(table_name, columns)))

            if not tasks:
                continue

            level_results = await asyncio.gather(*tasks)
            results.extend(level_results)

        return results
