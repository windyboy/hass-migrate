"""Migration service for orchestrating data migration."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

import asyncpg

from hass_migrate.database.mysql_client import MySQLClient
from hass_migrate.database.pg_client import PGClient
from hass_migrate.models.table_metadata import MigrationConfig, MigrationResult
from hass_migrate.utils.data_cleaner import clean_batch_values
from hass_migrate.utils.dependency import DependencyAnalyzer
from hass_migrate.utils.logger import StructuredLogger
from hass_migrate.utils.progress_tracker import ProgressTracker

# Tables with unique constraints
UNIQUE_CONSTRAINTS: Dict[str, List[List[str]]] = {
    "event_types": [["event_type"]],
    "states_meta": [["entity_id"]],
    "statistics_meta": [["statistic_id"]],
    "statistics": [["metadata_id", "start_ts"]],
    "statistics_short_term": [["metadata_id", "start_ts"]],
}


class MigrationService:
    """Service for orchestrating table migrations."""

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
            mysql_client: MySQL client
            pg_client: PostgreSQL client
            dependency_analyzer: Dependency analyzer
            logger: Logger instance
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
        mysql_conn=None,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> MigrationResult:
        """
        Migrate a single table.

        Args:
            table: Table name
            columns: Column names
            config: Migration configuration
            mysql_conn: Optional MySQL connection (for concurrent migrations)
            progress_tracker: Optional progress tracker

        Returns:
            Migration result
        """
        start_time = time.time()
        errors: List[str] = []

        mysql_connection = mysql_conn if mysql_conn is not None else self.mysql_client.connection
        if mysql_connection is None:
            raise RuntimeError("MySQL connection not established")

        if progress_tracker is None:
            progress_tracker = ProgressTracker(
                update_interval=config.progress_update_interval
            )

        pk_col = columns[0]  # Assume first column is primary key
        last_id = self.progress.get(table, {}).get("last_id", None)
        total_migrated = self.progress.get(table, {}).get("total", 0)

        cursor = mysql_connection.cursor(buffered=True)
        unique_constraints = UNIQUE_CONSTRAINTS.get(table)

        try:
            col_str = ", ".join(columns)
            if last_id is not None:
                cursor.execute(
                    f"SELECT {col_str} FROM {table} WHERE {pk_col} > %s ORDER BY {pk_col}",
                    (last_id,),
                )
            else:
                cursor.execute(f"SELECT {col_str} FROM {table} ORDER BY {pk_col}")

            total = total_migrated
            batch_count = 0

            async with self.pg_client.pool.acquire() as conn:
                while True:
                    rows = cursor.fetchmany(config.batch_size)
                    if not rows:
                        break

                    # Clean batch
                    cleaned_batch = clean_batch_values(table, columns, list(rows))

                    if not cleaned_batch:
                        continue

                    # Insert batch
                    try:
                        if config.use_copy:
                            try:
                                await conn.copy_records_to_table(
                                    table,
                                    records=cleaned_batch,
                                    columns=columns,
                                    schema_name=config.schema,
                                )
                                inserted_count = len(cleaned_batch)
                            except Exception as copy_error:
                                # Fall back to executemany
                                self.logger.warning(
                                    f"COPY failed for {table}, using executemany",
                                    error=str(copy_error),
                                )
                                inserted_count = await self._insert_executemany(
                                    conn, table, columns, cleaned_batch, unique_constraints, schema=config.schema
                                )
                        else:
                            inserted_count = await self._insert_executemany(
                                conn, table, columns, cleaned_batch, unique_constraints, schema=config.schema
                            )

                        total += inserted_count
                        batch_count += 1

                        # Update progress if needed
                        if progress_tracker.should_update():
                            last_id = rows[-1][0] if rows else last_id
                            self.progress[table] = {"last_id": last_id, "total": total}

                    except Exception as e:
                        error_msg = f"Error inserting batch for {table}: {e}"
                        errors.append(error_msg)
                        self.logger.error(error_msg)

                    # Log progress periodically
                    if batch_count % 10 == 0:
                        self.logger.info(f"{table}: {total:,} rows migrated...")

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

        finally:
            cursor.close()
            if mysql_conn is not None:
                mysql_connection.close()

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
            self.logger.info(f"Migrating level {level_idx + 1}: {', '.join(level_tables)}")

            # Create tasks for this level
            tasks = []
            for table_name in level_tables:
                # Find columns for this table
                columns = next((cols for t, cols in all_tables if t == table_name), None)
                if columns is None:
                    self.logger.warning(f"Columns not found for table {table_name}")
                    continue

                # Create MySQL connection for concurrent migration
                mysql_conn = self.mysql_client.create_connection()
                task = self.migrate_table(
                    table_name,
                    columns,
                    config,
                    mysql_conn=mysql_conn,
                )
                tasks.append(task)

            # Execute level migrations (can be parallel)
            if len(tasks) == 1:
                # Single table, no need for gather
                result = await tasks[0]
                results.append(result)
            else:
                # Multiple tables, migrate concurrently
                level_results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in level_results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Migration failed: {result}")
                    else:
                        results.append(result)

        return results

