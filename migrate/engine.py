from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import asyncpg
import mysql.connector
from asyncpg import Pool

DEFAULT_BATCH_SIZE = 20_000
PROGRESS_FILE = "migration_progress.json"

# Fields that need int→bool conversion (table_name, column_name)
BOOL_COLUMNS = {
    ("recorder_runs", "closed_incorrect"),
    ("statistics_meta", "has_mean"),
    ("statistics_meta", "has_sum"),
}

# Columns that are TIMESTAMP type and may receive float (Unix timestamp) values
TIMESTAMP_COLUMNS = {
    ("schema_changes", "changed"),
    ("recorder_runs", "start"),
    ("recorder_runs", "end"),
    ("recorder_runs", "created"),
    ("statistics_runs", "start"),
    ("statistics", "created"),
    ("statistics", "start"),
    ("statistics", "last_reset"),
    ("statistics_short_term", "created"),
    ("statistics_short_term", "start"),
    ("statistics_short_term", "last_reset"),
    ("events", "time_fired"),
    ("states", "last_changed"),
    ("states", "last_updated"),
}

# Tables with unique constraints that should use ON CONFLICT DO NOTHING
# Format: table_name -> list of columns in the unique constraint
UNIQUE_CONSTRAINTS = {
    "event_types": ["event_type"],
    "states_meta": ["entity_id"],
    "statistics_meta": ["statistic_id"],
    "statistics": ["metadata_id", "start_ts"],
    "statistics_short_term": ["metadata_id", "start_ts"],
}

# Tables with foreign keys that should use row-by-row insertion to handle violations gracefully
# These tables have foreign key constraints that might fail, so we need to skip invalid rows
FOREIGN_KEY_TABLES = {
    "events",  # Has foreign keys to event_data and event_types
    "states",  # Has foreign keys to state_attributes and states_meta
    "statistics",  # Has foreign key to statistics_meta
    "statistics_short_term",  # Has foreign key to statistics_meta
}

# Table names used for validation (must match cli.py TABLES)
TABLE_NAMES = [
    "event_types",
    "event_data",
    "events",
    "state_attributes",
    "states_meta",
    "states",
    "statistics_meta",
    "statistics",
    "statistics_short_term",
    "recorder_runs",
    "statistics_runs",
    "schema_changes",
    "migration_changes",
]


def clean_value(table: str, column: str, value: Any) -> Any:
    """
    Clean values from MySQL to ensure PostgreSQL compatibility.

    Data transformation rules:

    1. NULL remains NULL
    2. Strings:
       - Remove null bytes (\x00)
       - Empty strings "" → None (especially for CHAR(0) fields)
    3. Convert int(0/1) → bool for specific fields
    4. datetime → add tzinfo=UTC (Home Assistant uses UTC)
    5. Convert float (Unix timestamp) → datetime for TIMESTAMP columns
    6. Other types returned as-is
    """
    # 1. NULL returns as-is
    if value is None:
        return None

    # 2. String handling: remove null bytes + empty string→NULL
    if isinstance(value, str):
        if "\x00" in value:
            value = value.replace("\x00", "")
        if value == "":
            return None
        return value

    # 3. Only convert tinyint to bool for explicitly defined boolean fields
    if (table, column) in BOOL_COLUMNS:
        # MySQL driver typically returns int for tinyint(1)
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            # Only accept 0 and 1, error on other values rather than silent conversion
            if value in (0, 1):
                return bool(value)
            # Log warning for unexpected integer values
            print(
                f"Warning: {table}.{column} has non‑boolean integer {value!r}, keeping as‑is",
                file=sys.stderr,
            )
        # Shouldn't reach here, but if we do it indicates data anomaly
        return value

    # 4. datetime → convert to UTC naive datetime
    # PostgreSQL TIMESTAMP (without timezone) expects naive datetimes
    # We normalize to UTC but remove timezone info for compatibility
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # Assume naive datetimes are already in UTC (Home Assistant convention)
            return value
        # Convert timezone-aware datetime to UTC naive datetime
        utc_dt = value.astimezone(timezone.utc)
        return utc_dt.replace(tzinfo=None)

    # 5. Convert float (Unix timestamp) → datetime for TIMESTAMP columns
    if (table, column) in TIMESTAMP_COLUMNS and isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(tzinfo=None)
        except (ValueError, OSError) as e:
            print(
                f"Warning: {table}.{column} has invalid timestamp {value!r}: {e}, keeping as‑is",
                file=sys.stderr,
            )
            return value

    # 6. Other types (int, float, bytes, etc.) returned as-is
    return value


class Migrator:
    def __init__(self, cfg, batch_size: int = DEFAULT_BATCH_SIZE):
        self.cfg = cfg
        self.batch_size = batch_size
        self.mysql: Optional[mysql.connector.MySQLConnection] = None
        self.pool: Optional[Pool] = None
        self.progress: Dict[str, Dict[str, Any]] = {}
        self._progress_lock = asyncio.Lock()

    def load_progress(self) -> None:
        """Load migration progress from file."""
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r") as f:
                self.progress = json.load(f)

    def save_progress(self) -> None:
        """Save migration progress to file (synchronous version for compatibility)."""
        with open(PROGRESS_FILE, "w") as f:
            json.dump(self.progress, f, indent=2)

    async def update_progress(self, table: str, last_id: Any, total: int) -> None:
        """Update and save progress atomically (thread-safe)."""
        async with self._progress_lock:
            self.progress[table] = {"last_id": last_id, "total": total}
            with open(PROGRESS_FILE, "w") as f:
                json.dump(self.progress, f, indent=2)

    # ---------- Connections ----------

    def connect_mysql(self) -> None:
        self.mysql = mysql.connector.connect(
            host=self.cfg.mysql_host,
            port=self.cfg.mysql_port,
            user=self.cfg.mysql_user,
            password=self.cfg.mysql_password,
            database=self.cfg.mysql_db,
            charset="utf8mb4",
        )

    def create_mysql_connection(self) -> mysql.connector.MySQLConnection:
        """Create a new MySQL connection for concurrent migrations."""
        return mysql.connector.connect(
            host=self.cfg.mysql_host,
            port=self.cfg.mysql_port,
            user=self.cfg.mysql_user,
            password=self.cfg.mysql_password,
            database=self.cfg.mysql_db,
            charset="utf8mb4",
        )

    async def connect_pg(self) -> None:
        async def init_conn(conn):
            await conn.execute("SET timezone = 'UTC';")
            await conn.execute("SET search_path = 'hass', 'public';")

        self.pool = await asyncpg.create_pool(
            user=self.cfg.pg_user,
            password=self.cfg.pg_password,
            database=self.cfg.pg_db,
            host=self.cfg.pg_host,
            port=self.cfg.pg_port,
            init=init_conn,
        )

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
        if self.mysql is not None:
            self.mysql.close()
            self.mysql = None

    # ---------- Schema helpers ----------

    async def schema_exists(self) -> bool:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM pg_tables
                WHERE schemaname = 'hass'
                """
            )
        return count > 0

    async def apply_schema(self, filename: str, force: bool = False) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            if force:
                print(f"[DEBUG] apply_schema called with force={force}", file=sys.stderr)
                # First, get all tables in hass schema
                tables = await conn.fetch("""
                    SELECT tablename 
                    FROM pg_tables 
                    WHERE schemaname = 'hass'
                """)
                
                print(f"[DEBUG] Found {len(tables)} tables to drop", file=sys.stderr)
                
                # Drop all tables explicitly with schema qualification
                for table_row in tables:
                    table_name = table_row['tablename']
                    print(f"[DEBUG] Dropping table: hass.{table_name}", file=sys.stderr)
                    await conn.execute(f'DROP TABLE IF EXISTS hass."{table_name}" CASCADE;')
                
                # Verify all tables are dropped
                remaining = await conn.fetchval("""
                    SELECT COUNT(*) 
                    FROM pg_tables 
                    WHERE schemaname = 'hass'
                """)
                print(f"[DEBUG] Remaining tables after drop: {remaining}", file=sys.stderr)
                if remaining > 0:
                    # Force drop any remaining tables
                    remaining_tables = await conn.fetch("""
                        SELECT tablename 
                        FROM pg_tables 
                        WHERE schemaname = 'hass'
                    """)
                    for table_row in remaining_tables:
                        table_name = table_row['tablename']
                        print(f"[DEBUG] Force dropping remaining table: hass.{table_name}", file=sys.stderr)
                        await conn.execute(f'DROP TABLE IF EXISTS hass."{table_name}" CASCADE;')
            
            # Read and execute schema file
            with open(filename, "r", encoding="utf-8") as f:
                sql = f.read()
            await conn.execute(sql)

    async def truncate_table(self, table: str) -> None:
        assert self.pool is not None
        # RESTART IDENTITY resets sequences, CASCADE handles foreign key dependencies
        async with self.pool.acquire() as conn:
            await conn.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE;')

    # ---------- Data migration ----------

    def _build_insert_sql(self, table: str, columns: List[str]) -> str:
        """Build INSERT SQL with ON CONFLICT clause if table has unique constraints."""
        pg_columns = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        
        if table in UNIQUE_CONSTRAINTS:
            conflict_columns = UNIQUE_CONSTRAINTS[table]
            conflict_cols_qualified = ", ".join(f'"{col}"' for col in conflict_columns)
            return f'INSERT INTO "{table}" ({pg_columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_cols_qualified}) DO NOTHING'
        else:
            return f'INSERT INTO "{table}" ({pg_columns}) VALUES ({placeholders})'

    async def migrate_table(
        self, table: str, columns: List[str], mysql_conn: Optional[mysql.connector.MySQLConnection] = None
    ) -> None:
        """
        Generic table migration logic:
        - SELECT corresponding columns from MySQL, supports resume
        - Clean data row by row according to transformation rules
        - Insert into PostgreSQL (batch for simple tables, row-by-row for foreign key tables)

        Args:
            table: Table name to migrate
            columns: List of column names
            mysql_conn: Optional MySQL connection. If None, uses self.mysql
        """
        mysql_connection = mysql_conn if mysql_conn is not None else self.mysql
        assert mysql_connection is not None
        assert self.pool is not None

        pk_col = columns[0]  # Assume first column is primary key
        last_id = self.progress.get(table, {}).get("last_id", None)
        total_migrated = self.progress.get(table, {}).get("total", 0)

        # Use buffered cursor to avoid "Unread result found" errors
        cursor = mysql_connection.cursor(buffered=True)

        try:
            # MySQL side column names
            col_str = ", ".join(columns)
            if last_id is not None:
                cursor.execute(
                    f"SELECT {col_str} FROM {table} WHERE {pk_col} > %s ORDER BY {pk_col}",
                    (last_id,),
                )
            else:
                cursor.execute(f"SELECT {col_str} FROM {table} ORDER BY {pk_col}")

            # Build INSERT SQL once
            insert_sql = self._build_insert_sql(table, columns)
            total = total_migrated

            async with self.pool.acquire() as conn:
                while True:
                    rows: Sequence[Sequence[Any]] = cursor.fetchmany(self.batch_size)
                    if not rows:
                        break

                    # Clean each value by column name with row validation
                    cleaned_batch = []
                    for row in rows:
                        if len(row) != len(columns):
                            print(
                                f"Warning: {table} row has {len(row)} columns, expected {len(columns)}. Skipping row.",
                                file=sys.stderr,
                            )
                            continue
                        cleaned_row = [clean_value(table, col, val) for col, val in zip(columns, row)]
                        cleaned_batch.append(cleaned_row)

                    if not cleaned_batch:
                        continue

                    # For tables with foreign keys, use row-by-row to skip invalid rows
                    # For other tables, use batch insert with retry on failure
                    if table in FOREIGN_KEY_TABLES:
                        inserted_count = await self._insert_rows_individually(
                            conn, table, columns, cleaned_batch, insert_sql
                        )
                    else:
                        inserted_count = await self._insert_batch_with_retry(
                            conn, table, columns, cleaned_batch, insert_sql
                        )

                    total += inserted_count

                    # Update progress atomically
                    last_id = rows[-1][0]  # Assume first column is ID
                    await self.update_progress(table, last_id, total)

                    print(f"{table}: {total:,} rows migrated...")
        finally:
            cursor.close()
            # Close the connection if it was created specifically for this migration
            if mysql_conn is not None:
                mysql_connection.close()

    async def _insert_rows_individually(
        self, conn, table: str, columns: List[str], cleaned_batch: List[List[Any]], insert_sql: str
    ) -> int:
        """
        Insert rows one by one, skipping rows that fail due to constraints.
        Returns the number of successfully inserted rows.
        """
        inserted_count = 0
        skipped_count = 0
        
        for row in cleaned_batch:
            try:
                await conn.execute(insert_sql, *row)
                inserted_count += 1
            except Exception as e:
                # Skip rows that fail (foreign key violations, etc.)
                skipped_count += 1
                if skipped_count <= 10:  # Only log first 10 skipped rows to avoid spam
                    print(
                        f"Warning: Skipping row in {table} due to error: {e}",
                        file=sys.stderr,
                    )
        
        if skipped_count > 10:
            print(
                f"Warning: Skipped {skipped_count} additional rows in {table} due to constraint violations",
                file=sys.stderr,
            )
        
        return inserted_count

    async def _insert_batch_with_retry(
        self, conn, table: str, columns: List[str], cleaned_batch: List[List[Any]], insert_sql: str
    ) -> int:
        """
        Insert batch with retry logic. Falls back to row-by-row if batch fails.
        Returns the number of successfully inserted rows.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await conn.executemany(insert_sql, cleaned_batch)
                # With ON CONFLICT DO NOTHING, we can't know exact count, so return batch size
                # This is approximate but acceptable for progress reporting
                return len(cleaned_batch)
            except Exception as e:
                error_msg = str(e).lower()
                
                # On last retry, fall back to row-by-row insertion
                if attempt == max_retries - 1:
                    print(
                        f"Warning: Batch insert failed for {table} after {max_retries} attempts. "
                        f"Falling back to row-by-row insertion. Error: {e}",
                        file=sys.stderr,
                    )
                    return await self._insert_rows_individually(conn, table, columns, cleaned_batch, insert_sql)
                
                # Retry for transient errors
                print(
                    f"Retry {attempt + 1}/{max_retries} for {table} batch: {e}",
                    file=sys.stderr,
                )
                await asyncio.sleep(1)
        
        # Should never reach here, but return 0 if we do
        return 0

    async def fix_sequence(self, table: str, pk: str) -> None:
        """
        Adjust PostgreSQL sequence to MAX(pk) to avoid primary key conflicts on future inserts.
        """
        assert self.pool is not None

        async with self.pool.acquire() as conn:
            seq = await conn.fetchval(
                "SELECT pg_get_serial_sequence($1, $2)", table, pk
            )
            if not seq:
                # e.g., migration_changes.migration_id which is varchar PK has no sequence
                return

            await conn.execute(
                f"SELECT setval($1, (SELECT COALESCE(MAX({pk}), 1) FROM {table}))",
                seq,
            )

    async def validate_table_counts(self) -> Dict[str, Dict[str, int]]:
        """
        Compare row counts between MySQL and PostgreSQL for all tables.
        Returns a dict mapping table name to {'mysql': count, 'postgres': count, 'schema': schema_name}.
        """
        assert self.mysql is not None
        assert self.pool is not None

        results = {}

        # MySQL counts
        mysql_cursor = self.mysql.cursor()
        for table in TABLE_NAMES:
            mysql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
            mysql_count = mysql_cursor.fetchone()[0]
            results[table] = {'mysql': mysql_count}

        mysql_cursor.close()

        # PostgreSQL counts - detect schema and use fully qualified names
        async with self.pool.acquire() as conn:
            # First, find which schema each table is in
            table_schemas = {}
            for table in TABLE_NAMES:
                schema_info = await conn.fetch(
                    """
                    SELECT schemaname 
                    FROM pg_tables 
                    WHERE tablename = $1 AND schemaname IN ('public', 'hass')
                    ORDER BY schemaname
                    """,
                    table,
                )
                if schema_info:
                    # Use the first schema found (prefer public if both exist)
                    schemas = [row['schemaname'] for row in schema_info]
                    table_schemas[table] = 'public' if 'public' in schemas else schemas[0]
                else:
                    # Table not found in either schema
                    table_schemas[table] = None

            # Count rows using fully qualified schema.table names
            for table in TABLE_NAMES:
                schema = table_schemas[table]
                if schema is None:
                    # Table doesn't exist in PostgreSQL
                    results[table]['postgres'] = 0
                    results[table]['schema'] = None
                else:
                    # Use fully qualified name to avoid search_path issues
                    pg_count = await conn.fetchval(
                        f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                    )
                    results[table]['postgres'] = pg_count
                    results[table]['schema'] = schema

        return results
