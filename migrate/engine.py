from __future__ import annotations

import asyncio
import json
import os
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
    5. Other types returned as-is
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
        # Shouldn't reach here, but if we do it indicates data anomaly
        return value

    # 4. datetime → add timezone info
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        # Convert to UTC to avoid timezone confusion
        return value.astimezone(timezone.utc)

    # 5. Other types (int, float, bytes, etc.) returned as-is
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

    async def connect_pg(self) -> None:
        async def init_conn(conn):
            await conn.execute("SET timezone = 'UTC';")

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
                WHERE schemaname = 'public'
                """
            )
        return count > 0

    async def apply_schema(self, filename: str) -> None:
        assert self.pool is not None
        with open(filename, "r", encoding="utf-8") as f:
            sql = f.read()
        async with self.pool.acquire() as conn:
            await conn.execute(sql)

    async def truncate_table(self, table: str) -> None:
        assert self.pool is not None
        # RESTART IDENTITY resets sequences, CASCADE handles foreign key dependencies
        async with self.pool.acquire() as conn:
            await conn.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE;')

    # ---------- Data migration ----------

    async def migrate_table(self, table: str, columns: List[str]) -> None:
        """
        Generic table migration logic:
        - SELECT corresponding columns from MySQL, supports resume
        - Clean data row by row according to transformation rules
        - Batch INSERT into PostgreSQL
        """
        assert self.mysql is not None
        assert self.pool is not None

        pk_col = columns[0]  # Assume first column is primary key
        last_id = self.progress.get(table, {}).get("last_id", None)
        total_migrated = self.progress.get(table, {}).get("total", 0)

        cursor = self.mysql.cursor()

        # MySQL side column names
        col_str = ", ".join(columns)
        if last_id is not None:
            cursor.execute(
                f"SELECT {col_str} FROM {table} WHERE {pk_col} > %s ORDER BY {pk_col}",
                (last_id,),
            )
        else:
            cursor.execute(f"SELECT {col_str} FROM {table} ORDER BY {pk_col}")

        # PostgreSQL side
        pg_columns = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        insert_sql = f'INSERT INTO "{table}" ({pg_columns}) VALUES ({placeholders})'

        total = total_migrated

        async with self.pool.acquire() as conn:
            while True:
                rows: Sequence[Sequence[Any]] = cursor.fetchmany(self.batch_size)
                if not rows:
                    break

                # Clean each value by column name
                cleaned_batch = [
                    [clean_value(table, col, val) for col, val in zip(columns, row)]
                    for row in rows
                ]

                # Retry mechanism
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        await conn.executemany(insert_sql, cleaned_batch)
                        break
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise e
                        print(
                            f"Retry {attempt + 1}/{max_retries} for {table} batch: {e}"
                        )
                        await asyncio.sleep(1)

                total += len(cleaned_batch)

                # Update progress atomically
                last_id = rows[-1][0]  # Assume first column is ID
                await self.update_progress(table, last_id, total)

                print(f"{table}: {total:,} rows migrated...")

        cursor.close()

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
