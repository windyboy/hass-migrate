"""PostgreSQL database client."""

from __future__ import annotations

from typing import Any, List, Optional

import asyncpg
from asyncpg import Connection, Pool

from migrate.config import DBConfig


class PGClient:
    """PostgreSQL database client for writing data."""

    def __init__(self, config: DBConfig, schema: str = None):
        """
        Initialize PostgreSQL client.

        Args:
            config: Database configuration
            schema: Schema name (defaults to config.pg_schema or 'hass')
        """
        self.config = config
        self.schema = schema or getattr(config, "pg_schema", "hass")
        self.pool: Optional[Pool] = None

    async def connect(
        self, min_size: Optional[int] = None, max_size: Optional[int] = None
    ) -> None:
        """
        Establish connection pool to PostgreSQL using configuration parameters.

        Args:
            min_size: Minimum pool size (overrides config)
            max_size: Maximum pool size (overrides config)
        """

        async def init_conn(conn: Connection):
            await conn.execute("SET timezone = 'UTC';")
            await conn.execute(f"SET search_path = '{self.schema}', 'public';")

        pool_min_size = min_size or self.config.pg_pool_minsize
        pool_max_size = max_size or self.config.pg_pool_maxsize

        self.pool = await asyncpg.create_pool(
            user=self.config.pg_user,
            password=self.config.pg_password,
            database=self.config.pg_db,
            host=self.config.pg_host,
            port=self.config.pg_port,
            min_size=pool_min_size,
            max_size=pool_max_size,
            init=init_conn,
        )

    async def close(self) -> None:
        """Close the connection pool."""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def begin_transaction(self) -> Connection:
        """
        Begin a new transaction.

        Returns:
            PostgreSQL connection with active transaction
        """
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        conn = await self.pool.acquire()
        await conn.execute("BEGIN")
        return conn

    async def commit_transaction(self, conn: Connection) -> None:
        """
        Commit transaction.

        Args:
            conn: Connection with active transaction
        """
        await conn.execute("COMMIT")
        await conn.close()

    async def rollback_transaction(self, conn: Connection) -> None:
        """
        Rollback transaction.

        Args:
            conn: Connection with active transaction
        """
        await conn.execute("ROLLBACK")
        await conn.close()

    async def count_rows(
        self, table: str, schema: str = None, conn: Connection = None
    ) -> int:
        """
        Count rows in a table.

        Args:
            table: Table name
            schema: Schema name
            conn: Optional connection (for use within transactions)

        Returns:
            Number of rows
        """
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        schema_name = schema or self.schema

        if conn:
            return await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema_name}"."{table}"'
            )
        else:
            async with self.pool.acquire() as conn:
                return await conn.fetchval(
                    f'SELECT COUNT(*) FROM "{schema_name}"."{table}"'
                )

    async def batch_insert_copy(
        self,
        table: str,
        columns: List[str],
        records: List[List[Any]],
        schema: str = None,
        conn: Connection = None,
    ) -> int:
        """
        Insert records using COPY FROM (fastest method).

        Args:
            table: Table name
            columns: Column names
            records: List of record lists
            schema: Schema name
            conn: Optional connection (for use within transactions)

        Returns:
            Number of inserted records
        """
        if not records:
            return 0

        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")

        schema_name = schema or self.schema

        if conn:
            try:
                await conn.copy_records_to_table(
                    table,
                    records=records,
                    columns=columns,
                    schema_name=schema_name,
                )
                return len(records)
            except Exception as e:
                # COPY might fail for certain data types, fall back to executemany
                raise RuntimeError(f"COPY failed: {e}") from e
        else:
            async with self.pool.acquire() as conn:
                try:
                    await conn.copy_records_to_table(
                        table,
                        records=records,
                        columns=columns,
                        schema_name=schema_name,
                    )
                    return len(records)
                except Exception as e:
                    # COPY might fail for certain data types, fall back to executemany
                    raise RuntimeError(f"COPY failed: {e}") from e

    async def batch_insert_executemany(
        self,
        table: str,
        columns: List[str],
        records: List[List[Any]],
        unique_constraints: Optional[List[List[str]]] = None,
        schema: str = None,
        conn: Connection = None,
    ) -> int:
        """
        Insert records using executemany (fallback method).

        Args:
            table: Table name
            columns: Column names
            records: List of record lists
            unique_constraints: Unique constraint columns for ON CONFLICT
            schema: Schema name
            conn: Optional connection (for use within transactions)

        Returns:
            Number of inserted records
        """
        if not records:
            return 0

        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")

        pg_columns = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))

        schema_name = schema or self.schema
        if unique_constraints:
            # Use first unique constraint for ON CONFLICT
            conflict_cols = unique_constraints[0]
            conflict_cols_qualified = ", ".join(f'"{col}"' for col in conflict_cols)
            insert_sql = f'INSERT INTO "{schema_name}"."{table}" ({pg_columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_cols_qualified}) DO NOTHING'
        else:
            insert_sql = f'INSERT INTO "{schema_name}"."{table}" ({pg_columns}) VALUES ({placeholders})'

        if conn:
            await conn.executemany(insert_sql, records)
            return len(records)
        else:
            async with self.pool.acquire() as conn:
                await conn.executemany(insert_sql, records)
                return len(records)

    async def truncate_table(self, table: str, schema: str = None) -> None:
        """
        Truncate a table.

        Args:
            table: Table name
            schema: Schema name
        """
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        schema_name = schema or self.schema
        async with self.pool.acquire() as conn:
            await conn.execute(
                f'TRUNCATE TABLE "{schema_name}"."{table}" RESTART IDENTITY CASCADE;'
            )

    async def fix_sequence(self, table: str, pk: str, schema: str = None) -> None:
        """
        Fix PostgreSQL sequence to match max primary key value.

        Args:
            table: Table name
            pk: Primary key column name
            schema: Schema name
        """
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        schema_name = schema or self.schema
        async with self.pool.acquire() as conn:
            seq = await conn.fetchval(
                "SELECT pg_get_serial_sequence($1, $2)", f"{schema_name}.{table}", pk
            )
            if not seq:
                return
            await conn.execute(
                f'SELECT setval($1, (SELECT COALESCE(MAX({pk}), 1) FROM "{schema_name}"."{table}"))',
                seq,
            )

    async def apply_schema(self, filename: str, force: bool = False) -> None:
        """
        Apply schema from SQL file.

        Args:
            filename: Path to SQL file
            force: Force recreate schema
        """
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")

        async with self.pool.acquire() as conn:
            if force:
                # Drop all tables in schema
                tables = await conn.fetch(
                    """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = $1
                """,
                    self.schema,
                )
                for table_row in tables:
                    table_name = table_row["tablename"]
                    await conn.execute(
                        f'DROP TABLE IF EXISTS "{self.schema}"."{table_name}" CASCADE;'
                    )

            # Read and execute schema file
            with open(filename, "r", encoding="utf-8") as f:
                sql = f.read()
            await conn.execute(sql)

    async def schema_exists(self) -> bool:
        """
        Check if schema exists.

        Returns:
            True if schema exists
        """
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                """
                SELECT 1
                FROM pg_namespace
                WHERE nspname = $1
                """,
                self.schema,
            )
        return exists is not None

    async def disable_foreign_keys(self) -> None:
        """Temporarily disable foreign key constraints (for migration performance)."""
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        # Note: PostgreSQL doesn't support disabling all FKs easily
        # This would need to be done per-constraint or by dropping/recreating
        # For now, we'll rely on DEFERRABLE constraints in schema

    async def enable_foreign_keys(self) -> None:
        """Re-enable foreign key constraints."""
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        # See disable_foreign_keys comment
