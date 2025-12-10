"""PostgreSQL database client."""

from __future__ import annotations

from typing import Any, List, Optional

import asyncpg
from asyncpg import Connection, Pool

from hass_migrate.config import DBConfig


class PGClient:
    """PostgreSQL database client for writing data."""

    def __init__(self, config: DBConfig, schema: str | None = None) -> None:
        """
        Initialize PostgreSQL client.

        Args:
            config: Database configuration
            schema: Schema name (defaults to config.pg_schema or 'public')
        """
        self.config = config
        self.schema = schema or getattr(config, "pg_schema", "public")
        self.pool: Optional[Pool] = None

    async def connect(self, min_size: int = 2, max_size: int = 10) -> None:
        """
        Establish connection pool to PostgreSQL.

        Args:
            min_size: Minimum pool size
            max_size: Maximum pool size
        """

        async def init_conn(conn: Connection) -> None:
            await conn.execute("SET timezone = 'UTC';")
            await conn.execute(f"SET search_path = '{self.schema}', 'public';")

        self.pool = await asyncpg.create_pool(
            user=self.config.pg_user,
            password=self.config.pg_password,
            database=self.config.pg_db,
            host=self.config.pg_host,
            port=self.config.pg_port,
            min_size=min_size,
            max_size=max_size,
            init=init_conn,
        )

    async def close(self) -> None:
        """Close the connection pool."""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    @staticmethod
    def _split_sql_statements(sql: str) -> List[str]:
        """
        Split a SQL script into executable statements, respecting strings and comments.
        """
        statements: List[str] = []
        buffer: List[str] = []

        in_single_quote = False
        in_double_quote = False
        dollar_quote_tag: str | None = None

        length = len(sql)
        i = 0

        while i < length:
            if dollar_quote_tag is not None:
                if sql.startswith(dollar_quote_tag, i):
                    buffer.append(dollar_quote_tag)
                    i += len(dollar_quote_tag)
                    dollar_quote_tag = None
                else:
                    buffer.append(sql[i])
                    i += 1
                continue

            if in_single_quote:
                buffer.append(sql[i])
                if sql[i] == "'" and i + 1 < length and sql[i + 1] == "'":
                    buffer.append("'")
                    i += 2
                elif sql[i] == "'":
                    in_single_quote = False
                    i += 1
                else:
                    i += 1
                continue

            if in_double_quote:
                buffer.append(sql[i])
                if sql[i] == '"' and i + 1 < length and sql[i + 1] == '"':
                    buffer.append('"')
                    i += 2
                elif sql[i] == '"':
                    in_double_quote = False
                    i += 1
                else:
                    i += 1
                continue

            if sql.startswith("--", i):
                newline = sql.find("\n", i)
                if newline == -1:
                    break
                i = newline + 1
                continue

            if sql.startswith("/*", i):
                end_comment = sql.find("*/", i + 2)
                if end_comment == -1:
                    break
                i = end_comment + 2
                continue

            char = sql[i]

            if char == "'":
                in_single_quote = True
                buffer.append(char)
                i += 1
                continue

            if char == '"':
                in_double_quote = True
                buffer.append(char)
                i += 1
                continue

            if char == "$":
                tag_end = i + 1
                while tag_end < length and (
                    sql[tag_end].isalnum() or sql[tag_end] == "_"
                ):
                    tag_end += 1
                if tag_end < length and sql[tag_end] == "$":
                    dollar_quote_tag = sql[i : tag_end + 1]
                    buffer.append(dollar_quote_tag)
                    i = tag_end + 1
                    continue
                buffer.append(char)
                i += 1
                continue

            if char == ";":
                statement = "".join(buffer).strip()
                if statement:
                    statements.append(statement)
                buffer = []
                i += 1
                continue

            buffer.append(char)
            i += 1

        tail = "".join(buffer).strip()
        if tail:
            statements.append(tail)

        return statements

    async def count_rows(self, table: str, schema: str | None = None) -> int:
        """
        Count rows in a table.

        Args:
            table: Table name
            schema: Schema name

        Returns:
            Number of rows
        """
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        schema_name = schema or self.schema
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema_name}"."{table}"'
            )

    async def batch_insert_copy(
        self,
        table: str,
        columns: List[str],
        records: List[List[Any]],
        schema: str | None = None,
    ) -> int:
        """
        Insert records using COPY FROM (fastest method).

        Args:
            table: Table name
            columns: Column names
            records: List of record lists
            schema: Schema name

        Returns:
            Number of inserted records
        """
        if not records:
            return 0

        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")

        schema_name = schema or self.schema
        async with self.pool.acquire() as conn:
            try:
                await conn.copy_records_to_table(
                    table,
                    records=records,
                    columns=columns,
                    schema_name=schema_name,
                )
                return len(records)
            except Exception as exc:
                raise RuntimeError(f"COPY failed: {exc}") from exc

    async def batch_insert_executemany(
        self,
        table: str,
        columns: List[str],
        records: List[List[Any]],
        unique_constraints: Optional[List[List[str]]] = None,
        schema: str | None = None,
    ) -> int:
        """
        Insert records using executemany (fallback method).

        Args:
            table: Table name
            columns: Column names
            records: List of record lists
            unique_constraints: Unique constraint columns for ON CONFLICT
            schema: Schema name

        Returns:
            Number of inserted records
        """
        if not records:
            return 0

        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")

        pg_columns = ", ".join(f'"{column}"' for column in columns)
        placeholders = ", ".join(f"${index + 1}" for index in range(len(columns)))
        schema_name = schema or self.schema

        if unique_constraints:
            conflict_cols = ", ".join(f'"{col}"' for col in unique_constraints[0])
            insert_sql = (
                f'INSERT INTO "{schema_name}"."{table}" ({pg_columns}) '
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO NOTHING"
            )
        else:
            insert_sql = f'INSERT INTO "{schema_name}"."{table}" ({pg_columns}) VALUES ({placeholders})'

        async with self.pool.acquire() as conn:
            await conn.executemany(insert_sql, records)
            return len(records)

    async def truncate_table(self, table: str, schema: str | None = None) -> None:
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

    async def fix_sequence(
        self, table: str, pk: str, schema: str | None = None
    ) -> None:
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
                "SELECT pg_get_serial_sequence($1, $2)",
                f"{schema_name}.{table}",
                pk,
            )
            if not seq:
                return

            max_value = await conn.fetchval(
                f'SELECT MAX("{pk}") FROM "{schema_name}"."{table}"'
            )
            if max_value is None:
                await conn.execute("SELECT setval($1, 1, false)", seq)
            else:
                await conn.execute("SELECT setval($1, $2, true)", seq, max_value)

    async def apply_schema(self, filename: str, force: bool = False) -> None:
        """
        Apply schema from SQL file.

        Args:
            filename: Path to SQL file
            force: Force recreate schema
        """
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")

        with open(filename, "r", encoding="utf-8") as file_obj:
            sql = file_obj.read()

        statements = self._split_sql_statements(sql)
        if not statements:
            return

        async with self.pool.acquire() as conn:
            if force:
                tables = await conn.fetch(
                    """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = $1
                    """,
                    self.schema,
                )
                for row in tables:
                    table_name = row["tablename"]
                    await conn.execute(
                        f'DROP TABLE IF EXISTS "{self.schema}"."{table_name}" CASCADE;'
                    )

            async with conn.transaction():
                for statement in statements:
                    await conn.execute(statement)

    async def schema_exists(self) -> bool:
        """
        Check if schema exists.

        Returns:
            True if schema exists
        """
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        async with self.pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM pg_tables
                WHERE schemaname = $1
                """,
                self.schema,
            )
        return count > 0

    async def disable_foreign_keys(self) -> None:
        """Temporarily disable foreign key constraints (for migration performance)."""
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        # PostgreSQL requires constraint-specific handling; rely on deferred constraints.

    async def enable_foreign_keys(self) -> None:
        """Re-enable foreign key constraints."""
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool not established")
        # See disable_foreign_keys note.
