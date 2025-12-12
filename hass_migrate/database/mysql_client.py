"""MySQL database client."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import aiomysql

from hass_migrate.config import DBConfig


class MySQLClient:
    """MySQL database client for reading data."""

    def __init__(self, config: DBConfig):
        """
        Initialize MySQL client.

        Args:
            config: Database configuration
        """
        self.config = config
        self.pool: Optional[aiomysql.Pool] = None

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote an identifier for use in SQL statements."""
        if "`" in identifier or "\x00" in identifier:
            raise ValueError(f"Invalid identifier: {identifier!r}")
        return f"`{identifier}`"

    async def connect(self) -> None:
        """Establish connection pool to MySQL database using configuration parameters."""
        self.pool = await aiomysql.create_pool(
            host=self.config.mysql_host,
            port=self.config.mysql_port,
            user=self.config.mysql_user,
            password=self.config.mysql_password,
            db=self.config.mysql_db,
            charset="utf8mb4",
            autocommit=True,
            minsize=self.config.mysql_pool_minsize,
            maxsize=self.config.mysql_pool_maxsize,
            connect_timeout=self.config.mysql_pool_timeout,
        )

    async def close(self) -> None:
        """Close the connection pool."""
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def create_connection(self) -> aiomysql.Connection:
        """
        Create a single connection from the pool for concurrent operations.

        Returns:
            MySQL connection object
        """
        if self.pool is None:
            raise RuntimeError("MySQL connection pool not established")
        return await self.pool.acquire()

    async def count_rows(self, table: str) -> int:
        """
        Count rows in a table.

        Args:
            table: Table name

        Returns:
            Number of rows
        """
        if self.pool is None:
            raise RuntimeError("MySQL connection pool not established")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                table_sql = self._quote_identifier(table)
                await cursor.execute(f"SELECT COUNT(*) FROM {table_sql}")
                return (await cursor.fetchone())[0]

    async def fetch_batch(
        self,
        table: str,
        columns: List[str],
        batch_size: int,
        last_id: Optional[Any] = None,
        primary_key: str = None,
    ) -> Sequence[Sequence[Any]]:
        """Fetch a batch of rows from MySQL with memory optimization.

        Args:
            table: Table name
            columns: List of column names
            batch_size: Number of rows to fetch
            last_id: Last processed ID for resume
            primary_key: Primary key column name

        Returns:
            Sequence of row tuples
        """
        if self.pool is None:
            raise RuntimeError("MySQL connection pool not established")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                table_sql = self._quote_identifier(table)
                columns_sql = ", ".join(self._quote_identifier(col) for col in columns)
                pk_sql = self._quote_identifier(primary_key) if primary_key else None

                if last_id is not None and pk_sql:
                    query = (
                        f"SELECT {columns_sql} FROM {table_sql} "
                        f"WHERE {pk_sql} > %s ORDER BY {pk_sql} LIMIT %s"
                    )
                    params = (last_id, batch_size)
                elif pk_sql:
                    query = (
                        f"SELECT {columns_sql} FROM {table_sql} "
                        f"ORDER BY {pk_sql} LIMIT %s"
                    )
                    params = (batch_size,)
                else:
                    query = f"SELECT {columns_sql} FROM {table_sql} LIMIT %s"
                    params = (batch_size,)

                await cursor.execute(query, params)

                # 使用fetchall()获取结果，但我们会在调用方立即处理和释放内存
                result = await cursor.fetchall()
                return result

    async def list_tables(self) -> List[str]:
        """List all tables available in the current database."""
        if self.pool is None:
            raise RuntimeError("MySQL connection pool not established")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SHOW TABLES")
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def get_table_columns(self, table: str) -> List[Dict[str, Any]]:
        """Retrieve column metadata for the provided table."""
        if self.pool is None:
            raise RuntimeError("MySQL connection pool not established")
        table_sql = self._quote_identifier(table)
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(f"SHOW COLUMNS FROM {table_sql}")
                columns = await cursor.fetchall()
                return list(columns)

    async def fetch_batch_with_resume(
        self,
        table: str,
        columns: List[str],
        batch_size: int,
        last_id: Optional[Any] = None,
        primary_key: str = None,
    ) -> tuple[Sequence[Sequence[Any]], Optional[Any]]:
        """
        Fetch a batch of rows with resume support.

        Args:
            table: Table name
            columns: List of column names
            batch_size: Number of rows to fetch
            last_id: Last processed ID for resume
            primary_key: Primary key column name

        Returns:
            Tuple of (rows, new_last_id)
        """
        rows = await self.fetch_batch(table, columns, batch_size, last_id, primary_key)
        if rows and primary_key:
            try:
                pk_index = columns.index(primary_key)
            except ValueError:
                pk_index = 0
            new_last_id = rows[-1][pk_index]
        else:
            new_last_id = None
        return rows, new_last_id
