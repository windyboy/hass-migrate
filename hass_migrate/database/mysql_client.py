"""MySQL database client."""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

import mysql.connector

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
        self.connection: Optional[mysql.connector.MySQLConnection] = None

    def connect(self) -> None:
        """Establish connection to MySQL database."""
        self.connection = mysql.connector.connect(
            host=self.config.mysql_host,
            port=self.config.mysql_port,
            user=self.config.mysql_user,
            password=self.config.mysql_password,
            database=self.config.mysql_db,
            charset="utf8mb4",
        )

    def create_connection(self) -> mysql.connector.MySQLConnection:
        """Create a new MySQL connection for concurrent operations."""
        return mysql.connector.connect(
            host=self.config.mysql_host,
            port=self.config.mysql_port,
            user=self.config.mysql_user,
            password=self.config.mysql_password,
            database=self.config.mysql_db,
            charset="utf8mb4",
        )

    def close(self) -> None:
        """Close the connection."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def count_rows(self, table: str) -> int:
        """
        Count rows in a table.

        Args:
            table: Table name

        Returns:
            Number of rows
        """
        if self.connection is None:
            raise RuntimeError("MySQL connection not established")
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def fetch_batch(
        self,
        table: str,
        columns: List[str],
        batch_size: int,
        last_id: Optional[Any] = None,
        primary_key: str = None,
    ) -> Sequence[Sequence[Any]]:
        """
        Fetch a batch of rows from MySQL.

        Args:
            table: Table name
            columns: List of column names
            batch_size: Number of rows to fetch
            last_id: Last processed ID for resume
            primary_key: Primary key column name

        Returns:
            Sequence of row tuples
        """
        if self.connection is None:
            raise RuntimeError("MySQL connection not established")

        cursor = self.connection.cursor(buffered=True)
        try:
            col_str = ", ".join(columns)
            if last_id is not None and primary_key:
                cursor.execute(
                    f"SELECT {col_str} FROM {table} WHERE {primary_key} > %s ORDER BY {primary_key} LIMIT %s",
                    (last_id, batch_size),
                )
            else:
                cursor.execute(
                    f"SELECT {col_str} FROM {table} ORDER BY {primary_key} LIMIT %s",
                    (batch_size,),
                )
            return cursor.fetchall()
        finally:
            cursor.close()

    def create_cursor(self, buffered: bool = True):
        """
        Create a cursor for streaming queries.

        Args:
            buffered: Use buffered cursor

        Returns:
            MySQL cursor
        """
        if self.connection is None:
            raise RuntimeError("MySQL connection not established")
        return self.connection.cursor(buffered=buffered)

