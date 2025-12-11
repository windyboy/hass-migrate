"""Validation service for data integrity checks."""

from __future__ import annotations

from typing import List

from hass_migrate.database.mysql_client import MySQLClient
from hass_migrate.database.pg_client import PGClient
from hass_migrate.cli.constants import TABLE_PK
from hass_migrate.models.table_metadata import ValidationResult
from hass_migrate.utils.data_cleaner import clean_row
from hass_migrate.utils.logger import StructuredLogger


class ValidationService:
    """Service for validating migration data integrity."""

    def __init__(
        self,
        mysql_client: MySQLClient,
        pg_client: PGClient,
        logger: StructuredLogger,
    ):
        """
        Initialize validation service.

        Args:
            mysql_client: MySQL client
            pg_client: PostgreSQL client
            logger: Logger instance
        """
        self.mysql_client = mysql_client
        self.pg_client = pg_client
        self.logger = logger

    async def validate_table(
        self, table: str, sample_size: int = 1000
    ) -> ValidationResult:
        """
        Validate migration for a single table.

        Args:
            table: Table name
            sample_size: Number of rows to sample for comparison

        Returns:
            Validation result
        """
        # 1. Row count comparison
        mysql_count = self.mysql_client.count_rows(table)
        pg_count = await self.pg_client.count_rows(table)

        row_count_match = mysql_count == pg_count

        # 2. Sample comparison (optional, for large tables)
        sample_match = True
        if row_count_match and mysql_count > 0:
            # Sample some rows and compare
            # This is a simplified version - could be enhanced with checksums
            sample_match = await self._sample_compare(table, sample_size, TABLE_PK.get(table, 'id'))

        return ValidationResult(
            table=table,
            row_count_match=row_count_match,
            mysql_count=mysql_count,
            pg_count=pg_count,
            sample_match=sample_match,
        )

    async def validate_all_tables(
        self, table_names: List[str], sample_size: int = 1000
    ) -> List[ValidationResult]:
        """
        Validate all tables.

        Args:
            table_names: List of table names
            sample_size: Number of rows to sample

        Returns:
            List of validation results
        """
        results: List[ValidationResult] = []
        for table in table_names:
            try:
                result = await self.validate_table(table, sample_size)
                results.append(result)
                if result.all_match:
                    self.logger.info(
                        f"Validation passed: {table}",
                        mysql_count=result.mysql_count,
                        pg_count=result.pg_count,
                    )
                else:
                    self.logger.error(
                        f"Validation failed: {table}",
                        mysql_count=result.mysql_count,
                        pg_count=result.pg_count,
                    )
            except Exception as e:
                self.logger.error(f"Validation error for {table}: {e}")
                results.append(
                    ValidationResult(
                        table=table,
                        row_count_match=False,
                        mysql_count=0,
                        pg_count=0,
                        errors=[str(e)],
                    )
                )

        return results

    async def _sample_compare(self, table: str, sample_size: int, pk_column: str) -> bool:
        """
        Compare a sample of rows between MySQL and PostgreSQL.

        Args:
            table: Table name
            sample_size: Number of rows to sample
            pk_column: Primary key column name

        Returns:
            True if samples match
        """
        try:
            # Fetch sample from MySQL
            mysql_conn = self.mysql_client.create_connection()
            mysql_cursor = mysql_conn.cursor(dictionary=True)
            mysql_cursor.execute(f"SELECT * FROM {table} ORDER BY {pk_column} LIMIT %s", (sample_size,))
            mysql_rows = mysql_cursor.fetchall()
            mysql_cursor.close()
            mysql_conn.close()

            # Fetch sample from PostgreSQL
            async with self.pg_client.pool.acquire() as pg_conn:
                pg_rows = await pg_conn.fetch(f"SELECT * FROM {table} ORDER BY {pk_column} LIMIT $1", sample_size)
            
            if not pg_rows:
                return not mysql_rows  # both empty

            # Convert PG rows to dicts
            pg_rows_dict = [dict(row) for row in pg_rows]

            # Clean and compare
            cleaned_mysql = [clean_row(table, dict(row)) for row in mysql_rows]
            cleaned_pg = [clean_row(table, row) for row in pg_rows_dict]

            return cleaned_mysql == cleaned_pg
        except Exception as e:
            self.logger.error(f"Sample comparison failed for {table}: {e}")
            return False

