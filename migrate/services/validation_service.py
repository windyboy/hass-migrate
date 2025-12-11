"""Validation service for data integrity checks."""

from __future__ import annotations

from typing import List

from migrate.database.mysql_client import MySQLClient
from migrate.database.pg_client import PGClient
from migrate.models.table_metadata import ValidationResult
from migrate.utils.logger import StructuredLogger


class ValidationService:
    """Service for validating migration data integrity between MySQL and PostgreSQL."""

    def __init__(
        self,
        mysql_client: MySQLClient,
        pg_client: PGClient,
        logger: StructuredLogger,
    ):
        """
        Initialize validation service.

        Args:
            mysql_client: Async MySQL client instance
            pg_client: Async PostgreSQL client instance
            logger: Structured logger for validation events
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
        mysql_count = await self.mysql_client.count_rows(table)
        pg_count = await self.pg_client.count_rows(table)

        row_count_match = mysql_count == pg_count

        # 2. Sample comparison (placeholder for future implementation)
        # TODO: Implement actual row data sampling and comparison
        sample_match = True  # Placeholder - currently always true

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
