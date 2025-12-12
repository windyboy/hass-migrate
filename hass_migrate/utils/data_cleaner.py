"""Data cleaning utilities for migration."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any

from hass_migrate.utils.logger import StructuredLogger

# Initialize logger
logger = StructuredLogger("data_cleaner")

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

    Args:
        table: Table name
        column: Column name
        value: Value to clean

    Returns:
        Cleaned value compatible with PostgreSQL
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
            logger.warning(
                f"{table}.{column} has non-boolean integer {value!r}, keeping as-is"
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
            return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(
                tzinfo=None
            )
        except (ValueError, OSError, OverflowError) as e:
            logger.warning(
                f"{table}.{column} has invalid timestamp {value!r}: {e}, keeping as-is"
            )
            return value

    # 6. Other types (int, float, bytes, etc.) returned as-is
    return value


def clean_batch_values(
    table: str, columns: list[str], rows: list[tuple[Any, ...]]
) -> list[list[Any]]:
    """
    Clean a batch of rows efficiently.

    Args:
        table: Table name
        columns: List of column names
        rows: List of row tuples from MySQL

    Returns:
        List of cleaned row lists
    """
    if not rows:
        return []

    # Pre-compute column validation to avoid repeated checks
    expected_cols = len(columns)
    cleaned_batch = []

    for row in rows:
        if len(row) != expected_cols:
            logger.warning(
                f"{table} row has {len(row)} columns, expected {expected_cols}. Skipping row."
            )
            continue

        # Use list comprehension for better performance
        cleaned_row = [
            clean_value(table, col, val) for col, val in zip(columns, row)
        ]
        cleaned_batch.append(cleaned_row)

    return cleaned_batch


def clean_row(table: str, row: dict[str, Any]) -> dict[str, Any]:
    """
    Clean a single row dict.

    Args:
        table: Table name
        row: Row as dict

    Returns:
        Cleaned row dict
    """
    return {col: clean_value(table, col, val) for col, val in row.items()}

