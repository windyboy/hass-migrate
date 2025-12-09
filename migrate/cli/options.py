from __future__ import annotations

from typing import List, Optional, Tuple

import typer

from migrate.config import DBConfig
from migrate.cli.constants import TABLES


def get_schema_option() -> typer.Option:
    """Get schema option factory."""
    return typer.Option(
        None, "--schema", help="PostgreSQL schema name (default: PG_SCHEMA env var or 'hass')"
    )


def get_batch_size_option() -> typer.Option:
    """Get batch size option factory."""
    return typer.Option(
        20000, "--batch-size", help="Number of rows per batch (default: 20000)"
    )


def get_force_option() -> typer.Option:
    """Get force option factory."""
    return typer.Option(
        False, "--force", "-f", help="Skip confirmation prompts and truncate tables"
    )


def validate_batch_size(value: int) -> int:
    """Validate batch size is positive."""
    if value <= 0:
        raise typer.BadParameter("--batch-size must be greater than 0")
    return value


def validate_max_concurrent(value: int) -> int:
    """Validate max concurrent is positive."""
    if value <= 0:
        raise typer.BadParameter("--max-concurrent must be greater than 0")
    return value


def get_table_info(table_name: str) -> Tuple[str, List[str]]:
    """Get table column information.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Tuple of (table_name, column_list)
        
    Raises:
        typer.BadParameter: If table not found
    """
    table_info = [t for t in TABLES if t[0] == table_name]
    if not table_info:
        raise typer.BadParameter(
            f"Table '{table_name}' not found. Use 'tables' command to list available tables."
        )
    return table_info[0]


def get_schema_name(cfg: DBConfig, schema: Optional[str]) -> str:
    """Get schema name from option or config."""
    return schema or getattr(cfg, 'pg_schema', 'hass')

