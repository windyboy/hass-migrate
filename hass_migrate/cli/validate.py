from __future__ import annotations

import asyncio
from typing import Optional

import typer

from hass_migrate.config import DBConfig
from hass_migrate.database.mysql_client import MySQLClient
from hass_migrate.database.pg_client import PGClient
from hass_migrate.services.validation_service import ValidationService
from hass_migrate.cli.constants import TABLES, console, logger
from hass_migrate.cli.options import get_schema_option, get_schema_name, get_table_info


def _validate_all_impl(schema: Optional[str] = None):
    """Internal implementation of validate all."""
    cfg = DBConfig()
    mysql_client = MySQLClient(cfg)

    async def _run():
        schema_name = get_schema_name(cfg, schema)
        pg_client = PGClient(cfg, schema=schema_name)
        await pg_client.connect()
        await mysql_client.connect()
        console.rule("[bold cyan]VALIDATION[/bold cyan]")
        try:
            validation_service = ValidationService(mysql_client, pg_client, logger)
            table_names = [t[0] for t in TABLES]
            results = await validation_service.validate_all_tables(table_names)

            all_ok = True
            for result in results:
                if result.all_match:
                    console.print(
                        f"[green]✓ {result.table}: {result.mysql_count:,} rows[/green]"
                    )
                else:
                    console.print(
                        f"[red]✗ {result.table}: MySQL={result.mysql_count:,} PostgreSQL={result.pg_count:,}[/red]"
                    )
                    all_ok = False

            if all_ok:
                console.print("[bold green]All tables match![/bold green]")
                raise typer.Exit(0)
            else:
                console.print("[bold red]Validation failed: row counts mismatch[/bold red]")
                raise typer.Exit(1)
        finally:
            await mysql_client.close()
            await pg_client.close()

    asyncio.run(_run())


def register_validate_commands(validate_app: typer.Typer) -> None:
    """Register validation commands."""
    
    @validate_app.callback(invoke_without_command=True)
    def validate_callback(
        ctx: typer.Context,
        schema: str = get_schema_option(),
    ):
        """Validate all tables by comparing row counts (default behavior).
        
        Compares row counts for all migrated tables between source (MySQL) and target
        (PostgreSQL) databases. Use this command after migration to verify data integrity.
        
        Exits with error code 1 if any table has a row count mismatch.
        """
        if ctx.invoked_subcommand is None:
            # No subcommand specified, run validate_all
            _validate_all_impl(schema=schema)

    @validate_app.command("table")
    def validate_table(
        name: str = typer.Argument(..., help="Table name to validate"),
        schema: str = get_schema_option(),
    ):
        """Validate a single table by comparing row counts."""
        try:
            table_name, _ = get_table_info(name)
        except typer.BadParameter as e:
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)
        
        cfg = DBConfig()
        mysql_client = MySQLClient(cfg)

        async def _run():
            schema_name = get_schema_name(cfg, schema)
            pg_client = PGClient(cfg, schema=schema_name)
            await pg_client.connect()
            await mysql_client.connect()
            console.rule(f"[bold cyan]VALIDATION: {table_name}[/bold cyan]")
            try:
                validation_service = ValidationService(mysql_client, pg_client, logger)
                result = await validation_service.validate_table(table_name)

                if result.all_match:
                    console.print(
                        f"[green]✓ {result.table}: {result.mysql_count:,} rows match[/green]"
                    )
                else:
                    console.print(
                        f"[red]✗ {result.table}: MySQL={result.mysql_count:,} PostgreSQL={result.pg_count:,}[/red]"
                    )
                    raise typer.Exit(1)
            finally:
                await mysql_client.close()
                await pg_client.close()

        asyncio.run(_run())

