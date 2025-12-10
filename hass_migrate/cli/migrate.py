import asyncio
import json
import os
import time

import typer

from hass_migrate.config import DBConfig
from hass_migrate.database.mysql_client import MySQLClient
from hass_migrate.database.pg_client import PGClient
from hass_migrate.models.table_metadata import MigrationConfig
from hass_migrate.services.backup_service import BackupService
from hass_migrate.services.migration_service import MigrationService
from hass_migrate.utils.dependency import DependencyAnalyzer
from hass_migrate.cli.constants import TABLES, PROGRESS_FILE, console, logger
from hass_migrate.cli.options import (
    get_force_option,
    get_batch_size_option,
    validate_batch_size,
    validate_max_concurrent,
    get_schema_option,
    get_schema_name,
    get_table_info,
)
from hass_migrate.cli.schema import ensure_schema


def register_migrate_commands(migrate_app: typer.Typer) -> None:
    """Register migration commands."""
    
    @migrate_app.command("table")
    def migrate_table(
        name: str = typer.Argument(..., help="Table name to migrate"),
        force: bool = get_force_option(),
        batch_size: int = typer.Option(
            20000, "--batch-size", callback=validate_batch_size, help="Number of rows per batch (default: 20000)"
        ),
        schema: str = get_schema_option(),
    ):
        """Migrate a single table."""
        try:
            table_name, cols = get_table_info(name)
        except typer.BadParameter as e:
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)
        
        cfg = DBConfig()
        mysql_client = MySQLClient(cfg)
        mysql_client.connect()

        async def _run():
            schema_name = get_schema_name(cfg, schema)
            pg_client = PGClient(cfg, schema=schema_name)
            try:
                await pg_client.connect()
                await ensure_schema(pg_client, force=force)

                if not force:
                    if not typer.confirm(f"Truncate target table '{table_name}' before migration?"):
                        console.print("[yellow]Cancelled.[/yellow]")
                        return

                await pg_client.truncate_table(table_name)
                console.rule(f"[cyan]Migrating {table_name}[/cyan]")

                dependency_analyzer = DependencyAnalyzer()
                migration_service = MigrationService(
                    mysql_client, pg_client, dependency_analyzer, logger
                )
                config = MigrationConfig(batch_size=batch_size, schema=schema_name)
                result = await migration_service.migrate_table(table_name, cols, config)

                await pg_client.fix_sequence(table_name, cols[0])

                if result.success:
                    console.print(
                        f"[bold green]Done: {table_name} - {result.rows_migrated:,} rows in {result.duration:.2f}s[/bold green]"
                    )
                else:
                    console.print(f"[bold red]Done with errors: {table_name}[/bold red]")
                    raise typer.Exit(1)
            finally:
                mysql_client.close()
                await pg_client.close()

        asyncio.run(_run())

    @migrate_app.command("all")
    def migrate_all(
        force: bool = get_force_option(),
        batch_size: int = typer.Option(
            20000, "--batch-size", callback=validate_batch_size, help="Number of rows per batch (default: 20000)"
        ),
        max_concurrent: int = typer.Option(
            4, "--max-concurrent", callback=validate_max_concurrent, help="Maximum number of tables to migrate concurrently (default: 4)"
        ),
        backup: bool = typer.Option(
            False, "--backup", help="Create backup before migration (ignored if --resume is used)"
        ),
        schema: str = get_schema_option(),
    ):
        """Perform a complete migration of all Home Assistant recorder tables.
        
        This command orchestrates the full migration process:
        1. Creates PostgreSQL schema if it doesn't exist
        2. Truncates target tables
        3. Migrates all tables in dependency order
        4. Fixes PostgreSQL sequences
        5. Validates data integrity
        
        Use --backup to create a backup before starting migration.
        """
        cfg = DBConfig()
        mysql_client = MySQLClient(cfg)
        mysql_client.connect()

        async def _run():
            start_time = time.time()
            schema_name = get_schema_name(cfg, schema)
            pg_client = PGClient(cfg, schema=schema_name)
            backup_service = BackupService(logger)

            try:
                await pg_client.connect()
                await ensure_schema(pg_client, force=force)

                if not force:
                    if not typer.confirm("Truncate ALL tables before migration?"):
                        console.print("[yellow]Cancelled.[/yellow]")
                        return

                # Create backup if requested
                backup_path = None
                if backup:
                    try:
                        backup_path = await backup_service.create_backup(cfg)
                        console.print(f"[green]Backup created: {backup_path}[/green]")
                    except Exception as e:
                        console.print(f"[yellow]Backup failed: {e}, continuing...[/yellow]")

                # Initialize services
                dependency_analyzer = DependencyAnalyzer()
                migration_service = MigrationService(
                    mysql_client, pg_client, dependency_analyzer, logger
                )

                # Migration config
                config = MigrationConfig(
                    batch_size=batch_size,
                    max_concurrent_tables=max_concurrent,
                    schema=schema_name,
                )

                # Truncate tables
                console.print("[yellow]Truncating tables...[/yellow]")
                for table, _ in TABLES:
                    await pg_client.truncate_table(table)

                # Migrate all tables
                console.rule("[bold cyan]Starting Migration[/bold cyan]")
                results = await migration_service.migrate_all_tables(TABLES, config)

                # Fix sequences
                console.print("[yellow]Fixing sequences...[/yellow]")
                for table, cols in TABLES:
                    await pg_client.fix_sequence(table, cols[0])

                # Save final progress
                final_progress = migration_service.get_progress()
                with open(PROGRESS_FILE, "w") as f:
                    json.dump(final_progress, f, indent=2)

                # Clean up progress file if migration successful
                all_success = all(r.success for r in results)
                if all_success and os.path.exists(PROGRESS_FILE):
                    os.remove(PROGRESS_FILE)

                # Display results
                console.rule("[bold cyan]Migration Results[/bold cyan]")
                for result in results:
                    if result.success:
                        console.print(
                            f"[green]✓ {result.table}: {result.rows_migrated:,} rows in {result.duration:.2f}s[/green]"
                        )
                    else:
                        console.print(
                            f"[red]✗ {result.table}: {result.rows_migrated:,} rows with errors[/red]"
                        )
                        for error in result.errors:
                            console.print(f"  [red]  - {error}[/red]")

                duration = time.time() - start_time
                if all_success:
                    console.rule(
                        f"[bold green]Migration completed successfully in {duration:.2f}s![/bold green]"
                    )
                    raise typer.Exit(0)
                else:
                    console.rule(
                        f"[bold yellow]Migration completed with errors in {duration:.2f}s[/bold yellow]"
                    )
                    raise typer.Exit(1)

            finally:
                mysql_client.close()
                await pg_client.close()

        asyncio.run(_run())

    @migrate_app.command("resume")
    def migrate_resume(
        batch_size: int = typer.Option(
            20000, "--batch-size", callback=validate_batch_size, help="Number of rows per batch (default: 20000)"
        ),
        max_concurrent: int = typer.Option(
            4, "--max-concurrent", callback=validate_max_concurrent, help="Maximum number of tables to migrate concurrently (default: 4)"
        ),
        schema: str = get_schema_option(),
    ):
        """Resume interrupted migration."""
        if not os.path.exists(PROGRESS_FILE):
            console.print("[red]Error: No progress file found. Cannot resume migration.[/red]")
            raise typer.Exit(1)
        
        cfg = DBConfig()
        mysql_client = MySQLClient(cfg)
        mysql_client.connect()

        async def _run():
            start_time = time.time()
            schema_name = get_schema_name(cfg, schema)
            pg_client = PGClient(cfg, schema=schema_name)

            try:
                await pg_client.connect()
                await ensure_schema(pg_client, force=False)

                # Load progress
                with open(PROGRESS_FILE, "r") as f:
                    progress_data = json.load(f)

                if not progress_data:
                    console.print("[yellow]Progress file is empty. Starting fresh migration.[/yellow]")
                    progress_data = {}

                # Initialize services
                dependency_analyzer = DependencyAnalyzer()
                migration_service = MigrationService(
                    mysql_client, pg_client, dependency_analyzer, logger
                )
                migration_service.load_progress(progress_data)

                # Migration config
                config = MigrationConfig(
                    batch_size=batch_size,
                    max_concurrent_tables=max_concurrent,
                    schema=schema_name,
                )

                # Migrate all tables (service will skip already migrated ones based on progress)
                console.rule("[bold cyan]Resuming Migration[/bold cyan]")
                results = await migration_service.migrate_all_tables(TABLES, config)

                # Fix sequences
                console.print("[yellow]Fixing sequences...[/yellow]")
                for table, cols in TABLES:
                    await pg_client.fix_sequence(table, cols[0])

                # Save final progress
                final_progress = migration_service.get_progress()
                with open(PROGRESS_FILE, "w") as f:
                    json.dump(final_progress, f, indent=2)

                # Clean up progress file if migration successful
                all_success = all(r.success for r in results)
                if all_success and os.path.exists(PROGRESS_FILE):
                    os.remove(PROGRESS_FILE)

                # Display results
                console.rule("[bold cyan]Migration Results[/bold cyan]")
                for result in results:
                    if result.success:
                        console.print(
                            f"[green]✓ {result.table}: {result.rows_migrated:,} rows in {result.duration:.2f}s[/green]"
                        )
                    else:
                        console.print(
                            f"[red]✗ {result.table}: {result.rows_migrated:,} rows with errors[/red]"
                        )
                        for error in result.errors:
                            console.print(f"  [red]  - {error}[/red]")

                duration = time.time() - start_time
                if all_success:
                    console.rule(
                        f"[bold green]Migration completed successfully in {duration:.2f}s![/bold green]"
                    )
                    raise typer.Exit(0)
                else:
                    console.rule(
                        f"[bold yellow]Migration completed with errors in {duration:.2f}s[/bold yellow]"
                    )
                    raise typer.Exit(1)

            finally:
                mysql_client.close()
                await pg_client.close()

        asyncio.run(_run())

