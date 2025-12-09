import asyncio
import json
import os
import sys
import time
from typing import List, Optional, Tuple

import typer
from rich.console import Console
from rich.progress import Progress, TaskID
from rich.table import Table

from migrate.config import DBConfig
from migrate.database.mysql_client import MySQLClient
from migrate.database.pg_client import PGClient
from migrate.models.table_metadata import MigrationConfig
from migrate.services.backup_service import BackupService
from migrate.services.migration_service import MigrationService
from migrate.services.validation_service import ValidationService
from migrate.utils.dependency import DependencyAnalyzer
from migrate.utils.logger import StructuredLogger

app = typer.Typer(help="Home Assistant MySQL → PostgreSQL migration tool")
console = Console()

# Path to schema file relative to this file's directory
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema", "postgres_schema.sql")
PROGRESS_FILE = "migration_progress.json"

# Initialize logger
logger = StructuredLogger("migrate")

# Create sub-command groups
schema_app = typer.Typer(help="Manage PostgreSQL schema")
app.add_typer(schema_app, name="schema")

migrate_app = typer.Typer(help="Migrate data from MySQL to PostgreSQL")
app.add_typer(migrate_app, name="migrate")

validate_app = typer.Typer(help="Validate migration integrity", invoke_without_command=True)
app.add_typer(validate_app, name="validate")


# ============================================================================
# Common Options and Validation Functions
# ============================================================================

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

# 注意：这里每个 tuple 是 (表名, [字段列表])
TABLES = [
    # 先迁 event 相关基础表
    ("event_types", ["event_type_id", "event_type"]),
    ("event_data", ["data_id", "hash", "shared_data"]),
    (
        "events",
        [
            "event_id",
            "event_type",
            "event_data",
            "origin",
            "origin_idx",
            "time_fired",
            "time_fired_ts",
            "context_id",
            "context_user_id",
            "context_parent_id",
            "data_id",
            "context_id_bin",
            "context_user_id_bin",
            "context_parent_id_bin",
            "event_type_id",
        ],
    ),
    # states 相关
    ("state_attributes", ["attributes_id", "hash", "shared_attrs"]),
    ("states_meta", ["metadata_id", "entity_id"]),
    (
        "states",
        [
            "state_id",
            "entity_id",
            "state",
            "attributes",
            "event_id",
            "last_changed",
            "last_changed_ts",
            "last_reported_ts",
            "last_updated",
            "last_updated_ts",
            "old_state_id",
            "attributes_id",
            "context_id",
            "context_user_id",
            "context_parent_id",
            "origin_idx",
            "context_id_bin",
            "context_user_id_bin",
            "context_parent_id_bin",
            "metadata_id",
        ],
    ),
    # 统计相关
    (
        "statistics_meta",
        [
            "id",
            "statistic_id",
            "source",
            "unit_of_measurement",
            "unit_class",
            "has_mean",
            "has_sum",
            "name",
            "mean_type",
        ],
    ),
    (
        "statistics",
        [
            "id",
            "created",
            "created_ts",
            "metadata_id",
            "start",
            "start_ts",
            "mean",
            "mean_weight",
            "min",
            "max",
            "last_reset",
            "last_reset_ts",
            "state",
            "sum",
        ],
    ),
    (
        "statistics_short_term",
        [
            "id",
            "created",
            "created_ts",
            "metadata_id",
            "start",
            "start_ts",
            "mean",
            "mean_weight",
            "min",
            "max",
            "last_reset",
            "last_reset_ts",
            "state",
            "sum",
        ],
    ),
    # runs / schema / migration
    ("recorder_runs", ["run_id", "start", "end", "closed_incorrect", "created"]),
    ("statistics_runs", ["run_id", "start"]),
    ("schema_changes", ["change_id", "schema_version", "changed"]),
    ("migration_changes", ["migration_id", "version"]),
]


async def ensure_schema(pg_client: PGClient, force: bool = False):
    """Ensure PostgreSQL schema exists."""
    if not os.path.exists(SCHEMA_FILE):
        console.print(f"[red]Missing schema file: {SCHEMA_FILE}[/red]")
        raise typer.Exit(1)

    exists = await pg_client.schema_exists()
    if force:
        console.print("[yellow]Force mode: dropping and recreating schema...[/yellow]")
        await pg_client.apply_schema(SCHEMA_FILE, force=True)
        console.print("[green]Schema recreated successfully[/green]")
    elif not exists:
        console.print("[yellow]Schema missing → applying schema.sql...[/yellow]")
        await pg_client.apply_schema(SCHEMA_FILE, force=False)
        console.print("[green]Schema applied successfully[/green]")
    else:
        console.print("[cyan]Schema exists in PostgreSQL[/cyan]")


# ============================================================================
# Schema Management Commands
# ============================================================================

@schema_app.command("apply")
def schema_apply(
    force: bool = get_force_option(),
    schema: str = get_schema_option(),
):
    """Apply PostgreSQL schema."""
    cfg = DBConfig()
    schema_name = get_schema_name(cfg, schema)
    pg_client = PGClient(cfg, schema=schema_name)

    async def _run():
        try:
            await pg_client.connect()
            await ensure_schema(pg_client, force=force)
        finally:
            await pg_client.close()

    asyncio.run(_run())


@schema_app.command("drop")
def schema_drop(
    force: bool = get_force_option(),
    schema: str = get_schema_option(),
):
    """Drop PostgreSQL schema (dangerous operation)."""
    cfg = DBConfig()
    schema_name = get_schema_name(cfg, schema)

    if not force:
        if not typer.confirm(f"Are you sure you want to drop schema '{schema_name}'? This will delete all tables."):
            console.print("[yellow]Cancelled.[/yellow]")
            return

    pg_client = PGClient(cfg, schema=schema_name)

    async def _run():
        try:
            await pg_client.connect()
            
            if not await pg_client.schema_exists():
                console.print(f"[yellow]Schema '{schema_name}' does not exist.[/yellow]")
                return

            console.print(f"[yellow]Dropping all tables in schema '{schema_name}'...[/yellow]")
            
            # Get all tables in the schema
            async with pg_client.pool.acquire() as conn:
                tables = await conn.fetch(
                    """
                    SELECT tablename 
                    FROM pg_tables 
                    WHERE schemaname = $1
                    """,
                    schema_name,
                )
                
                for table_row in tables:
                    table_name = table_row["tablename"]
                    await conn.execute(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;')
                    console.print(f"[green]Dropped table: {table_name}[/green]")
            
            console.print(f"[bold green]Schema '{schema_name}' dropped successfully[/bold green]")
        finally:
            await pg_client.close()

    asyncio.run(_run())


# ============================================================================
# Utility Commands
# ============================================================================

@app.command()
def check():
    """Test connectivity to both MySQL and PostgreSQL databases.
    
    Verifies that database credentials and network connectivity are properly configured.
    Exits with error code 1 if any connection fails.
    """
    cfg = DBConfig()
    console.rule("[bold cyan]DB CONNECTION CHECK[/bold cyan]")

    # MySQL
    import mysql.connector

    try:
        conn = mysql.connector.connect(
            host=cfg.mysql_host,
            port=cfg.mysql_port,
            user=cfg.mysql_user,
            password=cfg.mysql_password,
            database=cfg.mysql_db,
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        console.print("[green]MySQL OK[/green]")
    except Exception as e:
        console.print(f"[red]MySQL error:[/red] {e}")
        raise typer.Exit(1)

    # PostgreSQL
    import asyncpg

    async def _pg():
        try:
            conn = await asyncpg.connect(
                user=cfg.pg_user,
                password=cfg.pg_password,
                database=cfg.pg_db,
                host=cfg.pg_host,
                port=cfg.pg_port,
            )
            await conn.execute("SELECT 1")
            await conn.close()
            console.print("[green]PostgreSQL OK[/green]")
        except Exception as e:
            console.print(f"[red]PostgreSQL error:[/red] {e}")
            raise typer.Exit(1)

    asyncio.run(_pg())
    console.print("[bold green]All connections OK[/bold green]")


@app.command()
def tables():
    """List all tables that can be migrated."""
    table_display = Table(title="Available Tables", show_header=True, header_style="bold cyan")
    table_display.add_column("Name", style="cyan")
    table_display.add_column("Category", style="yellow")
    
    categories = {
        "event": ["event_types", "event_data", "events"],
        "state": ["state_attributes", "states_meta", "states"],
        "statistics": ["statistics_meta", "statistics", "statistics_short_term"],
        "system": ["recorder_runs", "statistics_runs", "schema_changes", "migration_changes"],
    }
    
    for cat, table_names in categories.items():
        for name in table_names:
            if any(name == t[0] for t in TABLES):
                table_display.add_row(name, cat)
    
    console.print(table_display)


@app.command()
def status(schema: str = get_schema_option()):
    """Compare row counts between MySQL and PostgreSQL."""
    cfg = DBConfig()
    mysql_client = MySQLClient(cfg)
    mysql_client.connect()

    async def _run():
        schema_name = get_schema_name(cfg, schema)
        pg_client = PGClient(cfg, schema=schema_name)
        await pg_client.connect()
        
        console.rule("[bold cyan]STATUS COMPARISON[/bold cyan]")
        
        try:
            validation_service = ValidationService(mysql_client, pg_client, logger)
            table_names = [t[0] for t in TABLES]
            results = await validation_service.validate_all_tables(table_names)

            status_table = Table(title="Migration Status", show_header=True, header_style="bold cyan")
            status_table.add_column("Table", style="cyan")
            status_table.add_column("MySQL Rows", style="yellow", justify="right")
            status_table.add_column("PostgreSQL Rows", style="yellow", justify="right")
            status_table.add_column("Status", style="green")

            all_match = True
            for result in results:
                if result.all_match:
                    status_table.add_row(
                        result.table,
                        f"{result.mysql_count:,}",
                        f"{result.pg_count:,}",
                        "✓ Match"
                    )
                else:
                    status_table.add_row(
                        result.table,
                        f"{result.mysql_count:,}",
                        f"{result.pg_count:,}",
                        "✗ Mismatch"
                    )
                    all_match = False

            console.print(status_table)
            
            if all_match:
                console.print("[bold green]All tables match![/bold green]")
            else:
                console.print("[bold yellow]Some tables have row count mismatches.[/bold yellow]")
        finally:
            mysql_client.close()
            await pg_client.close()

    asyncio.run(_run())


@app.command()
def progress():
    """Show current migration progress."""
    if not os.path.exists(PROGRESS_FILE):
        console.print("[yellow]No active migration progress found.[/yellow]")
        return
    
    with open(PROGRESS_FILE, "r") as f:
        progress_data = json.load(f)
    
    if not progress_data:
        console.print("[yellow]Progress file is empty.[/yellow]")
        return
    
    progress_table = Table(title="Migration Progress", show_header=True, header_style="bold cyan")
    progress_table.add_column("Table", style="cyan")
    progress_table.add_column("Last ID", style="yellow")
    progress_table.add_column("Total Rows", style="green", justify="right")
    
    for table, info in progress_data.items():
        last_id = str(info.get("last_id", "N/A"))
        total = info.get("total", "N/A")
        if isinstance(total, int):
            total = f"{total:,}"
        progress_table.add_row(table, last_id, total)
    
    console.print(progress_table)


# ============================================================================
# Validation Commands
# ============================================================================

def _validate_all_impl(schema: Optional[str] = None):
    """Internal implementation of validate all."""
    cfg = DBConfig()
    mysql_client = MySQLClient(cfg)
    mysql_client.connect()

    async def _run():
        schema_name = get_schema_name(cfg, schema)
        pg_client = PGClient(cfg, schema=schema_name)
        await pg_client.connect()
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
            else:
                console.print("[bold red]Validation failed: row counts mismatch[/bold red]")
                raise typer.Exit(1)
        finally:
            mysql_client.close()
            await pg_client.close()

    asyncio.run(_run())


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
    mysql_client.connect()

    async def _run():
        schema_name = get_schema_name(cfg, schema)
        pg_client = PGClient(cfg, schema=schema_name)
        await pg_client.connect()
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
            mysql_client.close()
            await pg_client.close()

    asyncio.run(_run())


# ============================================================================
# Migration Commands
# ============================================================================

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
            else:
                console.rule(
                    f"[bold yellow]Migration completed with errors in {duration:.2f}s[/bold yellow]"
                )
                raise typer.Exit(1)

        finally:
            mysql_client.close()
            await pg_client.close()

    asyncio.run(_run())
