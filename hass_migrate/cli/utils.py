import asyncio
import json
import os

import typer
from rich.table import Table

from hass_migrate.config import DBConfig
from hass_migrate.database.mysql_client import MySQLClient
from hass_migrate.database.pg_client import PGClient
from hass_migrate.services.validation_service import ValidationService
from hass_migrate.cli.constants import TABLES, PROGRESS_FILE, console, logger
from hass_migrate.cli.options import get_schema_option, get_schema_name


def register_utils_commands(app: typer.Typer) -> None:
    """Register utility commands."""
    
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

