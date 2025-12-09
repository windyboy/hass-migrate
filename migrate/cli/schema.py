import asyncio
import os

import typer

from migrate.config import DBConfig
from migrate.database.pg_client import PGClient
from migrate.cli.constants import SCHEMA_FILE, console
from migrate.cli.options import get_force_option, get_schema_option, get_schema_name


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


def register_schema_commands(schema_app: typer.Typer) -> None:
    """Register schema management commands."""
    
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

