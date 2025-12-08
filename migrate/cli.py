import asyncio
import os
import time
from typing import List

import typer
from rich.console import Console
from rich.progress import Progress, TaskID

from migrate.config import DBConfig
from migrate.engine import Migrator

app = typer.Typer(help="Home Assistant MySQL → PostgreSQL migration tool")
console = Console()

SCHEMA_FILE = "migrate/schema/postgres_schema.sql"

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


async def ensure_schema(migrator: Migrator):
    if not os.path.exists(SCHEMA_FILE):
        console.print(f"[red]Missing schema file: {SCHEMA_FILE}[/red]")
        raise typer.Exit(1)

    exists = await migrator.schema_exists()
    if not exists:
        console.print("[yellow]Schema missing → applying schema.sql...[/yellow]")
        await migrator.apply_schema(SCHEMA_FILE)
        console.print("[green]Schema applied successfully[/green]")
    else:
        console.print("[cyan]Schema exists in PostgreSQL[/cyan]")


@app.command()
def check():
    """Check database connections."""
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


@app.command("migrate-event-data")
def migrate_event_data(
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
    batch_size: int = typer.Option(
        20000, "--batch-size", help="Batch size for migration"
    ),
):
    """Migrate only event_data table (for testing)."""
    cfg = DBConfig()
    m = Migrator(cfg, batch_size)
    m.connect_mysql()

    async def _run():
        await m.connect_pg()
        await ensure_schema(m)

        table = "event_data"
        cols = [c for (t, c) in TABLES if t == table][0]

        if not force:
            if not typer.confirm(f"Truncate target table '{table}' before migration?"):
                console.print("[yellow]Cancelled.[/yellow]")
                await m.close()
                return

        await m.truncate_table(table)
        console.rule(f"[cyan]Migrating {table}[/cyan]")
        await m.migrate_table(table, cols)
        await m.fix_sequence(table, cols[0])

        await m.close()
        console.print(f"[bold green]Done: {table}[/bold green]")

    asyncio.run(_run())


@app.command("migrate-all")
def migrate_all(
    force: bool = typer.Option(False, "--force", "-f", help="Truncate without asking"),
    resume: bool = typer.Option(
        False, "--resume", help="Resume from previous migration"
    ),
    batch_size: int = typer.Option(
        20000, "--batch-size", help="Batch size for migration"
    ),
):
    """Full migration: schema → truncate → migrate all recorder tables."""
    cfg = DBConfig()
    m = Migrator(cfg, batch_size)
    m.connect_mysql()
    m.load_progress()

    async def _run():
        start_time = time.time()
        await m.connect_pg()
        await ensure_schema(m)

        if not resume and not force:
            if not typer.confirm("Truncate ALL tables before migration?"):
                console.print("[yellow]Cancelled.[/yellow]")
                await m.close()
                return

        # Define dependency order: migrate base tables first, then concurrently migrate large data tables
        dependent_tables = [
            "event_types",
            "event_data",
            "state_attributes",
            "states_meta",
            "statistics_meta",
        ]
        concurrent_tables = [
            "events",
            "states",
            "statistics",
            "statistics_short_term",
            "recorder_runs",
            "statistics_runs",
            "schema_changes",
            "migration_changes",
        ]

        # First migrate dependent tables sequentially
        for table in dependent_tables:
            cols = next(c for t, c in TABLES if t == table)
            if resume and table in m.progress:
                console.print(
                    f"[cyan]Resuming {table} from {m.progress[table]['total']:,} rows[/cyan]"
                )
            else:
                if not resume:
                    await m.truncate_table(table)
                console.rule(f"[cyan]Migrating {table}[/cyan]")

            await m.migrate_table(table, cols)
            await m.fix_sequence(table, cols[0])

            if table in m.progress:
                del m.progress[table]
                m.save_progress()

            console.print(f"[green]{table}: done[/green]")

        # Concurrently migrate remaining tables
        async def migrate_concurrent(table: str, cols: List[str]):
            if resume and table in m.progress:
                console.print(
                    f"[cyan]Resuming {table} from {m.progress[table]['total']:,} rows[/cyan]"
                )
            else:
                if not resume:
                    await m.truncate_table(table)
                console.rule(f"[cyan]Migrating {table}[/cyan]")

            await m.migrate_table(table, cols)
            await m.fix_sequence(table, cols[0])

            if table in m.progress:
                del m.progress[table]
                m.save_progress()

            console.print(f"[green]{table}: done[/green]")

        tasks = [
            migrate_concurrent(table, next(c for t, c in TABLES if t == table))
            for table in concurrent_tables
        ]
        await asyncio.gather(*tasks)

        # Clean up progress file
        if os.path.exists("migration_progress.json"):
            os.remove("migration_progress.json")

        duration = time.time() - start_time
        await m.close()
        console.rule(
            f"[bold green]Migration completed successfully in {duration:.2f}s! 🎉[/bold green]"
        )

    asyncio.run(_run())
