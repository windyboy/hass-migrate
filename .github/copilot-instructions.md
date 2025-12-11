# Home Assistant Migration Tool - AI Agent Instructions

## Architecture Overview
This is a CLI tool for migrating Home Assistant data from MySQL/MariaDB to PostgreSQL. Key components:
- **CLI Layer** (`hass_migrate/cli/`): Typer-based commands for user interaction (e.g., `migrate.py` handles migration commands).
- **Database Clients** (`hass_migrate/database/`): `MySQLClient` and `PGClient` for connections, using pymysql and asyncpg.
- **Services** (`hass_migrate/services/`): Core logic like `MigrationService` for data transfer, `BackupService` for pre-migration backups, `ValidationService` for post-migration checks.
- **Utils** (`hass_migrate/utils/`): `DataCleaner` for fixing null bytes, `DependencyAnalyzer` for table migration order, `ProgressTracker` for resume capability.
- **Data Flow**: Read MySQL in batches (20k rows), clean data, insert into PG with unique constraint handling via `ON CONFLICT DO NOTHING` in `MigrationService._insert_executemany`.

Service boundaries: CLI parses args, services handle business logic, utils provide shared functions. Async throughout for concurrency.

## Key Workflows
- **Setup**: `uv sync` to install deps (Python 3.13+ required).
- **Testing**: `uv run pytest` for all tests; `uv run pytest tests/test_file.py::TestClass::test_method` for specific. E2E tests in `test_e2e.py`.
- **Migration**: `hamigrate check` (verify connections), `hamigrate migrate all --force` (full migration with resume via progress tracking).
- **Debugging**: Use `StructuredLogger` for events; check progress with `hamigrate progress`. Errors logged via Rich console.

## Code Conventions
- **Imports**: Std lib → third-party → local, one per line, `from __future__ import annotations`.
- **Style**: 4-space indent, f-strings, double quotes for strings, single for docstrings. `snake_case` vars/functions, `PascalCase` classes.
- **Types**: Full hints on params/returns, use `Optional[T]`, `List[T]`, `Dict[K,V]` from typing.
- **Async**: Consistent `async/await`, `asyncio.run()` at top-level, `async with` for cleanup.
- **Database**: Parameterized queries (e.g., `%s` in MySQL, `$1` in PG), batch inserts (20k rows), connection pooling.
- **Error Handling**: Specific exceptions, `typer.Exit(1)` for CLI, Rich console for user messages. No credential logging.
- **Patterns**: Dependency injection in services (e.g., `MigrationService` takes clients/logger). Batch processing in `migrate_table` with progress tracking.

## Integration Points
- **External Deps**: asyncpg (PG), pymysql (MySQL), typer (CLI), rich (console), pydantic (config).
- **Cross-Component**: Services communicate via injected deps; CLI calls services directly. Config via `.env` (e.g., `MYSQL_HOST`).
- **Example**: In `MigrationService.migrate_table`, fetch from MySQL cursor, clean via `DataCleaner`, insert via `_insert_executemany` with schema from config.

Reference: `AGENTS.md` for style, `README.md` for commands, `migration_service.py` for core logic.