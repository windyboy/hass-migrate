# Home Assistant Migration Tool - Agent Guide

## Build/Test Commands
- Setup: `uv sync` (Python 3.13+ required)
- Run all tests: `uv run pytest`
- Run single test: `uv run pytest tests/test_file.py::TestClass::test_method`
- Run E2E: `uv run pytest tests/test_e2e.py`
- Run last failed: `uv run pytest --lf`

## Code Style Guidelines
- **Imports**: Std lib → third-party → local, one per line, `from __future__ import annotations` first
- **Formatting**: 4 spaces indent, f-strings, double quotes for strings, single for docstrings
- **Naming**: `snake_case` vars/functions, `PascalCase` classes, `UPPER_CASE` constants, `_` private
- **Types**: Full hints, `Optional[T]`, `List[T]`, `Dict[K,V]`, `|` unions (3.13+)
- **Async**: `async/await`, `asyncio.run()` top-level, `async with` resources
- **Database**: `%s` MySQL, `$1` PG parameterized queries, batch ops, pooling
- **Error Handling**: Specific exceptions, `typer.Exit(1)` CLI, Rich console, no credential logging
- **Patterns**: Dependency injection, batch processing, progress tracking

## Migration Commands
- Check: `uv run hamigrate check`
- Migrate all: `uv run hamigrate migrate all --force`
- Resume: `uv run hamigrate migrate resume`

## Key Components
- CLI: `hass_migrate/cli/` (Typer)
- Services: `hass_migrate/services/` (logic)
- Database: `hass_migrate/database/` (clients)
- Utils: `hass_migrate/utils/` (cleaner, progress)

See `.github/copilot-instructions.md` for additional agent instructions.