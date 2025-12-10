# Agent Instructions for Home Assistant Migration Tool

## Build/Lint/Test Commands
```bash
uv sync                                    # Install dependencies
uv run pytest                             # Run all unit tests
uv run pytest tests/test_file.py::TestClass::test_method  # Run single test

# Note: Migration commands are fully implemented
hamigrate check                           # Test database connections (implemented)
hamigrate tables                          # List tables (implemented)
hamigrate status                          # Check migration status (implemented)
hamigrate migrate table <name> --force    # Migrate single table (implemented)
hamigrate migrate all --force             # Full migration (implemented)
hamigrate migrate resume                  # Resume interrupted migration (implemented)
```

## Code Style Guidelines
- **Imports**: Standard library first, third-party second, local last. One per line. Use `from __future__ import annotations`
- **Naming**: `snake_case` for functions/variables, `PascalCase` for classes, `UPPER_CASE` for constants
- **Types**: Type hints on all parameters/returns. Use `Optional[T]`, `List[T]`, `Dict[K,V]` from typing
- **Error Handling**: Specific exceptions in try/except. `typer.Exit(1)` for CLI errors. Rich console for messages
- **Formatting**: 4 spaces indent, f-strings, double quotes for strings, single for docstrings
- **Async**: `async`/`await` consistently, `asyncio.run()` for top-level, proper cleanup with `async with`
- **Database**: Parameterized queries, batch operations (20k rows), connection pooling/cleanup
- **Security**: No credential logging, env vars for config, validate inputs before DB operations</content>
<parameter name="filePath">/Users/windy/Projects/hass/migrate/AGENTS.md