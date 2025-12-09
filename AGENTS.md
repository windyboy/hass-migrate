# Agent Instructions for Home Assistant Migration Tool

## Build/Lint/Test Commands

### Installation & Setup
```bash
uv sync  # Install dependencies
```

### Running the Tool
```bash
hamigrate check                    # Test database connections
hamigrate migrate-all --force     # Full migration (truncates target DB)
hamigrate migrate-event-data      # Test migration of single table
```

### Testing
```bash
# Run all unit tests
uv run pytest

# Run with verbose output
uv run pytest -v
```
Unit tests are located in the `tests/` directory and cover:
- Configuration validation (`test_config.py`)
- Data cleaning utilities (`test_data_cleaner.py`)
- Dependency analysis (`test_dependency.py`)
- Exception handling (`test_exceptions.py`)
- Data models (`test_table_metadata.py`)

## Code Style Guidelines

### Imports
- Standard library imports first
- Third-party imports second  
- Local imports last
- One import per line
- Use `from __future__ import annotations` for forward references

### Naming Conventions
- Functions/variables: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_CASE`
- Modules: `snake_case`

### Types & Type Hints
- Use type hints for all function parameters and return values
- Use `Optional[T]` for nullable types
- Use `List[T]`, `Dict[K,V]` from typing module

### Error Handling
- Use specific exception types in try/except blocks
- Raise `typer.Exit(1)` for CLI errors
- Use Rich console for user-friendly error messages

### Formatting
- 4 spaces indentation
- Line length: reasonable (no strict limit enforced)
- Use f-strings for string formatting
- Double quotes for strings, single quotes for docstrings

### Async Code
- Use `async`/`await` pattern consistently
- Use `asyncio.run()` for top-level async functions
- Proper connection cleanup with `async with` or manual close

### Database Operations
- Use parameterized queries to prevent SQL injection
- Batch operations with reasonable batch sizes (20k rows)
- Proper connection pooling and cleanup

### Comments & Documentation
- English comments preferred (some legacy Chinese comments exist)
- Docstrings for public functions and classes
- Inline comments for complex logic only

### Security
- Never log or expose database credentials
- Use environment variables for configuration
- Validate user inputs before database operations</content>
<parameter name="filePath">/Users/windy/Projects/hass/migrate/AGENTS.md