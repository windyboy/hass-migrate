# Home Assistant MySQL to PostgreSQL Migration Tool

> **Status**: Beta. Core migration logic is functional and tested. Please backup your data before use.

A robust tool for migrating Home Assistant Recorder data from MySQL/MariaDB to PostgreSQL with data integrity validation and type conversion.

## Features

- ✅ High-performance bulk migration with configurable batch sizes
- ✅ Schema-aware field type transformation (MySQL → PostgreSQL)
- ✅ Data cleaning (null bytes, empty strings, timezone conversion)
- ✅ Primary key sequence correction
- ✅ Built-in validation and consistency checks
- ✅ Resume capability for interrupted migrations
- ✅ Concurrent table migration for improved performance
- ✅ Progress tracking with atomic updates

## Why PostgreSQL?

Home Assistant recommends PostgreSQL for optimal performance and stability, especially for long-term statistics and time-series data. PostgreSQL offers:

- Better performance for complex queries
- More robust handling of concurrent operations
- Advanced indexing capabilities
- Better support for JSON data
- Improved reliability and data integrity

## Installation

### Prerequisites

- Python 3.13+
- Access to both MySQL/MariaDB and PostgreSQL databases
- `uv` package manager (recommended) or `pip`

### Setup

```bash
# Clone or navigate to the project directory
cd migrate

# Install dependencies
uv sync

# Create configuration file
cp .env.example .env

# Edit .env with your database credentials
nano .env
```

### Configuration

Edit `.env` with your database connection details:

```env
# MySQL/MariaDB Source Database
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=homeassistant
MYSQL_PASSWORD=your_mysql_password
MYSQL_DB=homeassistant

# PostgreSQL Target Database
PG_HOST=localhost
PG_PORT=5432
PG_USER=homeassistant
PG_PASSWORD=your_postgres_password
PG_DB=homeassistant
```

## Usage

### 1. Test Database Connections

Before migrating, verify both databases are accessible:

```bash
hamigrate check
```

This will test connections to both MySQL and PostgreSQL databases.

### 2. List Available Tables

View all tables that can be migrated:

```bash
hamigrate tables
```

### 3. Check Migration Status

Compare row counts between MySQL and PostgreSQL:

```bash
hamigrate status
```

### 4. View Migration Progress

Check the progress of an ongoing or interrupted migration:

```bash
hamigrate progress
```

### 5. Schema Management

Apply PostgreSQL schema:

```bash
hamigrate schema apply
hamigrate schema apply --force  # Force recreate schema
```

Drop schema (dangerous operation):

```bash
hamigrate schema drop --force
```

### 6. Migrate Data

#### Migrate a Single Table

Test the migration process with a single table:

```bash
hamigrate migrate table event_data --force
```

**Options:**
- `--force` / `-f`: Skip confirmation prompts and truncate tables
- `--batch-size`: Number of rows per batch (default: 20,000)
- `--schema`: PostgreSQL schema name (default: PG_SCHEMA env var or 'hass')

#### Migrate All Tables

Perform a complete migration of all Home Assistant recorder tables:

```bash
hamigrate migrate all --force
```

**Options:**
- `--force` / `-f`: Skip confirmation prompts and truncate tables
- `--batch-size`: Number of rows per batch (default: 20,000)
- `--max-concurrent`: Maximum number of tables to migrate concurrently (default: 4)
- `--backup`: Create backup before migration
- `--schema`: PostgreSQL schema name (default: PG_SCHEMA env var or 'hass')

**Example with custom batch size:**
```bash
hamigrate migrate all --force --batch-size 50000
```

#### Resume Interrupted Migration

Resume from a previous interrupted migration:

```bash
hamigrate migrate resume
```

**Options:**
- `--batch-size`: Number of rows per batch (default: 20,000)
- `--max-concurrent`: Maximum number of tables to migrate concurrently (default: 4)
- `--schema`: PostgreSQL schema name (default: PG_SCHEMA env var or 'hass')

### 7. Validate Migration

Validate all tables:

```bash
hamigrate validate
```

Validate a single table:

```bash
hamigrate validate table event_data
```

**Options:**
- `--schema`: PostgreSQL schema name (default: PG_SCHEMA env var or 'hass')

## Supported Tables

The tool migrates all Home Assistant recorder tables:

### Event Tables
- `event_types` - Event type definitions
- `event_data` - Shared event data
- `events` - Event records

### State Tables
- `state_attributes` - Shared state attributes
- `states_meta` - Entity metadata
- `states` - State records

### Statistics Tables
- `statistics_meta` - Statistics metadata
- `statistics` - Long-term statistics
- `statistics_short_term` - Short-term statistics

### System Tables
- `recorder_runs` - Recorder session information
- `statistics_runs` - Statistics run tracking
- `schema_changes` - Database schema version history
- `migration_changes` - Migration tracking

## Migration Process

The tool follows this process:

1. **Schema Setup**: Applies PostgreSQL schema if not present
2. **Dependency Order**: Migrates base tables first (event_types, event_data, etc.)
3. **Concurrent Migration**: Large data tables migrate in parallel
4. **Data Transformation**: 
   - Converts MySQL types to PostgreSQL equivalents
   - Removes null bytes from strings
   - Converts empty strings to NULL
   - Adds timezone info to datetime fields
   - Converts tinyint(1) to boolean for specific fields
5. **Sequence Correction**: Updates PostgreSQL sequences to match data
6. **Progress Tracking**: Saves progress for resume capability

## Post-Migration Steps

1. **Validate the migration:**
   ```bash
   hamigrate validate
   ```

2. **Update Home Assistant configuration:**
   
   Edit `configuration.yaml`:
   ```yaml
   recorder:
     db_url: postgresql://homeassistant:password@localhost/homeassistant
   ```

3. **Restart Home Assistant**

4. **Verify in Home Assistant:**
   - Check logs for any database errors
   - Verify history is accessible
   - Check that statistics are displaying correctly

5. **Backup and cleanup:**
   - Keep MySQL database as backup for 1-2 weeks
   - Monitor PostgreSQL performance
   - Once confirmed working, you can remove the MySQL database

## Troubleshooting

### Connection Errors

**Error: "Missing required environment variable"**
- Ensure `.env` file exists and contains all required variables
- Check that variable names are correct

**Error: "Database connection refused"**
- Verify database services are running
- Check host, port, username, and password
- Test connections manually with `mysql` and `psql` clients

### Migration Issues

**Error: "Schema file not found"**
- The PostgreSQL schema file should be at `migrate/schema/postgres_schema.sql`
- Ensure you're running the latest version

**Slow migration performance:**
- Increase batch size: `--batch-size 50000`
- Check network latency between databases
- Ensure databases have adequate resources

**Resume not working:**
- Check for `migration_progress.json` file in working directory
- Ensure file has not been corrupted
- If needed, delete progress file and restart migration

### Validation Failures

**Row count mismatches:**
- Check if source database is still receiving writes during migration
- Verify no errors occurred during migration (check console output)
- Re-run migration on affected tables

## Performance Tips

- **Batch Size**: Larger batches (50k-100k) can improve performance but use more memory
- **Network**: Run migration tool close to databases (low latency)
- **Resources**: Ensure PostgreSQL has adequate memory and CPU
- **Concurrent**: The tool automatically migrates independent tables concurrently
- **Timing**: Run during low-activity periods for best performance

## Data Transformations

The tool handles these MySQL → PostgreSQL conversions:

| MySQL Type | PostgreSQL Type | Notes |
|------------|-----------------|-------|
| `BIGINT(n) AUTO_INCREMENT` | `BIGSERIAL` | Sequence automatically created |
| `INT(n) UNSIGNED` | `INTEGER` | Size parameter removed |
| `TINYINT(1)` | `BOOLEAN` | Only for specific boolean fields |
| `SMALLINT(n)` | `SMALLINT` | Size parameter removed |
| `VARCHAR(n)` | `VARCHAR(n)` | Same |
| `LONGTEXT` | `TEXT` | PostgreSQL uses TEXT for large text |
| `TINYBLOB` | `BYTEA` | Binary data |
| `DATETIME(6)` | `TIMESTAMP` | Timezone info added (UTC) |
| `DOUBLE` | `DOUBLE PRECISION` | Standard SQL name |

**Special Handling:**
- Null bytes (`\x00`) are removed from strings
- Empty strings converted to NULL (especially for deprecated CHAR(0) fields)
- Timezone info added to all timestamps (UTC)

## Development

### Running Tests

```bash
# Install dependencies including test tools
uv sync --extra test

# Run all unit tests
uv run pytest

# Run tests with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_config.py

# Run with coverage (if pytest-cov is installed)
uv run pytest --cov=migrate --cov-report=html
```

### Testing

### Project Structure

```
migrate/
├── migrate/
│   ├── __init__.py          # Package initialization
│   ├── cli.py               # Command-line interface
│   ├── config.py            # Configuration and validation
│   ├── engine.py            # Migration engine
│   └── schema/
│       ├── postgres_schema.sql  # PostgreSQL schema (USED)
│       └── schema.sql           # MySQL schema (reference only)
├── .env.example             # Environment template
├── pyproject.toml           # Project dependencies
└── README.md                # This file
```

**Note:** The migration tool uses `postgres_schema.sql` which contains the proper PostgreSQL schema. The `schema.sql` file is a MySQL dump kept for reference only.

### Code Quality

The project follows these standards:
- Type hints throughout
- Async/await for database operations
- Connection pooling for PostgreSQL
- Retry logic for transient failures
- Atomic progress updates

See `AGENTS.md` for detailed coding guidelines.

## License

MIT

## Support

For issues or questions:
1. Check this README's Troubleshooting section
2. Verify your configuration in `.env`
3. Test connections with `hamigrate check`
4. Check Home Assistant documentation for PostgreSQL setup

## Version

Current version: 0.1.0

Features in this version:
- Complete table migration
- Data type conversion
- Progress tracking and resume
- Validation command
- Concurrent table migration
- Robust error handling