# Home Assistant MySQL to PostgreSQL Migration Tool

> **✅ MIGRATION TOOL IS FULLY IMPLEMENTED AND READY FOR PRODUCTION USE**
> 
> **All migration commands are functional. Ready for testing in non-production environments.**
> 
> **Current Status:**
> - ✅ Configuration validation and PostgreSQL schema are complete
> - ✅ All CLI commands (check, tables, status, progress, schema, migrate, validate) are functional
> - ✅ Data migration engine with resume capability is implemented
> - ✅ Progress tracking, validation, and error handling are implemented

---

## Why PostgreSQL?

Home Assistant recommends PostgreSQL for larger installations, particularly for long-term statistics. This tool facilitates the migration from MySQL/MariaDB by handling:

- **Type Conversion**: Maps MySQL types to PostgreSQL equivalents (e.g., `TINYINT(1)` to `BOOLEAN`).
- **Data Cleaning**: Fixes common issues like null bytes in strings.
- **Schema Optimization**: Applies a schema optimized for Home Assistant's data structure.

## Installation

### Prerequisites

- Python 3.13+
- Access to both MySQL/MariaDB and PostgreSQL databases
- `uv` package manager (recommended) or `pip`

### Setup

```bash
# Clone the repository
git clone https://github.com/windyboy/hass-migrate.git
cd hass-migrate

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
PG_SCHEMA=public  # Default schema (use 'public' for standard HA setup)
```

## Usage

> **Note:** All commands are implemented. See below for details.

### 1. Test Database Connections

Before migrating, verify both databases are accessible:

```bash
uv run hamigrate check
```

This will test connections to both MySQL and PostgreSQL databases.

### 2. List Available Tables

View all tables that can be migrated:

```bash
uv run hamigrate tables
```

### 3. Check Migration Status

Compare row counts between MySQL and PostgreSQL:

```bash
uv run hamigrate status
```

### 4. View Migration Progress

Check the progress of an ongoing or interrupted migration:

```bash
uv run hamigrate progress
```

### 5. Schema Management

Apply PostgreSQL schema:

```bash
uv run hamigrate schema apply
uv run hamigrate schema apply --force  # Force recreate schema
```

Drop schema (dangerous operation):

```bash
uv run hamigrate schema drop --force
```

### 6. Migrate Data

#### Migrate a Single Table

Test the migration process with a single table:

```bash
uv run hamigrate migrate table event_data --force
```

**Options:**
- `--force` / `-f`: Skip confirmation prompts and truncate tables
- `--batch-size`: Number of rows per batch (default: 20,000)
- `--schema`: PostgreSQL schema name (default: PG_SCHEMA env var or 'public')

#### Migrate All Tables

Perform a complete migration of all Home Assistant recorder tables:

```bash
uv run hamigrate migrate all --force
```

**Options:**
- `--force` / `-f`: Skip confirmation prompts and truncate tables
- `--batch-size`: Number of rows per batch (default: 20,000)
- `--max-concurrent`: Maximum number of tables to migrate concurrently (default: 4)
- `--backup`: Create backup before migration
- `--schema`: PostgreSQL schema name (default: PG_SCHEMA env var or 'public')

**Example with custom batch size:**
```bash
uv run hamigrate migrate all --force --batch-size 50000
```

#### Resume Interrupted Migration

Resume from a previous interrupted migration:

```bash
uv run hamigrate migrate resume
```

**Options:**
- `--batch-size`: Number of rows per batch (default: 20,000)
- `--max-concurrent`: Maximum number of tables to migrate concurrently (default: 4)
- `--schema`: PostgreSQL schema name (default: PG_SCHEMA env var or 'public')

### 7. Validate Migration

Validate all tables:

```bash
uv run hamigrate validate
```

Validate a single table:

```bash
uv run hamigrate validate table event_data
```

**Options:**
- `--schema`: PostgreSQL schema name (default: PG_SCHEMA env var or 'public')

**Validation Limitations:**

The current validation (`uv run hamigrate validate`) performs only row count comparison between source and target databases. This is a basic check that ensures the number of rows matches but does not verify data integrity, content accuracy, or detect subtle migration errors.

For production migrations, consider additional validation steps:
- **Manual Sampling**: Spot-check random rows in key tables
- **Checksum Validation**: Compare MD5/SHA256 hashes of table data (requires custom scripts)
- **Application Testing**: Run Home Assistant with migrated data and verify functionality
- **Statistics Verification**: Ensure historical data and trends are preserved

Future versions may include configurable sampling and checksum validation options.

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
7. **Progress Tracking**: Saves progress for resume capability

## Migration Progress and Resume

The tool uses a `migration_progress.json` file in the working directory to track migration progress. This allows resuming interrupted migrations without restarting from the beginning.

### Progress File Structure

The progress file contains a JSON object with the following structure:

```json
{
  "table_name": {
    "last_id": 12345,
    "total": 67890,
    "status": "in_progress",
    "updated_at": "2023-10-01T12:00:00Z"
  },
  ...
}
```

- `last_id`: The last processed primary key value for resume
- `total`: Total number of rows in the table
- `status`: One of "in_progress", "completed", "failed"
- `updated_at`: Timestamp of last update

### Resume Strategy

- **Safe to Resume**: When migration was interrupted due to network issues, temporary database unavailability, or manual stop (Ctrl+C)
- **Restart Required**: When source data has changed significantly, schema mismatches, or if progress file is corrupted
- **Manual Resume**: Use `uv run hamigrate migrate resume` command
- **Force Restart**: Delete `migration_progress.json` and run `uv run hamigrate migrate all --force`

### Best Practices

- Do not modify the progress file manually
- Keep backups of the progress file during long migrations
- Monitor disk space as progress file grows with table count

## Post-Migration Steps

1. **Validate the migration:**
   ```bash
   uv run hamigrate validate
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
- The PostgreSQL schema file should be at `hass_migrate/schema/postgres_schema.sql`
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
uv sync

# Run all unit tests
uv run python -m pytest

# Run tests with verbose output
uv run python -m pytest -v

# Run specific test file
uv run python -m pytest tests/test_config.py
```

### Testing

### Project Structure

```
hass-migrate/
├── hass_migrate/
│   ├── __init__.py          # Package initialization
│   ├── cli/                 # Command-line interface
│   ├── config.py            # Configuration and validation
│   ├── database/            # Database clients
│   ├── models/              # Data models
│   ├── schema/
│       ├── postgres_schema.sql  # PostgreSQL schema (USED)
│       └── schema.sql           # MySQL schema (reference only)
│   ├── services/            # Business logic services
│   └── utils/               # Utility functions
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
3. Test connections with `uv run hamigrate check`
4. Check Home Assistant documentation for PostgreSQL setup

## Version

Current version: 0.1.0-dev

Implemented in this version:
- ✅ Configuration validation
- ✅ Database connection testing
- ✅ PostgreSQL schema SQL
- ✅ Table listing and status checking
- ✅ Data migration engine
- ✅ Schema management commands
- ✅ Progress tracking and resume
- ✅ Validation commands
- ✅ Concurrent table migration