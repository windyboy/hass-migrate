# Home Assistant MySQL to PostgreSQL Migration Tool

A robust, production-ready tool for migrating Home Assistant Recorder data from MySQL/MariaDB to PostgreSQL with data integrity validation and type conversion.

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

### 2. Validate Migration (After Migration)

After completing a migration, validate data integrity:

```bash
hamigrate validate
```

This compares row counts between MySQL and PostgreSQL for all tables.

### 3. Test Migration (Single Table)

Test the migration process with a single table:

```bash
hamigrate migrate-event-data --force
```

### 4. Full Migration

Perform a complete migration of all Home Assistant recorder tables:

```bash
hamigrate migrate-all --force
```

**Options:**
- `--force` / `-f`: Skip confirmation prompt (truncates target tables)
- `--resume`: Resume from previous interrupted migration
- `--batch-size`: Set custom batch size (default: 20,000 rows)

**Example with custom batch size:**
```bash
hamigrate migrate-all --force --batch-size 50000
```

**Resume interrupted migration:**
```bash
hamigrate migrate-all --resume
```

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
# Install dev dependencies
uv sync

# Run validation after migration
hamigrate validate
```

### Project Structure

```
migrate/
├── migrate/
│   ├── __init__.py          # Package initialization
│   ├── cli.py               # Command-line interface
│   ├── config.py            # Configuration and validation
│   ├── engine.py            # Migration engine
│   └── schema/
│       └── postgres_schema.sql  # PostgreSQL schema
├── .env.example             # Environment template
├── pyproject.toml           # Project dependencies
└── README.md                # This file
```

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