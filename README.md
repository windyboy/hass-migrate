# Home Assistant MySQL to PostgreSQL Migration Tool

A tool for migrating Home Assistant Recorder data from MySQL/MariaDB to PostgreSQL with data integrity and type conversion.

## Features

- High-performance bulk migration
- Schema-aware field type transformation
- Data cleaning (null bytes, empty strings to NULL)
- Primary key sequence correction
- Validation and consistency checks

## Why PostgreSQL?

Home Assistant recommends PostgreSQL for optimal performance and stability, especially for long-term statistics and time-series data.

## Installation

```bash
uv sync
cp .env.example .env
```

Configure database connections in `.env`:

```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=homeassistant
MYSQL_PASSWORD=your_mysql_password
MYSQL_DB=homeassistant

PG_HOST=localhost
PG_PORT=5432
PG_USER=homeassistant
PG_PASSWORD=your_pg_password
PG_DB=homeassistant
```

## Usage

1. Test database connectivity:
   ```bash
   hamigrate check
   ```

2. Perform full migration (drops target data with `--force`):
   ```bash
   hamigrate migrate-all --force
   ```

## Post-Migration

- Validate by inserting a test row into `states` table.
- Update `configuration.yaml` to use PostgreSQL.
- Restart Home Assistant.

## Supported Tables

- event_types
- events
- event_data
- state_attributes
- states_meta
- states
- statistics_meta
- statistics
- statistics_short_term
- recorder_runs
- statistics_runs
- schema_changes

## License

MIT
