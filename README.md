Home Assistant MySQL → PostgreSQL Migration Tool

A safe, fast, automated migration tool to move Home Assistant Recorder data
from MySQL / MariaDB → PostgreSQL, with full data integrity and optimized type conversion.

Supports:

🚀 High-performance bulk migration

🔄 Schema-aware field type transformation

🧹 Data cleaning (null bytes, empty → NULL)

🧩 Autocorrect Primary Key SEQUENCEs

🧪 Validation & consistency checks

Powered by: Python + Typer + asyncpg + mysql-connector.

✨ Why PostgreSQL?

Home Assistant strongly recommends PostgreSQL for Recorder:

Backend	Recommendation	Notes
SQLite	❌ Not suitable for long-term data	
MySQL / MariaDB	⚠️ Limited statistics performance	
PostgreSQL	✅ Best performance & stability	

Benefits:

Better aggregation for long-term statistics

Stronger time-series support (TimescaleDB)

True timezone-aware timestamps

Efficient JSONB storage

This tool makes the migration painless.

🔄 Database Differences & Conversion Strategy
Area	MySQL/MariaDB	PostgreSQL	Migration Action
Boolean	TINYINT(1)	boolean	0→FALSE, 1→TRUE
Datetime	DATETIME	timestamptz	Auto-add UTC timezone
JSON Data	TEXT	jsonb	json.loads() and convert
Invalid characters	Allows \x00	❌ not allowed	Strip null-bytes
Empty vs NULL	Empty string may mean null	NULL required	Convert "" → NULL
Auto-PK	AUTO_INCREMENT	SEQUENCE	Auto setval(last_value)
Batch Read	Slow cursor fetch	executemany() optimized	Bulk-size = 20k rows

All conversions applied automatically per table.

🏁 Installation
uv sync


Copy environment template:

cp .env.example .env


Configure MySQL & PostgreSQL connection:

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

▶️ Usage
1️⃣ Test DB connectivity
hamigrate check


Expected output:

MySQL OK
PostgreSQL OK
All connections OK

2️⃣ Full migration

Drops all data in target DB before importing (when --force is used)

hamigrate migrate-all --force


Workflow:

Step	Action
1	Validate or auto-create target schema
2	TRUNCATE all recording tables
3	Migrate data ordered by FK dependency
4	Fix all sequences to max ID
5	Summary & timing

Example success:

recorder_runs: done
Migration completed successfully! 🎉

🔍 Post-migration validation

Insert a test row:

INSERT INTO states (entity_id, state, last_changed)
VALUES ('test.validation', 'on', NOW())
RETURNING state_id;


If a valid, incremented state_id returns → sequence repair confirmed ✔️

Configure Home Assistant

In configuration.yaml:

recorder:
  db_url: postgresql://homeassistant:password@localhost/homeassistant


Reboot HA → migration complete 🚀

📊 Supported Recorder Tables
Table	Purpose
event_types	Event type mapping
events	Event history
event_data	JSON event payloads
state_attributes	State attributes mapping
states_meta	Entity metadata
states	Entity state history
statistics_meta	Long-term statistics metadata
statistics	Long-term aggregated metrics
statistics_short_term	Short-term metrics (cache)
recorder_runs	Recorder service run history
statistics_runs	Statistics job run history
schema_changes	HA DB schema versioning
migration_changes	Migration tracking (optional)
⚙️ Recommended PostgreSQL Optimizations
Goal	SQL
Improve query planning	VACUUM ANALYZE;
Enable TimescaleDB	SELECT create_hypertable('statistics', 'start');
Common index boost	CREATE INDEX ON states (entity_id);

Time-Series + HA = 🔥 Performance.

🚧 Known Limitations
Area	Status
Very large DBs (>50GB statistics)	Needs long-run testing
Custom HA recorder tables	Not auto-migrated
Historical timezone shifts	UTC applied uniformly
🧱 Roadmap

🔄 Incremental migration (--since DAYS)

♻️ Resume migration (store progress)

🧪 Hash checksum verification

🌐 Web dashboard for migration progress

📜 License

MIT — Contributions welcome! 🎯
Issues / PRs highly appreciated.

If you’d like, I can additionally provide:

✔ ASCII architecture diagram
✔ Migration progress screenshots
✔ Benchmark results
✔ A GitHub Release build + Home Assistant guide

Would you like a bilingual version (English + Chinese) as well?
I can generate it cleanly with tabbed sections for GitHub ✨
