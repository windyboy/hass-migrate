-- Home Assistant PostgreSQL Schema with TimescaleDB Support
-- Converted from MySQL/MariaDB for optimal PostgreSQL performance with TimescaleDB hypertables
-- Version: 2.0 (TimescaleDB)
-- Date: 2024-12-08
--
-- Key Changes for TimescaleDB:
-- 1. Time columns in hypertables are NOT NULL
-- 2. Primary keys in hypertables include time column (composite primary key)
-- 3. Foreign key constraints referencing hypertables are removed
-- 4. Tables are converted to hypertables after creation

SET timezone = 'UTC';

-- Create TimescaleDB extension (must be done before creating hypertables)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Table: event_types
-- Note: Not a hypertable (metadata table)
CREATE TABLE event_types (
    event_type_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(64)
);

CREATE UNIQUE INDEX ix_event_types_event_type ON event_types(event_type);

-- Table: event_data
-- Note: Not a hypertable (metadata table)
CREATE TABLE event_data (
    data_id BIGSERIAL PRIMARY KEY,
    hash BIGINT,
    shared_data TEXT
);

CREATE INDEX ix_event_data_hash ON event_data(hash);

-- Table: events
-- Note: Will be converted to hypertable with time_fired as partition key
-- Primary key changed to composite (event_id, time_fired) for TimescaleDB compatibility
-- Note: BIGSERIAL with composite primary key requires explicit DEFAULT nextval() to maintain auto-increment
CREATE TABLE events (
    event_id BIGSERIAL DEFAULT nextval('events_event_id_seq'),
    event_type VARCHAR(64),
    event_data TEXT,
    origin VARCHAR(255),
    origin_idx SMALLINT,
    time_fired TIMESTAMP NOT NULL,  -- NOT NULL required for hypertable
    time_fired_ts DOUBLE PRECISION,
    context_id VARCHAR(36),
    context_user_id VARCHAR(36),
    context_parent_id VARCHAR(36),
    data_id BIGINT,
    context_id_bin BYTEA,
    context_user_id_bin BYTEA,
    context_parent_id_bin BYTEA,
    event_type_id BIGINT,
    PRIMARY KEY (event_id, time_fired)  -- Composite primary key required for hypertable
);

CREATE INDEX ix_events_context_id_bin ON events(context_id_bin);
CREATE INDEX ix_events_time_fired_ts ON events(time_fired_ts);
CREATE INDEX ix_events_event_type_id_time_fired_ts ON events(event_type_id, time_fired_ts);
CREATE INDEX ix_events_data_id ON events(data_id);

-- Table: state_attributes
-- Note: Not a hypertable (metadata table)
CREATE TABLE state_attributes (
    attributes_id BIGSERIAL PRIMARY KEY,
    hash BIGINT,
    shared_attrs TEXT
);

CREATE INDEX ix_state_attributes_hash ON state_attributes(hash);

-- Table: states_meta
-- Note: Not a hypertable (metadata table)
CREATE TABLE states_meta (
    metadata_id BIGSERIAL PRIMARY KEY,
    entity_id VARCHAR(255)
);

CREATE UNIQUE INDEX ix_states_meta_entity_id ON states_meta(entity_id);

-- Table: states
-- Note: Will be converted to hypertable with last_updated as partition key
-- Primary key changed to composite (state_id, last_updated) for TimescaleDB compatibility
-- Foreign key constraints to events and states (self-reference) are removed (hypertable limitation)
-- Note: BIGSERIAL with composite primary key requires explicit DEFAULT nextval() to maintain auto-increment
CREATE TABLE states (
    state_id BIGSERIAL DEFAULT nextval('states_state_id_seq'),
    entity_id VARCHAR(255),
    state VARCHAR(255),
    attributes TEXT,
    event_id BIGINT,  -- Foreign key constraint removed (references hypertable)
    last_changed TIMESTAMP,
    last_changed_ts DOUBLE PRECISION,
    last_reported_ts DOUBLE PRECISION,
    last_updated TIMESTAMP NOT NULL,  -- NOT NULL required for hypertable
    last_updated_ts DOUBLE PRECISION,
    old_state_id BIGINT,  -- Foreign key constraint removed (self-reference to hypertable)
    attributes_id BIGINT,
    context_id VARCHAR(36),
    context_user_id VARCHAR(36),
    context_parent_id VARCHAR(36),
    origin_idx SMALLINT,
    context_id_bin BYTEA,
    context_user_id_bin BYTEA,
    context_parent_id_bin BYTEA,
    metadata_id BIGINT,
    PRIMARY KEY (state_id, last_updated)  -- Composite primary key required for hypertable
);

CREATE INDEX ix_states_attributes_id ON states(attributes_id);
CREATE INDEX ix_states_last_updated_ts ON states(last_updated_ts);
CREATE INDEX ix_states_context_id_bin ON states(context_id_bin);
CREATE INDEX ix_states_metadata_id_last_updated_ts ON states(metadata_id, last_updated_ts);
CREATE INDEX ix_states_old_state_id ON states(old_state_id);

-- Table: statistics_meta
-- Note: Not a hypertable (metadata table)
CREATE TABLE statistics_meta (
    id BIGSERIAL PRIMARY KEY,
    statistic_id VARCHAR(255),
    source VARCHAR(32),
    unit_of_measurement VARCHAR(255),
    unit_class VARCHAR(255),
    has_mean BOOLEAN,
    has_sum BOOLEAN,
    name VARCHAR(255),
    mean_type SMALLINT NOT NULL
);

CREATE UNIQUE INDEX ix_statistics_meta_statistic_id ON statistics_meta(statistic_id);

-- Table: statistics
-- Note: Will be converted to hypertable with start as partition key
-- Primary key changed to composite (id, start) for TimescaleDB compatibility
-- Note: BIGSERIAL with composite primary key requires explicit DEFAULT nextval() to maintain auto-increment
CREATE TABLE statistics (
    id BIGSERIAL DEFAULT nextval('statistics_id_seq'),
    created TIMESTAMP,
    created_ts DOUBLE PRECISION,
    metadata_id BIGINT,
    start TIMESTAMP NOT NULL,  -- NOT NULL required for hypertable
    start_ts DOUBLE PRECISION,
    mean DOUBLE PRECISION,
    mean_weight DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    last_reset TIMESTAMP,
    last_reset_ts DOUBLE PRECISION,
    state DOUBLE PRECISION,
    sum DOUBLE PRECISION,
    PRIMARY KEY (id, start)  -- Composite primary key required for hypertable
);

-- Unique constraint: (metadata_id, start_ts) - maintains original uniqueness semantics
-- Note: start column is NOT included to preserve original constraint behavior
CREATE UNIQUE INDEX ix_statistics_statistic_id_start_ts ON statistics(metadata_id, start_ts);
CREATE INDEX ix_statistics_start_ts ON statistics(start_ts);

-- Table: statistics_short_term
-- Note: Will be converted to hypertable with start as partition key
-- Primary key changed to composite (id, start) for TimescaleDB compatibility
-- Note: BIGSERIAL with composite primary key requires explicit DEFAULT nextval() to maintain auto-increment
CREATE TABLE statistics_short_term (
    id BIGSERIAL DEFAULT nextval('statistics_short_term_id_seq'),
    created TIMESTAMP,
    created_ts DOUBLE PRECISION,
    metadata_id BIGINT,
    start TIMESTAMP NOT NULL,  -- NOT NULL required for hypertable
    start_ts DOUBLE PRECISION,
    mean DOUBLE PRECISION,
    mean_weight DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    last_reset TIMESTAMP,
    last_reset_ts DOUBLE PRECISION,
    state DOUBLE PRECISION,
    sum DOUBLE PRECISION,
    PRIMARY KEY (id, start)  -- Composite primary key required for hypertable
);

-- Unique constraint: (metadata_id, start_ts) - maintains original uniqueness semantics
-- Note: start column is NOT included to preserve original constraint behavior
CREATE UNIQUE INDEX ix_statistics_short_term_statistic_id_start_ts ON statistics_short_term(metadata_id, start_ts);
CREATE INDEX ix_statistics_short_term_start_ts ON statistics_short_term(start_ts);

-- Table: recorder_runs
-- Note: Not a hypertable (management table)
CREATE TABLE recorder_runs (
    run_id BIGSERIAL PRIMARY KEY,
    start TIMESTAMP NOT NULL,
    "end" TIMESTAMP,
    closed_incorrect BOOLEAN NOT NULL,
    created TIMESTAMP NOT NULL
);

CREATE INDEX ix_recorder_runs_start_end ON recorder_runs(start, "end");

-- Table: statistics_runs
-- Note: Not a hypertable (management table)
CREATE TABLE statistics_runs (
    run_id BIGSERIAL PRIMARY KEY,
    start TIMESTAMP NOT NULL
);

CREATE INDEX ix_statistics_runs_start ON statistics_runs(start);

-- Table: schema_changes
-- Note: Not a hypertable (management table)
CREATE TABLE schema_changes (
    change_id BIGSERIAL PRIMARY KEY,
    schema_version INTEGER,
    changed TIMESTAMP NOT NULL
);

-- Table: migration_changes
-- Note: Not a hypertable (management table)
CREATE TABLE migration_changes (
    migration_id VARCHAR(255) PRIMARY KEY,
    version SMALLINT NOT NULL
);

-- Foreign Key Constraints
-- Note: Only constraints that do NOT reference hypertables are kept

-- events → event_data (event_data is not a hypertable, constraint kept)
ALTER TABLE events
    ADD CONSTRAINT fk_events_data_id
    FOREIGN KEY (data_id) REFERENCES event_data(data_id);

-- events → event_types (event_types is not a hypertable, constraint kept)
ALTER TABLE events
    ADD CONSTRAINT fk_events_event_type_id
    FOREIGN KEY (event_type_id) REFERENCES event_types(event_type_id);

-- states → events (REMOVED: events is a hypertable, cannot be referenced by foreign key)
-- states → states (REMOVED: self-reference to hypertable, cannot be referenced by foreign key)

-- states → state_attributes (state_attributes is not a hypertable, constraint kept)
ALTER TABLE states
    ADD CONSTRAINT fk_states_attributes_id
    FOREIGN KEY (attributes_id) REFERENCES state_attributes(attributes_id);

-- states → states_meta (states_meta is not a hypertable, constraint kept)
ALTER TABLE states
    ADD CONSTRAINT fk_states_metadata_id
    FOREIGN KEY (metadata_id) REFERENCES states_meta(metadata_id);

-- statistics → statistics_meta (statistics_meta is not a hypertable, constraint kept)
ALTER TABLE statistics
    ADD CONSTRAINT fk_statistics_metadata_id
    FOREIGN KEY (metadata_id) REFERENCES statistics_meta(id)
    ON DELETE CASCADE;

-- statistics_short_term → statistics_meta (statistics_meta is not a hypertable, constraint kept)
ALTER TABLE statistics_short_term
    ADD CONSTRAINT fk_statistics_short_term_metadata_id
    FOREIGN KEY (metadata_id) REFERENCES statistics_meta(id)
    ON DELETE CASCADE;

-- ============================================================================
-- TimescaleDB Hypertable Conversion
-- ============================================================================
-- Convert tables to hypertables AFTER all tables and constraints are created
-- Hypertables must be empty or have data inserted after conversion
--
-- IMPORTANT: Data migration should happen AFTER hypertable conversion
-- ============================================================================

-- Convert events table to hypertable
-- Chunk interval: 7 days (balances query performance and write efficiency)
SELECT create_hypertable(
    'events',
    'time_fired',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Convert states table to hypertable
-- Chunk interval: 7 days (balances query performance and write efficiency)
SELECT create_hypertable(
    'states',
    'last_updated',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Convert statistics table to hypertable
-- Chunk interval: 1 day (statistics are typically queried by day)
SELECT create_hypertable(
    'statistics',
    'start',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Convert statistics_short_term table to hypertable
-- Chunk interval: 1 hour (short-term statistics are queried more frequently)
SELECT create_hypertable(
    'statistics_short_term',
    'start',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ============================================================================
-- Optional: Data Retention Policies
-- ============================================================================
-- Uncomment and adjust intervals as needed for your use case
-- ============================================================================

-- Automatically delete events older than 1 year
-- SELECT add_retention_policy('events', INTERVAL '1 year');

-- Automatically delete states older than 1 year
-- SELECT add_retention_policy('states', INTERVAL '1 year');

-- Automatically delete statistics older than 5 years
-- SELECT add_retention_policy('statistics', INTERVAL '5 years');

-- Automatically delete short-term statistics older than 30 days
-- SELECT add_retention_policy('statistics_short_term', INTERVAL '30 days');

-- ============================================================================
-- Optional: Compression Policies
-- ============================================================================
-- Uncomment and adjust as needed for your use case
-- Compression reduces storage and can improve query performance for historical data
-- ============================================================================

-- Enable compression for events table (compress data older than 30 days)
-- ALTER TABLE events SET (
--     timescaledb.compress,
--     timescaledb.compress_segmentby = 'event_type_id'
-- );
-- SELECT add_compression_policy('events', INTERVAL '30 days');

-- Enable compression for states table (compress data older than 30 days)
-- ALTER TABLE states SET (
--     timescaledb.compress,
--     timescaledb.compress_segmentby = 'metadata_id'
-- );
-- SELECT add_compression_policy('states', INTERVAL '30 days');

-- Enable compression for statistics table (compress data older than 7 days)
-- ALTER TABLE statistics SET (
--     timescaledb.compress,
--     timescaledb.compress_segmentby = 'metadata_id'
-- );
-- SELECT add_compression_policy('statistics', INTERVAL '7 days');

-- Enable compression for statistics_short_term table (compress data older than 1 day)
-- ALTER TABLE statistics_short_term SET (
--     timescaledb.compress,
--     timescaledb.compress_segmentby = 'metadata_id'
-- );
-- SELECT add_compression_policy('statistics_short_term', INTERVAL '1 day');

