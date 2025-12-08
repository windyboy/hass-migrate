-- Home Assistant PostgreSQL Schema
-- Converted from MySQL/MariaDB for optimal PostgreSQL performance
-- Version: 1.0
-- Date: 2024-12-08

SET timezone = 'UTC';

-- Table: event_types
CREATE TABLE event_types (
    event_type_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(64)
);

CREATE UNIQUE INDEX ix_event_types_event_type ON event_types(event_type);

-- Table: event_data
CREATE TABLE event_data (
    data_id BIGSERIAL PRIMARY KEY,
    hash INTEGER,
    shared_data TEXT
);

CREATE INDEX ix_event_data_hash ON event_data(hash);

-- Table: events
CREATE TABLE events (
    event_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(64),
    event_data TEXT,
    origin VARCHAR(255),
    origin_idx SMALLINT,
    time_fired TIMESTAMP,
    time_fired_ts DOUBLE PRECISION,
    context_id VARCHAR(36),
    context_user_id VARCHAR(36),
    context_parent_id VARCHAR(36),
    data_id BIGINT,
    context_id_bin BYTEA,
    context_user_id_bin BYTEA,
    context_parent_id_bin BYTEA,
    event_type_id BIGINT
);

CREATE INDEX ix_events_context_id_bin ON events(context_id_bin);
CREATE INDEX ix_events_time_fired_ts ON events(time_fired_ts);
CREATE INDEX ix_events_event_type_id_time_fired_ts ON events(event_type_id, time_fired_ts);
CREATE INDEX ix_events_data_id ON events(data_id);

-- Table: state_attributes
CREATE TABLE state_attributes (
    attributes_id BIGSERIAL PRIMARY KEY,
    hash INTEGER,
    shared_attrs TEXT
);

CREATE INDEX ix_state_attributes_hash ON state_attributes(hash);

-- Table: states_meta
CREATE TABLE states_meta (
    metadata_id BIGSERIAL PRIMARY KEY,
    entity_id VARCHAR(255)
);

CREATE UNIQUE INDEX ix_states_meta_entity_id ON states_meta(entity_id);

-- Table: states
CREATE TABLE states (
    state_id BIGSERIAL PRIMARY KEY,
    entity_id VARCHAR(255),
    state VARCHAR(255),
    attributes TEXT,
    event_id SMALLINT,
    last_changed TIMESTAMP,
    last_changed_ts DOUBLE PRECISION,
    last_reported_ts DOUBLE PRECISION,
    last_updated TIMESTAMP,
    last_updated_ts DOUBLE PRECISION,
    old_state_id BIGINT,
    attributes_id BIGINT,
    context_id VARCHAR(36),
    context_user_id VARCHAR(36),
    context_parent_id VARCHAR(36),
    origin_idx SMALLINT,
    context_id_bin BYTEA,
    context_user_id_bin BYTEA,
    context_parent_id_bin BYTEA,
    metadata_id BIGINT
);

CREATE INDEX ix_states_attributes_id ON states(attributes_id);
CREATE INDEX ix_states_last_updated_ts ON states(last_updated_ts);
CREATE INDEX ix_states_context_id_bin ON states(context_id_bin);
CREATE INDEX ix_states_metadata_id_last_updated_ts ON states(metadata_id, last_updated_ts);
CREATE INDEX ix_states_old_state_id ON states(old_state_id);

-- Table: statistics_meta
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
CREATE TABLE statistics (
    id BIGSERIAL PRIMARY KEY,
    created TIMESTAMP,
    created_ts DOUBLE PRECISION,
    metadata_id BIGINT,
    start TIMESTAMP,
    start_ts DOUBLE PRECISION,
    mean DOUBLE PRECISION,
    mean_weight DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    last_reset TIMESTAMP,
    last_reset_ts DOUBLE PRECISION,
    state DOUBLE PRECISION,
    sum DOUBLE PRECISION
);

CREATE UNIQUE INDEX ix_statistics_statistic_id_start_ts ON statistics(metadata_id, start_ts);
CREATE INDEX ix_statistics_start_ts ON statistics(start_ts);

-- Table: statistics_short_term
CREATE TABLE statistics_short_term (
    id BIGSERIAL PRIMARY KEY,
    created TIMESTAMP,
    created_ts DOUBLE PRECISION,
    metadata_id BIGINT,
    start TIMESTAMP,
    start_ts DOUBLE PRECISION,
    mean DOUBLE PRECISION,
    mean_weight DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    last_reset TIMESTAMP,
    last_reset_ts DOUBLE PRECISION,
    state DOUBLE PRECISION,
    sum DOUBLE PRECISION
);

CREATE UNIQUE INDEX ix_statistics_short_term_statistic_id_start_ts ON statistics_short_term(metadata_id, start_ts);
CREATE INDEX ix_statistics_short_term_start_ts ON statistics_short_term(start_ts);

-- Table: recorder_runs
CREATE TABLE recorder_runs (
    run_id BIGSERIAL PRIMARY KEY,
    start TIMESTAMP NOT NULL,
    "end" TIMESTAMP,
    closed_incorrect BOOLEAN NOT NULL,
    created TIMESTAMP NOT NULL
);

CREATE INDEX ix_recorder_runs_start_end ON recorder_runs(start, "end");

-- Table: statistics_runs
CREATE TABLE statistics_runs (
    run_id BIGSERIAL PRIMARY KEY,
    start TIMESTAMP NOT NULL
);

CREATE INDEX ix_statistics_runs_start ON statistics_runs(start);

-- Table: schema_changes
CREATE TABLE schema_changes (
    change_id BIGSERIAL PRIMARY KEY,
    schema_version INTEGER,
    changed TIMESTAMP NOT NULL
);

-- Table: migration_changes
CREATE TABLE migration_changes (
    migration_id VARCHAR(255) PRIMARY KEY,
    version SMALLINT NOT NULL
);

-- Foreign Key Constraints
ALTER TABLE events
    ADD CONSTRAINT fk_events_data_id
    FOREIGN KEY (data_id) REFERENCES event_data(data_id);

ALTER TABLE events
    ADD CONSTRAINT fk_events_event_type_id
    FOREIGN KEY (event_type_id) REFERENCES event_types(event_type_id);

ALTER TABLE states
    ADD CONSTRAINT fk_states_old_state_id
    FOREIGN KEY (old_state_id) REFERENCES states(state_id);

ALTER TABLE states
    ADD CONSTRAINT fk_states_attributes_id
    FOREIGN KEY (attributes_id) REFERENCES state_attributes(attributes_id);

ALTER TABLE states
    ADD CONSTRAINT fk_states_metadata_id
    FOREIGN KEY (metadata_id) REFERENCES states_meta(metadata_id);

ALTER TABLE statistics
    ADD CONSTRAINT fk_statistics_metadata_id
    FOREIGN KEY (metadata_id) REFERENCES statistics_meta(id)
    ON DELETE CASCADE;

ALTER TABLE statistics_short_term
    ADD CONSTRAINT fk_statistics_short_term_metadata_id
    FOREIGN KEY (metadata_id) REFERENCES statistics_meta(id)
    ON DELETE CASCADE;
