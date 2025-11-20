-- Файл создаёт базовую схему и таблицы климатического хранилища.

CREATE SCHEMA IF NOT EXISTS climate;
SET search_path TO climate;

CREATE TABLE IF NOT EXISTS locations (
    location_id SERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    elevation DOUBLE PRECISION,
    timezone TEXT,
    timezone_abbreviation TEXT,
    raw_location_hash TEXT UNIQUE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_locations_lat_lon
    ON locations (latitude, longitude);

CREATE TABLE IF NOT EXISTS raw_climate (
    id BIGSERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(location_id),
    observed_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    source_file TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_climate_location_time
    ON raw_climate (location_id, observed_at);

CREATE TABLE IF NOT EXISTS climate_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(location_id),
    period_start DATE NOT NULL,
    period_level TEXT NOT NULL,
    avg_temperature DOUBLE PRECISION,
    total_precipitation DOUBLE PRECISION,
    aggregation_window TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(location_id, period_start, period_level, aggregation_window)
);
