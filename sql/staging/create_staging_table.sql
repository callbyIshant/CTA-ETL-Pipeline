CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.cta_ridership_raw (
    station_id INTEGER,
    stationname TEXT,
    ride_date TEXT,        -- kept as raw MM/DD/YYYY string; cast happens in transform
    daytype TEXT,
    rides TEXT,            -- kept as raw string; cast to INTEGER in transform
    loaded_at TIMESTAMP DEFAULT now()
);