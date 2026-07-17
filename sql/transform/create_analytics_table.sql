CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.cta_ridership_daily (
    station_id INTEGER NOT NULL,
    stationname TEXT NOT NULL,
    ride_date DATE NOT NULL,
    daytype CHAR(1) NOT NULL,
    day_type_desc TEXT NOT NULL,
    rides INTEGER NOT NULL,
    PRIMARY KEY (station_id, ride_date)
);

TRUNCATE TABLE analytics.cta_ridership_daily;

INSERT INTO analytics.cta_ridership_daily
    (station_id, stationname, ride_date, daytype, day_type_desc, rides)
SELECT
    station_id,
    MAX(stationname)                                   AS stationname,
    ride_date,
    MAX(daytype)                                        AS daytype,
    CASE MAX(daytype)
        WHEN 'W' THEN 'Weekday'
        WHEN 'A' THEN 'Saturday'
        WHEN 'U' THEN 'Sunday/Holiday'
        ELSE 'Unknown'
    END                                                  AS day_type_desc,
    ROUND(AVG(REPLACE(rides, ',', '')::INTEGER))::INTEGER AS rides
FROM (
    SELECT
        station_id,
        stationname,
        TO_DATE(ride_date, 'MM/DD/YYYY') AS ride_date,
        daytype,
        rides
    FROM staging.cta_ridership_raw
) cleaned
GROUP BY station_id, ride_date;