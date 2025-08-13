WITH BIKE_RAW AS (
    SELECT start_station_name,
    start_station_id,
    start_lat,
    start_lng,
    end_station_name,
    end_station_id,
    end_lat,
    end_lng
    FROM {{ source('demo_schema', 'bike_raw') }}
),

START_STATIONS AS (
    SELECT
    start_station_name AS station_name,
    start_station_id AS station_id,
    start_lat AS station_lat,
    start_lng AS station_lng
    FROM BIKE_RAW
    WHERE start_station_name IS NOT NULL
        AND start_station_id IS NOT NULL
        AND start_lat IS NOT NULL
        AND start_lng IS NOT NULL
),

END_STATIONS AS (
    SELECT
    end_station_name AS station_name,
    end_station_id AS station_id,
    end_lat AS station_lat,
    end_lng AS station_lng
    FROM BIKE_RAW
    WHERE end_station_name IS NOT NULL
        AND end_station_id IS NOT NULL
        AND end_lat IS NOT NULL
        AND end_lng IS NOT NULL
),

ALL_STATIONS AS (
    SELECT station_name,
    station_id,
    station_lat,
    station_lng
    FROM START_STATIONS
    
    UNION ALL
    
    SELECT station_name,
    station_id,
    station_lat,
    station_lng
    FROM END_STATIONS
)

SELECT station_name,
    station_id,
    AVG(station_lat) as station_lat,
    AVG(station_lng) as station_lon
FROM ALL_STATIONS
GROUP BY station_name, station_id