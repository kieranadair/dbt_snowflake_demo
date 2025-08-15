WITH bike_raw AS (
    
    SELECT started_at,
        ended_at,
        start_station_id,
        end_station_id,
        user_type
    FROM {{ source("demo_schema", "bike_raw") }}
    LIMIT 10

),

bike_transform AS (

    SELECT {{ dbt_utils.generate_surrogate_key(['started_at', 'ended_at', 'start_station_id', 'end_station_id']) }} AS ride_id,
        user_type AS membership_type,
        TO_DATE(started_at) AS trip_date,
        TO_TIME(started_at) AS trip_time,
        start_station_id,
        end_station_id,
        DATEDIFF('second', started_at, ended_at) AS trip_duration_seconds
    FROM bike_raw

)


SELECT ride_id,
    membership_type,
    trip_date,
    trip_time,
    start_station_id,
    end_station_id,
    trip_duration_seconds
FROM bike_transform
