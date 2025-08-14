WITH bike_raw AS (
    
    SELECT ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_station_id,
        end_station_id,
        member_casual
    FROM {{ source("demo_schema", "bike_raw") }}
    LIMIT 10

),

bike_transform AS (

    SELECT ride_id,
        rideable_type,
        member_casual AS membership_type,
        TO_DATE(started_at) AS trip_date,
        TO_TIME(started_at) AS trip_time,
        start_station_id,
        end_station_id,
        DATEDIFF('second', started_at, ended_at) AS trip_duration_seconds
    FROM bike_raw

)


SELECT ride_id,
    rideable_type,
    membership_type,
    trip_date,
    trip_time,
    start_station_id,
    end_station_id,
    trip_duration_seconds
FROM bike_transform
