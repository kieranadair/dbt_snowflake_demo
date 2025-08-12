WITH bike_data AS (
    SELECT *
    FROM {{ source('demo_schema', 'bronze_bike_data') }}
    LIMIT 10
)

SELECT *
FROM bike_data;