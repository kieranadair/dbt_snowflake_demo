WITH CTE AS (
    SELECT *
    FROM {{ source('demo_schema', 'bike_raw') }}
    LIMIT 10
)

SELECT *
FROM CTE;