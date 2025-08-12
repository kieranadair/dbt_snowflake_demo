WITH CTE AS (
    SELECT TO_DATE(observation_time) as OBSERVATION_DATE,
    weather_description,
    cloud,
    humidity,
    temperature
    FROM {{ source('demo_schema', 'weather_silver') }}
),


AVG_DAILY_WEATHER AS (
    SELECT observation_date,
    AVG(cloud) AS AVG_CLOUDS,
    AVG(humidity) AS AVG_HUMIDITY,
    AVG(temperature) AS AVG_TEMPERATURE
    from CTE
    GROUP BY observation_date
),

DAILY_DESCRIPTION AS(
    SELECT observation_date,
    weather_description,
    COUNT(weather_description) as description_count,
    ROW_NUMBER() OVER (PARTITION BY observation_date ORDER BY description_count DESC) AS row_number
    FROM CTE
    GROUP BY observation_date, weather_description
    QUALIFY row_number = 1
)

SELECT a.observation_date,
a.avg_clouds,
a.avg_humidity,
a.avg_temperature,
b.weather_description
FROM AVG_DAILY_WEATHER AS a
INNER JOIN DAILY_DESCRIPTION AS b
    ON a.observation_date = b.observation_date