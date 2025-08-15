WITH date_data AS (
    SELECT date_started_at AS date_actual,
        daytype_started_at AS date_daytype,
        season_started_at AS date_season
    FROM {{ ref('date_dimension') }}
),

weather_daily AS (
    SELECT observation_date AS date_actual,
        avg_temperature,
        weather_description
    FROM {{ ref('weather_daily') }}
),

biketrip_daily AS (
    SELECT trip_date AS date_actual,
        COUNT(ride_id) AS trip_count
    FROM {{ ref('trip_fact') }}
    GROUP BY trip_date
)

SELECT dates.date_actual,
    dates.date_daytype,
    dates.date_season,
    weather.avg_temperature,
    weather.weather_description,
    bike.trip_count
FROM date_data AS dates
INNER JOIN weather_daily AS weather
    ON dates.date_actual = weather.date_actual
INNER JOIN biketrip_daily AS bike
    ON dates.date_actual = bike.date_actual

