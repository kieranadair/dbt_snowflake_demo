WITH bike_start_times AS (

    SELECT started_at
    FROM {{ source('demo_schema', 'bike_raw') }}

),

individual_dates AS (

    SELECT DISTINCT DATE(started_at) AS date_started_at,
    FROM bike_start_times

),


date_details AS (

    SELECT date_started_at,
        DAYNAME(date_started_at) AS weekday_started_at,
        {{weekday_or_weekend('DATE_STARTED_AT')}} AS daytype_started_at,
        {{season('DATE_STARTED_AT')}} AS season_started_at
    FROM individual_dates

)


SELECT date_started_at,
    weekday_started_at,
    daytype_started_at,
    season_started_at,
FROM date_details
ORDER BY date_started_at ASC