{{ config(
    materialized='view'
) }}

SELECT
    KEY AS TRIP_KEY,
    FARE_AMOUNT,
    PICKUP_DATETIME,

    -- No DROPOFF_DATETIME in RAW, so calculate it
    DATEADD(
        minute,
        ROUND(UNIFORM(5, 30, RANDOM())),   -- random 5–30 min trip duration
        PICKUP_DATETIME
    ) AS DROPOFF_DATETIME,

    PICKUP_LONGITUDE,
    PICKUP_LATITUDE,
    DROPOFF_LONGITUDE,
    DROPOFF_LATITUDE,
    PASSENGER_COUNT
FROM {{ source('uber_raw', 'UBER_RAW') }}
