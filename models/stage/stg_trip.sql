{{ config(materialized='view') }}

SELECT
    KEY AS trip_key,
    fare_amount,
    pickup_datetime,
    DATEADD(
        minute,
        ROUND(UNIFORM(5, 30, RANDOM())),
        pickup_datetime
    ) AS dropoff_datetime,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    passenger_count,
    payment_type,
    trip_distance
FROM {{ source('uber_raw', 'UBER_RAW') }}