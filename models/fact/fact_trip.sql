{{ config(materialized='table') }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_trip') }}
),

dim_date AS (
    SELECT date_id, full_date FROM {{ ref('dim_date') }}
),

dim_location AS (
    SELECT location_id, zipcode, latitude, longitude
    FROM {{ ref('dim_location') }}
),

dim_payment AS (
    SELECT payment_id, payment_type
    FROM {{ ref('dim_payment') }}
),

dim_rider AS (
    SELECT rider_id
    FROM {{ ref('dim_rider') }}
),

dim_driver AS (
    SELECT driver_id
    FROM {{ ref('dim_driver') }}
)

SELECT
    -- surrogate fact id auto in Snowflake, not needed here
    d1.date_id AS pickup_date_id,
    d2.date_id AS dropoff_date_id,
    r.rider_id,
    dr.driver_id,
    l1.location_id AS pickup_location_id,
    l2.location_id AS dropoff_location_id,
    p.payment_id,
    stg.fare_amount,
    stg.trip_distance,
    stg.passenger_count,
    DATEDIFF(minute, stg.pickup_datetime, stg.dropoff_datetime) AS trip_duration_min

FROM stg
LEFT JOIN dim_date d1
    ON d1.full_date = TO_DATE(stg.pickup_datetime)

LEFT JOIN dim_date d2
    ON d2.full_date = TO_DATE(stg.dropoff_datetime)

LEFT JOIN dim_location l1
    ON l1.latitude = stg.pickup_latitude
    AND l1.longitude = stg.pickup_longitude

LEFT JOIN dim_location l2
    ON l2.latitude = stg.dropoff_latitude
    AND l2.longitude = stg.dropoff_longitude

LEFT JOIN dim_payment p
    ON p.payment_type = stg.payment_type

LEFT JOIN dim_rider r
    ON r.rider_id = (ABS(MOD(HASH(stg.pickup_datetime), 1480000)) + 1)

LEFT JOIN dim_driver dr
    ON dr.driver_id = (ABS(MOD(HASH(stg.dropoff_datetime), 74000)) + 1);
