{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY pickup_latitude, pickup_longitude) AS location_id,
    pickup_longitude AS longitude,
    pickup_latitude AS latitude,
    'New York' AS city,
    'NY' AS region
FROM (
    SELECT DISTINCT
        pickup_longitude,
        pickup_latitude
    FROM {{ ref('stg_trip') }}
)
ORDER BY pickup_latitude, pickup_longitude;
