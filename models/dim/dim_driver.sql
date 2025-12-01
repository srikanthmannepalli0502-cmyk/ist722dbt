{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY TRIP_KEY) AS driver_id,
    'Driver_' || TRIP_KEY AS driver_name,
    'New York' AS city,
    CASE
        WHEN TRIP_KEY % 7 = 0 THEN 'UberBlack'
        WHEN TRIP_KEY % 5 = 0 THEN 'UberXL'
        ELSE 'UberX'
    END AS vehicle_type,
    ROUND(UNIFORM(4.0, 5.0, RANDOM()), 2) AS avg_rating
FROM {{ ref('stg_trip') }}
GROUP BY TRIP_KEY
ORDER BY TRIP_KEY;
