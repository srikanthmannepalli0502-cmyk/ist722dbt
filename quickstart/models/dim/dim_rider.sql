{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY TRIP_KEY) AS rider_id,
    CASE
        WHEN TRIP_KEY % 10 = 0 THEN 'VIP Rider'
        WHEN TRIP_KEY % 3 = 0 THEN 'Returning Rider'
        ELSE 'New Rider'
    END AS rider_category,
    ROUND(UNIFORM(3.5, 5.0, RANDOM()), 2) AS avg_rating
FROM {{ ref('stg_trip') }}
GROUP BY TRIP_KEY
ORDER BY TRIP_KEY;
