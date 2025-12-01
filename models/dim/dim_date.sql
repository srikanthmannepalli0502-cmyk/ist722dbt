{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY full_date) AS date_id,
    full_date,
    DAYNAME(full_date) AS day_of_week,
    EXTRACT(month FROM full_date) AS month,
    EXTRACT(year FROM full_date) AS year,
    CASE WHEN DAYNAME(full_date) IN ('Saturday', 'Sunday') THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday
FROM (
    SELECT DISTINCT DATE(PICKUP_DATETIME) AS full_date
    FROM {{ ref('stg_trip') }}
) d
ORDER BY full_date;
