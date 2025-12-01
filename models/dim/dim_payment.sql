{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY payment_type) AS payment_id,
    payment_type
FROM (
    SELECT DISTINCT payment_type
    FROM {{ ref('stg_trip') }}
)
ORDER BY payment_type;
