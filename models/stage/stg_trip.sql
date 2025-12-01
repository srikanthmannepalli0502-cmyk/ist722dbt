SELECT
    KEY AS TRIP_KEY,
    FARE_AMOUNT,

    -- FIXED: convert 'YYYY-MM-DD HH:MM:SS UTC' correctly
    TO_TIMESTAMP_NTZ(REPLACE(PICKUP_DATETIME, ' UTC', '')) AS PICKUP_DATETIME,

    -- Synthetic dropoff datetime
    DATEADD(
        minute,
        ROUND(UNIFORM(5, 30, RANDOM())),
        TO_TIMESTAMP_NTZ(REPLACE(PICKUP_DATETIME, ' UTC', ''))
    ) AS DROPOFF_DATETIME,

    PICKUP_LONGITUDE,
    PICKUP_LATITUDE,
    DROPOFF_LONGITUDE,
    DROPOFF_LATITUDE,
    PASSENGER_COUNT,
    ROUND(UNIFORM(1, 10, RANDOM()), 2) AS TRIP_DISTANCE,

    CASE
        WHEN MOD(KEY, 3) = 0 THEN 'Cash'
        WHEN MOD(KEY, 3) = 1 THEN 'Card'
        ELSE 'Digital Wallet'
    END AS PAYMENT_TYPE

FROM {{ source('uber_raw', 'UBER_RAW') }}