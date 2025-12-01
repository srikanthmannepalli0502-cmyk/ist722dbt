SELECT
    KEY AS TRIP_KEY,
    FARE_AMOUNT,

    -- Convert text timestamp to real timestamp
    CAST(PICKUP_DATETIME AS TIMESTAMP_NTZ) AS PICKUP_DATETIME,

    -- Synthetic dropoff datetime
    DATEADD(
        minute,
        ROUND(UNIFORM(5, 30, RANDOM())),
        CAST(PICKUP_DATETIME AS TIMESTAMP_NTZ)
    ) AS DROPOFF_DATETIME,

    PICKUP_LONGITUDE,
    PICKUP_LATITUDE,
    DROPOFF_LONGITUDE,
    DROPOFF_LATITUDE,
    PASSENGER_COUNT,
    ROUND(UNIFORM(1, 10, RANDOM()), 2) AS TRIP_DISTANCE,

    -- synthetic payment type
    CASE
        WHEN MOD(KEY, 3) = 0 THEN 'Cash'
        WHEN MOD(KEY, 3) = 1 THEN 'Card'
        ELSE 'Digital Wallet'
    END AS PAYMENT_TYPE

FROM {{ source('uber_raw', 'UBER_RAW') }}
