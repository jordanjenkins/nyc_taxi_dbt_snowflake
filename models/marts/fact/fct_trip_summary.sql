{{ config(
    materialized='incremental',
    unique_key='trip_hour || pickup_borough || dropoff_borough',
    incremental_strategy='insert_overwrite',
    partition_by={
        'field': 'trip_hour',
        'data_type': 'timestamp',
        'granularity': 'month'
    }
) }}

WITH enriched_trips AS (
    SELECT
        DATE_TRUNC('hour', pickup_datetime) AS trip_hour,
        pickup_zone.borough AS pickup_borough,
        dropoff_zone.borough AS dropoff_borough,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount
    FROM {{ ref('stg_yellow_taxi_trips') }} trips
    LEFT JOIN {{ ref('dim_zone') }} pickup_zone
        ON trips.pulocationid = pickup_zone.location_id
    LEFT JOIN {{ ref('dim_zone') }} dropoff_zone
        ON trips.dolocationid = dropoff_zone.location_id
    -- {% if is_incremental() %}
    --   WHERE DATE_TRUNC('month', pickup_datetime) >= DATEADD('month', -1, CURRENT_DATE)
    -- {% endif %}
),

aggregated AS (
    SELECT
        trip_hour,
        pickup_borough,
        dropoff_borough,
        COUNT(*) AS total_trips,
        AVG(trip_distance) AS avg_trip_distance,
        AVG(fare_amount) AS avg_fare_amount,
        AVG(tip_amount) AS avg_tip_amount,
        AVG(total_amount) AS avg_revenue
    FROM enriched_trips
    GROUP BY trip_hour, pickup_borough, dropoff_borough
)

SELECT * FROM aggregated
