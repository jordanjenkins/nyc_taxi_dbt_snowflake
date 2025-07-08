{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        unique_key='trip_id',
        partition_by={
            'field': 'pickup_datetime',
            'data_type': 'date',
            'granularity': 'month'
        }
    )
}}

WITH source_data AS (
    SELECT
        *,
        MD5(TO_VARCHAR(tpep_pickup_datetime) || TO_VARCHAR(tpep_dropoff_datetime) || TO_VARCHAR(vendorid)) AS trip_id
    FROM {{ source('raw', 'yellow_taxi_trips') }}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('month', pickup_datetime) = DATE_TRUNC('month', CURRENT_DATE())
    {% endif %}
)

SELECT
    trip_id,
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee
FROM source_data