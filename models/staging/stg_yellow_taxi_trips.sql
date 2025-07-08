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
        TO_TIMESTAMP_NTZ("tpep_pickup_datetime" / 1000000) AS pickup_datetime,
        TO_TIMESTAMP_NTZ("tpep_dropoff_datetime" / 1000000) AS dropoff_datetime,
        MD5(TO_VARCHAR("tpep_pickup_datetime") || TO_VARCHAR("tpep_dropoff_datetime") || TO_VARCHAR("VendorID")) AS trip_id
    FROM {{ source('raw', 'yellow_taxi_trips') }}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('month', TO_TIMESTAMP_NTZ("tpep_pickup_datetime" / 1000000)) = DATE_TRUNC('month', CURRENT_DATE())
    {% endif %}
)

SELECT
    trip_id,
    "VendorID" AS vendorid,
    pickup_datetime,
    dropoff_datetime,
    "passenger_count" AS passenger_count,
    "trip_distance" AS trip_distance,
    "RatecodeID" AS ratecodeid,
    "store_and_fwd_flag" AS store_and_fwd_flag,
    "PULocationID" AS pulocationid,
    "DOLocationID" AS dolocationid,
    "payment_type" AS payment_type,
    "fare_amount" AS fare_amount,
    "extra" AS extra,
    "mta_tax" AS mta_tax,
    "tip_amount" AS tip_amount,
    "tolls_amount" AS tolls_amount,
    "improvement_surcharge" AS improvement_surcharge,
    "total_amount" AS total_amount,
    "congestion_surcharge" AS congestion_surcharge,
    "Airport_fee" AS airport_fee,
    "cbd_congestion_fee" AS cbd_congestion_fee,
FROM source_data
