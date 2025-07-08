{{ config(
    materialized='view'
) }}

-- This model selects data from the taxi_zone_lookup source table
-- and renames the columns to match the expected schema for the dimension table.
SELECT
    LocationID AS location_id,
    Borough AS borough,
    Zone AS zone,
    service_zone AS service_zone
FROM {{ ref('taxi_zone_lookup') }}
