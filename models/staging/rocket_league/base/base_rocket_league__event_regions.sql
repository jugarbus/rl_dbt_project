{{ config(
    materialized='view' 
) }}

WITH src_main AS (
    SELECT * FROM {{ source('rocket_league', 'raw_main') }}
),

normalized AS (
    SELECT 
        LOWER(COALESCE(TRIM(event_region::varchar), '{{ var("unknown_var", "unknown") }}')) AS event_region_clean,
        CONVERT_TIMEZONE('UTC', data_load) AS data_load
    FROM src_main
),

unique_regions AS (
    SELECT 
        event_region_clean,
        MAX(data_load) AS data_load
    FROM normalized
    GROUP BY event_region_clean
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['event_region_clean']) }} AS event_region_id,
        event_region_clean AS event_region_name,
        data_load
    FROM unique_regions
)

SELECT * FROM surrogate