{{
  config(
    materialized='view'
  )
}}

WITH src_games_teams AS (
    SELECT * 
    FROM {{ source('rocket_league', 'raw_main') }}
),

normalized AS (
    SELECT DISTINCT
    LOWER(COALESCE(TRIM(event_region::varchar), '{{ var("unknown_var") }}')) AS event_region_clean,
    CONVERT_TIMEZONE('UTC', data_load) AS data_load

    FROM src_games_teams
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['event_region_clean']) }} AS event_region_id,
        event_region_clean AS event_region_name,
        data_load
    FROM normalized
)

SELECT * FROM surrogate




