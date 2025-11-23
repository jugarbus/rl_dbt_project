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
    LOWER(COALESCE(TRIM(event_region), '{{ var("unknown_country_code") }}'))::varchar AS event_region_clean

    FROM src_games_teams
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['event_region_clean']) }}::varchar AS event_region_id,
        event_region_clean AS event_region_name
    FROM normalized
)

SELECT * FROM surrogate




