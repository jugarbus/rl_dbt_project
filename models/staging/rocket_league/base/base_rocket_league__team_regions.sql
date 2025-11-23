{{
  config(
    materialized='view'
  )
}}

WITH src_games_teams AS (
    SELECT * 
    FROM {{ source('rocket_league', 'raw_games_teams') }}
),

normalized AS (
    SELECT DISTINCT
    LOWER(COALESCE(TRIM(team_region), '{{ var("unknown_country_code") }}'))::varchar AS team_region_clean

    FROM src_games_teams
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['team_region_clean']) }}::varchar AS team_region_id,
        team_region_clean AS team_region_name
    FROM normalized
)

SELECT * FROM surrogate




