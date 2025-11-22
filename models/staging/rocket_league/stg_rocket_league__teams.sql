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
        TRIM(team_id)::varchar AS team_id,
        TRIM(team_slug)::varchar AS team_slug,
        TRIM(team_name)::varchar AS team_name,
    COALESCE(TRIM(team_region), '{{ var("unknown_country_code") }}')::varchar AS team_region_clean

    FROM src_games_teams
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['team_id']) }} AS team_id,
        team_slug,
        team_name,
        {{ dbt_utils.generate_surrogate_key(['team_region_clean']) }}::varchar AS team_region_id
    FROM normalized
)

SELECT * FROM surrogate



