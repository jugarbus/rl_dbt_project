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
    LOWER(COALESCE(TRIM(team_region::varchar), '{{ var("unknown_var") }}')) AS team_region_clean,
    CONVERT_TIMEZONE('UTC', data_load) AS data_load

    FROM src_games_teams
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['team_region_clean']) }} AS team_region_id,
        team_region_clean AS team_region_name,
        data_load
    FROM normalized
)

SELECT * FROM surrogate




