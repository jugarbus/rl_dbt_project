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
        LOWER(TRIM(team_id::varchar)) AS team_id_clean,
        TRIM(team_slug::varchar) AS team_url_clean,
        TRIM(team_name::varchar) AS team_name_clean,
        LOWER(COALESCE(TRIM(team_region::varchar), '{{ var("unknown_var") }}')) AS team_region_clean

    FROM src_games_teams
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['team_id_clean']) }} AS team_id,
        team_url_clean AS team_url,
        team_name_clean AS team_name,
        {{ dbt_utils.generate_surrogate_key(['team_region_clean']) }} AS team_region_id
    FROM normalized
)

SELECT * FROM surrogate



