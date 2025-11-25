{{ config(
    materialized='view'
) }}

WITH src_games_teams AS (
    SELECT * FROM {{ source('rocket_league', 'raw_games_teams') }}
),

normalized AS (
    SELECT 
        LOWER(COALESCE(TRIM(team_region::varchar), '{{ var("unknown_var") }}')) AS team_region_clean,
        CONVERT_TIMEZONE('UTC', data_load) AS data_load
    FROM src_games_teams
),

unique_regions AS (
    SELECT 
        team_region_clean,
        MAX(data_load) AS data_load
    FROM normalized
    GROUP BY team_region_clean
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['team_region_clean']) }} AS team_region_id,
        team_region_clean AS team_region_name,
        data_load
    FROM unique_regions
)

SELECT * FROM surrogate