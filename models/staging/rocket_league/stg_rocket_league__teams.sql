{{ config(
    materialized='view'
) }}

WITH src_games_teams AS (
    SELECT * FROM {{ source('rocket_league', 'raw_games_teams') }}
),

normalized AS (
    SELECT 
        LOWER(TRIM(team_id::varchar)) AS team_id_clean,
        TRIM(team_slug::varchar) AS team_url_clean,
        TRIM(team_name::varchar) AS team_name_clean,
        
        -- Limpieza de región
        LOWER(COALESCE(TRIM(team_region::varchar), '{{ var("unknown_var", "unknown") }}')) AS team_region_clean,
        
        -- Fecha UTC
        CONVERT_TIMEZONE('UTC', data_load) AS data_load

    FROM src_games_teams
    WHERE team_id IS NOT NULL
),

uniques AS (
    SELECT *
    FROM normalized
    
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY team_id_clean 
        ORDER BY     
            data_load DESC,
            team_name_clean DESC 
    ) = 1
),

surrogate AS (
    SELECT
        -- PK
        {{ dbt_utils.generate_surrogate_key(['team_id_clean']) }} AS team_id,
        team_id_clean AS team_nk,
        team_url_clean AS team_url,
        team_name_clean AS team_name,
        
        {{ dbt_utils.generate_surrogate_key(['team_region_clean']) }} AS team_region_id,
        
        -- Metadata
        data_load
    FROM uniques
)

SELECT * FROM surrogate