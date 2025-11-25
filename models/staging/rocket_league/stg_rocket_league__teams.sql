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
    SELECT 
        LOWER(TRIM(team_id::varchar)) AS team_id_clean,
        TRIM(team_slug::varchar) AS team_url_clean,
        TRIM(team_name::varchar) AS team_name_clean,
        LOWER(COALESCE(TRIM(team_region::varchar), '{{ var("unknown_var") }}')) AS team_region_clean,
        CONVERT_TIMEZONE('UTC', data_load) AS data_load


    FROM src_games_teams
    WHERE team_id IS NOT NULL
),

uniques AS (
    --Deduplicación Inteligente
    SELECT *
    FROM normalized
    -- "Agrupa por ID de equipo. Ordénalos para que los que tienen región (no unknown) salgan primero. Quédate con el 1"
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY team_id_clean 
        ORDER BY 
            CASE WHEN team_region_clean != 'unknown' THEN 1 ELSE 2 END ASC, -- Prioriza los que tienen región
            team_name_clean DESC -- Desempate por nombre
    ) = 1
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['team_id_clean']) }} AS team_id,
        team_url_clean AS team_url,
        team_name_clean AS team_name,
        {{ dbt_utils.generate_surrogate_key(['team_region_clean']) }} AS team_region_id,
        data_load
    FROM uniques
)

SELECT * FROM surrogate



