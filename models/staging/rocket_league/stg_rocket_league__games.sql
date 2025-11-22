{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

filtered AS (
    -- 1. Filtrar game_id no nulos y normalizar
    SELECT
       TRIM(game_id::varchar) AS game_id,
        TRIM(match_id::varchar)::varchar AS match_id,
        game_number::int as game_number,
        CONVERT_TIMEZONE('UTC', game_date) AS game_date_utc, 
        game_duration::int AS game_duration_secs,
        TRIM(map_id::varchar) AS map_id, 
        overtime    
        FROM src_main
    WHERE game_id IS NOT NULL
),

uniques AS (
    -- 2. Quedarte solo con los game_id únicos
    SELECT DISTINCT
        game_id,         
        match_id,        
        game_number,
        game_date_utc,
        game_duration_secs,
        map_id,         
        overtime
    FROM filtered
),

surrogate AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['game_id']) }} AS game_id,
        {{ dbt_utils.generate_surrogate_key(['match_id']) }} AS match_id,
        game_number,
        game_date_utc,
        game_duration_secs,
        {{ dbt_utils.generate_surrogate_key(['map_id']) }} AS map_id,
        overtime
    FROM uniques

)

SELECT *
FROM surrogate

