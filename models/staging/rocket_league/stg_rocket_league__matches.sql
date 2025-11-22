{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
    -- 1. FILTRO CRÍTICO: Eliminamos filas que no son juegos
    WHERE game_id IS NOT NULL 
      AND TRIM(game_id::varchar) != '' -- Por si acaso hay cadenas vacías
),

filtered AS (
    SELECT
        TRIM(match_id::varchar) AS match_id,
        TRIM(match_slug::varchar) AS match_slug, 
        
        -- Corrección de formatos que vimos antes
        CASE 
            WHEN match_format = 'best-of-67' THEN 'best-of-7'
            WHEN match_format = 'best-of-78' THEN 'best-of-7'
            ELSE match_format
        END AS match_format,
        
        match_number::int as match_number,
        TRIM(match_round::varchar) AS match_round, 
        CONVERT_TIMEZONE('UTC', match_date) AS match_date_utc, 
        
        COALESCE(reverse_sweep_attempt, FALSE) AS reverse_sweep_attempt, 
        COALESCE(reverse_sweep, FALSE) AS reverse_sweep

    FROM src_main
),

uniques AS (
    -- 2. Deduplicación 
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY match_id  
        ORDER BY match_date_utc DESC
    ) = 1
),

surrogate AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(["match_id"]) }} AS match_id,
        match_slug,
        match_number,
        {{ dbt_utils.generate_surrogate_key(["match_round"]) }} AS match_round_id,
        match_date_utc,
        {{ dbt_utils.generate_surrogate_key(["match_format"]) }} AS match_format_id,
        reverse_sweep_attempt,
        reverse_sweep
            -- METER STAGE_ID

    FROM uniques
)

SELECT *
FROM surrogate
