{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

normalization AS (
    SELECT
        TRIM(match_id::varchar) AS match_id,
        TRIM(stage) AS stage_name,
        TRIM(stage_step) AS stage_step,
        CONVERT_TIMEZONE('UTC', stage_start_date) AS stage_start_date_utc,
        TRIM(match_slug::varchar) AS match_slug, 
        
        -- Corrección de formatos de enfrentamientos
        CASE 
            WHEN match_format = 'best-of-67' THEN '{{ var("match_format") }}'
            WHEN match_format = 'best-of-78' THEN '{{ var("match_format") }}'
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
    -- Deduplicación 
    SELECT *
    FROM normalization
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY match_id  
        ORDER BY match_date_utc DESC
    ) = 1
),

surrogate AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(["match_id"]) }} AS match_id,
        --Generamos la PK única del Stage
        {{ dbt_utils.generate_surrogate_key([
            'stage_name', 
            'stage_start_date_utc', 
            'stage_step'
        ]) }} AS stage_id,
        match_slug,
        match_number,
        {{ dbt_utils.generate_surrogate_key(["match_round"]) }} AS match_round_id,
        match_date_utc,
        {{ dbt_utils.generate_surrogate_key(["match_format"]) }} AS match_format_id,
        reverse_sweep_attempt,
        reverse_sweep

    FROM uniques
)

SELECT *
FROM surrogate
