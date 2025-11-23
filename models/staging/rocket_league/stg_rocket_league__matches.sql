{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

normalization AS (
    SELECT
        LOWER(TRIM(match_id::varchar)) AS match_id_clean,
        LOWER(TRIM(stage)) AS stage_name_clean,
        LOWER(TRIM(stage_step)) AS stage_step_clean,
        CONVERT_TIMEZONE('UTC', stage_start_date) AS stage_start_date_utc,
        TRIM(match_slug::varchar) AS match_url_clean, 
        
        -- Corrección de formatos de enfrentamientos
        LOWER(TRIM(
        CASE 
            WHEN match_format = 'best-of-67' THEN '{{ var("match_format") }}'
            WHEN match_format = 'best-of-78' THEN '{{ var("match_format") }}'
            ELSE match_format
        END
         ))::varchar AS match_format_clean,
        
        match_number::int as match_number,
        LOWER(TRIM(match_round::varchar)) AS match_round_clean, 
        CONVERT_TIMEZONE('UTC', match_date) AS match_date_utc, 
        
        COALESCE(reverse_sweep_attempt, FALSE) AS reverse_sweep_attempt_clean, 
        COALESCE(reverse_sweep, FALSE) AS reverse_sweep_clean

    FROM src_main
),

uniques AS (
    -- Deduplicación 
    SELECT *
    FROM normalization
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY match_id_clean  
        ORDER BY match_date_utc DESC
    ) = 1
),

surrogate AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(["match_id_clean"]) }} AS match_id,
        --Generamos la PK única del Stage
        {{ dbt_utils.generate_surrogate_key([
            'stage_name_clean', 
            'stage_start_date_utc', 
            'stage_step_clean'
        ]) }} AS stage_id,
        match_url_clean AS match_url,
        match_number,
        {{ dbt_utils.generate_surrogate_key(["match_round_clean"]) }} AS match_round_id,
        match_date_utc,
        {{ dbt_utils.generate_surrogate_key(["match_format_clean"]) }} AS match_format_id,
        reverse_sweep_attempt_clean AS reverse_sweep_attempt,
        reverse_sweep_clean AS reverse_sweep

    FROM uniques
)

SELECT *
FROM surrogate
