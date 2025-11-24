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
        TRIM(match_slug::varchar) AS match_url_clean,
        
        -- FECHAS
        CONVERT_TIMEZONE('UTC', match_date) AS match_date_utc,
        
        -- DATOS DEL MATCH
        TRY_CAST(match_number AS INT) as match_number,
        COALESCE(TRY_CAST(reverse_sweep_attempt AS BOOLEAN), FALSE) AS reverse_sweep_attempt_clean, 
        COALESCE(TRY_CAST(reverse_sweep AS BOOLEAN), FALSE) AS reverse_sweep_clean,



        -- 1. MATCH ROUND
        LOWER(TRIM(COALESCE(match_round, '{{ var("unknown_var", "unknown") }}'))) AS match_round_clean, 

        -- 2. MATCH FORMAT
        LOWER(TRIM(
            COALESCE(
                CASE 
                    WHEN match_format = 'best-of-67' THEN '{{ var("match_format", "best-of-7") }}'
                    WHEN match_format = 'best-of-78' THEN '{{ var("match_format", "best-of-7") }}'
                    ELSE match_format
                END, 
                '{{ var("unknown_var", "unknown") }}'
            )
        )) AS match_format_clean,

        -- 3. DATOS PARA EL STAGE 

        LOWER(TRIM(event_id::varchar)) AS event_natural_key_clean, 
        LOWER(TRIM(COALESCE(event_phase::varchar, '{{ var("unknown_var", "unknown") }}'))) AS event_phase_clean, 
        
        LOWER(COALESCE(TRIM(stage), '{{ var("unknown_var", "unknown") }}')) AS stage_name_clean,
        TRY_CAST(stage_step AS INT) AS stage_step_clean,
        CONVERT_TIMEZONE('UTC', stage_start_date) AS stage_start_date_utc

    FROM src_main
    WHERE match_id IS NOT NULL
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
        -- Primary Key
        {{ dbt_utils.generate_surrogate_key(["match_id_clean"]) }} AS match_id,
        
        -- FK hacia Stage
        {{ dbt_utils.generate_surrogate_key([
            'event_natural_key_clean',  
            'event_phase_clean',        
            'stage_name_clean', 
            'stage_start_date_utc', 
            'stage_step_clean'
        ]) }} AS stage_id,
        
        -- FKs secundarias
        {{ dbt_utils.generate_surrogate_key(["match_round_clean"]) }} AS match_round_id,
        {{ dbt_utils.generate_surrogate_key(["match_format_clean"]) }} AS match_format_id,

        -- Datos naturales
        match_url_clean AS match_url,
        match_number,
        match_date_utc,
        reverse_sweep_attempt_clean AS reverse_sweep_attempt,
        reverse_sweep_clean AS reverse_sweep

    FROM uniques
)

SELECT *
FROM surrogate