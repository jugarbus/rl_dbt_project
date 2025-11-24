{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

unique_stages AS (
    -- 1. Seleccionamos columnas de Stage y Evento necesarias
    SELECT DISTINCT
        -- Limpieza de Stage
        LOWER(COALESCE(TRIM(stage::varchar), '{{ var("unknown_var", "unknown") }}')) AS stage_name_clean,
        stage_step::int AS stage_step_clean,
        CONVERT_TIMEZONE('UTC', stage_start_date) AS stage_start_date_utc,
        CONVERT_TIMEZONE('UTC', stage_end_date) AS stage_end_date_utc,
        stage_is_lan::boolean AS stage_is_lan,
        stage_is_qualifier::boolean AS stage_is_qualifier,

        -- Limpieza de Evento 
        LOWER(TRIM(event_id::varchar)) AS event_natural_key_clean,
        LOWER(TRIM(COALESCE(event_phase::varchar, '{{ var("unknown_var", "unknown") }}'))) AS event_phase_clean

    FROM src_main
    WHERE event_id IS NOT NULL 
),

final AS (
    SELECT
        -- 2. Generamos la SK del Stage 
        -- INCLUYE EL EVENTO PARA EVITAR DUPLICADOS
        {{ dbt_utils.generate_surrogate_key([
            'event_natural_key_clean',  
            'event_phase_clean',        
            'stage_name_clean', 
            'stage_start_date_utc', 
            'stage_step_clean'
        ]) }} AS stage_id,
        
        -- FK a Stage Name (Lookup table)
        {{ dbt_utils.generate_surrogate_key(["stage_name_clean"]) }} AS stage_name_id,

        -- 3. Generamos la FK hacia Events
        {{ dbt_utils.generate_surrogate_key([
            'event_natural_key_clean', 
            'event_phase_clean'
        ]) }} AS event_id,

        -- Campos informativos
        stage_step_clean AS stage_step,
        stage_start_date_utc,
        stage_end_date_utc,
        stage_is_lan,
        stage_is_qualifier

    FROM unique_stages
)

SELECT * FROM final