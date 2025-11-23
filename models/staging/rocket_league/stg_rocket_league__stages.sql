{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}

),

unique_stages AS (
    -- 1. Seleccionamos solo las columnas de Stage y eliminamos duplicados
SELECT DISTINCT
    LOWER(COALESCE(TRIM(stage::varchar), '{{ var("unknown_var") }}')) AS stage_name_clean,
    stage_step::int AS stage_step_clean,
    CONVERT_TIMEZONE('UTC', stage_start_date) AS stage_start_date_utc,
    CONVERT_TIMEZONE('UTC', stage_end_date) AS stage_end_date_utc,
    stage_is_lan::boolean AS stage_is_lan,
    stage_is_qualifier::boolean AS stage_is_qualifier,


    -- Datos para construir la FK del Evento
        LOWER(TRIM(event_id::varchar)) AS event_natural_key_clean,
        -- Limpieza idéntica a la tabla de eventos:
        LOWER(TRIM(COALESCE(event_phase::varchar, '{{ var("unknown_var") }}'))) AS event_phase_clean

FROM src_main

),

final AS (
    SELECT
        -- 2. Generamos la SK  del Stage
        {{ dbt_utils.generate_surrogate_key([
            'stage_name_clean', 
            'stage_start_date_utc', 
            'stage_step_clean'
        ]) }} AS stage_id,
        
        {{ dbt_utils.generate_surrogate_key(["stage_name_clean"]) }} AS stage_name_id,


        -- 3. Generamos la FK hacia Events (SOLO ID + FASE)
        {{ dbt_utils.generate_surrogate_key([
            'event_natural_key_clean', 
            'event_phase_clean'
        ]) }} AS event_id,  -- Esta columna conectará con stg_events


        -- Campos informativos
        stage_step_clean AS stage_step,
        stage_start_date_utc,
        stage_end_date_utc,
        stage_is_lan,
        stage_is_qualifier

    FROM unique_stages
)

SELECT * FROM final