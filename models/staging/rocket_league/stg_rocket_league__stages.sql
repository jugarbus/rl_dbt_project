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
    TRIM(stage) AS stage_name,
    TRIM(stage_step) AS stage_step,
    CONVERT_TIMEZONE('UTC', stage_start_date) AS stage_start_date_utc,
    CONVERT_TIMEZONE('UTC', stage_end_date) AS stage_end_date_utc,
    stage_is_lan,
    stage_is_qualifier,


    -- Datos para construir la FK del Evento (Ingredientes)
        TRIM(event_id::varchar) AS event_natural_key,
        -- Limpieza idéntica a la tabla de eventos:
        LOWER(TRIM(COALESCE(event_phase::varchar, 'Unknown'))) AS event_phase_clean

FROM src_main

),

final AS (
    SELECT
        -- 2. Generamos la PK única del Stage
        {{ dbt_utils.generate_surrogate_key([
            'stage_name', 
            'stage_start_date_utc', 
            'stage_step'
        ]) }} AS stage_id,
        
        {{ dbt_utils.generate_surrogate_key(["stage_name"]) }} AS stage_name_id,


        -- 3. Generamos la FK hacia Events (SOLO ID + FASE)
        {{ dbt_utils.generate_surrogate_key([
            'event_natural_key', 
            'event_phase_clean'
        ]) }} AS event_id,  -- Esta columna conectará con stg_events


        -- Campos informativos
        stage_step,
        stage_start_date_utc,
        stage_end_date_utc,
        stage_is_lan,
        stage_is_qualifier

    FROM unique_stages
)

SELECT * FROM final