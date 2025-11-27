{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

cleaned_stages AS (
    -- Limpieza 
    SELECT
        -- Datos del Stage
        LOWER(COALESCE(TRIM(stage::varchar), '{{ var("unknown_var", "unknown") }}')) AS stage_name_clean,
        stage_step::int AS stage_step_clean,
        CONVERT_TIMEZONE('UTC', stage_start_date) AS stage_start_date_utc,
        CONVERT_TIMEZONE('UTC', stage_end_date) AS stage_end_date_utc,
        stage_is_lan::boolean AS stage_is_lan,
        stage_is_qualifier::boolean AS stage_is_qualifier,

        -- Datos del Evento (Padre)
        LOWER(TRIM(event_id::varchar)) AS event_natural_key_clean,
        LOWER(TRIM(COALESCE(event_phase::varchar, '{{ var("unknown_var", "unknown") }}'))) AS event_phase_clean,
        
        -- Metadata
        CONVERT_TIMEZONE('UTC', data_load) AS data_load

    FROM src_main
    WHERE event_id IS NOT NULL
),

unique_stages AS (
    -- Agrupamos para eliminar duplicados y coger la fecha más reciente
    SELECT
        stage_name_clean,
        stage_step_clean,
        stage_start_date_utc,
        stage_end_date_utc,
        stage_is_lan,
        stage_is_qualifier,
        event_natural_key_clean,
        event_phase_clean,
        
        MAX(data_load) as data_load
        
    FROM cleaned_stages
    GROUP BY 
        stage_name_clean,
        stage_step_clean,
        stage_start_date_utc,
        stage_end_date_utc,
        stage_is_lan,
        stage_is_qualifier,
        event_natural_key_clean,
        event_phase_clean
),

final AS (
    SELECT
        -- 3. Generación de IDs
        {{ dbt_utils.generate_surrogate_key([
            'event_natural_key_clean',  
            'event_phase_clean',        
            'stage_name_clean', 
            'stage_start_date_utc', 
            'stage_step_clean'
        ]) }} AS stage_id,
        
        {{ dbt_utils.generate_surrogate_key(["stage_name_clean"]) }} AS stage_name_id,

        {{ dbt_utils.generate_surrogate_key([
            'event_natural_key_clean', 
            'event_phase_clean'
        ]) }} AS event_id,

        -- Campos
        stage_step_clean AS stage_step,
        stage_start_date_utc,
        stage_end_date_utc,
        stage_is_lan,
        stage_is_qualifier,
        data_load

    FROM unique_stages
)

SELECT * FROM final