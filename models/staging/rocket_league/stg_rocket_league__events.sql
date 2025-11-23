{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT * FROM {{ source('rocket_league', 'raw_main') }}
),

-- 1. Limpieza básica 
cleaned_data AS (
    SELECT 
        TRIM(event_id::varchar) AS event_natural_key,
        
        -- Atributos de texto
        TRIM(event::varchar) AS event_name,
        TRIM(event_split::varchar) AS event_split,

        COALESCE(TRIM(event_region), '{{ var("unknown_country_code") }}')::varchar AS event_region_clean,

        TRIM(event_tier::varchar) AS event_tier,
        
        -- Limpieza de la fase (parte de la clave)
        LOWER(TRIM(COALESCE(event_phase::varchar, 'unknown'))) AS event_phase_clean,
        TRIM(event_phase::varchar) AS event_phase_display,

        -- Fechas
        CONVERT_TIMEZONE('UTC', event_start_date) AS event_start_date_utc,
        CONVERT_TIMEZONE('UTC', event_end_date) AS event_end_date_utc,
        
        -- Aseguramos que el dinero sea numérico y convertimos nulos a 0 para poder ordenar
        COALESCE(prize_money::numeric(18,2), 0) AS prize_money,
        
        TRIM(liquipedia_link::varchar) AS liquipedia_link

    FROM src_main
    WHERE event_id IS NOT NULL
),

-- 2. Deduplicación Inteligente ("El dinero máximo manda")
deduplicated AS (
    SELECT *
    FROM cleaned_data
    -- Agrupamos por los campos que definen la CLAVE (ID  + Fase).
    -- Ordenamos por prize_money DESC (el mayor arriba).
    -- Nos quedamos solo con el primero (= 1).
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_natural_key, event_tier, event_phase_clean 
        ORDER BY prize_money DESC, event_start_date_utc DESC
    ) = 1
),

final AS (
    SELECT
        -- Generamos la SK basada en los datos YA deduplicados
        {{ dbt_utils.generate_surrogate_key([
            'event_natural_key', 
            'event_phase_clean'
        ]) }} AS event_id,

        event_natural_key AS event_nk,
        event_name,
        event_split,
        event_region_clean AS event_region,
        event_start_date_utc,
        event_end_date_utc,
        event_tier,
        event_phase_display AS event_phase,
        prize_money,
        liquipedia_link
    FROM deduplicated
)

SELECT * FROM final
