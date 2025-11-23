{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT * FROM {{ source('rocket_league', 'raw_main') }}
),

-- 1. Limpieza básica 
cleaned_data AS (
    SELECT 
        LOWER(TRIM(event_id::varchar)) AS event_natural_key,
        
        -- Atributos de texto
        LOWER(COALESCE(TRIM(event::varchar), '{{ var("unknown_var") }}')) AS event_name_clean,
        LOWER(COALESCE(TRIM(event_split::varchar), '{{ var("unknown_var") }}')) AS event_split_clean,
        LOWER(COALESCE(TRIM(event_region::varchar), '{{ var("unknown_var") }}')) AS event_region_clean,
        LOWER(COALESCE(TRIM(event_tier::varchar), '{{ var("unknown_var") }}')) AS event_tier_clean,
        LOWER(TRIM(COALESCE(event_phase::varchar, '{{ var("unknown_var") }}'))) AS event_phase_clean,

        -- Fechas
        CONVERT_TIMEZONE('UTC', event_start_date) AS event_start_date_utc,
        CONVERT_TIMEZONE('UTC', event_end_date) AS event_end_date_utc,
        
        -- Aseguramos que el dinero sea numérico y convertimos nulos a 0 para poder ordenar
        LOWER(COALESCE(prize_money::numeric(18,2), 0)) AS prize_money,
        
        TRIM(liquipedia_link::varchar) AS liquipedia_link

    FROM src_main
    WHERE event_id IS NOT NULL
),

-- 2. Deduplicación Inteligente por dinero máximo
deduplicated AS (
    SELECT *
    FROM cleaned_data
    -- Agrupamos por los campos que definen la CLAVE (ID  + Fase).
    -- Ordenamos por prize_money DESC (el mayor arriba).
    -- Nos quedamos solo con el primero (= 1).
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_natural_key, event_tier_clean, event_phase_clean 
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
        {{ dbt_utils.generate_surrogate_key(['event_name_clean']) }} AS event_name_id,
        {{ dbt_utils.generate_surrogate_key(['event_split_clean']) }} AS event_split_id,
        {{ dbt_utils.generate_surrogate_key(['event_region_clean']) }}  AS event_region_id, 
        event_start_date_utc,
        event_end_date_utc,
        {{ dbt_utils.generate_surrogate_key(['event_tier_clean']) }}  AS event_tier_id, 
        {{ dbt_utils.generate_surrogate_key(['event_phase_clean']) }}  AS event_phase_id, 
        prize_money,
        liquipedia_link
    FROM deduplicated
)

SELECT * FROM final
