{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

cleaned_formats AS (
    -- PASO 1: Limpiar los tipos y normalizar textos
    SELECT 
        CASE 
            WHEN match_format = 'best-of-67' THEN '{{ var("match_format") }}'
            WHEN match_format = 'best-of-78' THEN '{{ var("match_format") }}'
            ELSE match_format
        END AS match_format_clean
    FROM src_main
    WHERE match_id IS NOT NULL
      AND match_format IS NOT NULL -- Importante: no queremos formatos nulos en la dimensión
),

unique_formats AS (
    -- PASO 2: Deduplicar y generar ID sobre el dato YA limpio
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(["TRIM(match_format_clean::varchar)"]) }} AS match_format_id,   
        TRIM(match_format_clean::varchar) AS match_format_name
    FROM cleaned_formats
)

SELECT *
FROM unique_formats