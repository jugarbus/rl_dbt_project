{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

cleaned_formats AS (
    -- Limpiar los tipos y normalizar textos
    SELECT 
        LOWER(TRIM(
            COALESCE(
                CASE 
                    WHEN match_format = 'best-of-67' THEN '{{ var("match_format") }}'
                    WHEN match_format = 'best-of-78' THEN '{{ var("match_format") }}'
                    ELSE match_format
                END::varchar, 
                '{{ var("unknown_var") }}'
            )
        )) AS match_format_clean,

        CONVERT_TIMEZONE('UTC', data_load) AS data_load

    FROM src_main
    WHERE match_id IS NOT NULL
),

unique_formats AS (
    -- Deduplicar usando GROUP BY para evitar duplicados por fecha
    SELECT 
        -- Generamos el ID
        {{ dbt_utils.generate_surrogate_key(["match_format_clean"]) }} AS match_format_id,
        match_format_clean AS match_format_name,
        MAX(data_load) AS data_load
        
    FROM cleaned_formats
    GROUP BY match_format_clean -- Agrupamos solo por el nombre
)

SELECT *
FROM unique_formats