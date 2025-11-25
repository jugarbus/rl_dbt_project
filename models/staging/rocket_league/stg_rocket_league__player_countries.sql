{{ config(
    materialized='view'
) }}

WITH src_countries AS (
    SELECT * FROM {{ source('rocket_league', 'raw_player_countries') }}
),

normalized AS (
    SELECT
        LOWER(COALESCE(TRIM(raw_code::varchar), '{{ var("unknown_var", "unknown") }}')) AS raw_code_clean,
        LOWER(COALESCE(TRIM(country_name::varchar), '{{ var("unknown_var", "unknown") }}')) AS country_name_clean,
        LOWER(COALESCE(TRIM(continent::varchar), '{{ var("unknown_var", "unknown") }}')) AS continent_clean,
        -- Añadimos un valor por defecto a la fecha si viene nula para no romper el orden
        COALESCE(CONVERT_TIMEZONE('UTC', data_load), '1970-01-01'::timestamp_tz) AS data_load

    FROM src_countries

),

uniques AS (
    -- Deduplicación: Una fila por código de país
    SELECT *
    FROM normalized
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY raw_code_clean 
        ORDER BY data_load DESC
    ) = 1
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['raw_code_clean']) }} AS player_country_id,
        raw_code_clean AS player_country_nk,
        country_name_clean AS country_name,
        {{ dbt_utils.generate_surrogate_key(['continent_clean']) }} AS continent_id, 
        
        data_load

    FROM uniques
)

SELECT * FROM surrogate