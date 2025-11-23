{{
  config(
    materialized='view'
  )
}}

WITH src_countries AS (
    SELECT * 
    FROM {{ source('rocket_league', 'raw_player_countries') }}
),

normalized AS (
    SELECT
        LOWER(COALESCE(TRIM(raw_code::varchar), '{{ var("unknown_var") }}')) AS raw_code_clean,
        LOWER(COALESCE(TRIM(country_name::varchar),'{{ var("unknown_var") }}')) AS country_name_clean,
        LOWER(COALESCE(TRIM(continent::varchar),'{{ var("unknown_var") }}')) AS continent_clean
    FROM src_countries
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['raw_code_clean']) }} AS player_country_id,
        raw_code_clean AS raw_code,
        country_name_clean AS country_name,
        {{ dbt_utils.generate_surrogate_key(['continent_clean']) }} AS continent_id, 
 
    FROM normalized
)

SELECT * FROM surrogate




