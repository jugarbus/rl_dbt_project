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
        COALESCE(TRIM(raw_code), '{{ var("unknown_country_code") }}')::varchar AS raw_code_clean,
        COALESCE(TRIM(country_name),'{{ var("unknown_country_code") }}')::varchar AS country_name,
        COALESCE(TRIM(continent),'{{ var("unknown_country_code") }}')::varchar AS continent
    FROM src_countries
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['raw_code_clean']) }} AS player_country_id,
        raw_code_clean AS raw_code,
        country_name AS country_name,
        {{ dbt_utils.generate_surrogate_key(['continent']) }} AS continent_id, 
 
    FROM normalized
)

SELECT * FROM surrogate




