{{
  config(
    materialized='incremental'
  )
}}

WITH src_countries AS (
    SELECT * 
    FROM {{ source('rocket_league', 'raw_player_countries') }}
),

normalized AS (
    SELECT
        COALESCE(raw_code, var('unknown_country_code')) AS raw_code_clean,
        country_name,
        iso2,
        iso3166_2,
        continent
    FROM src_countries
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['raw_code_clean']) }} AS country_id,
        raw_code_clean AS raw_code,
        country_name,
        iso2,
        iso3166_2,
        continent
    FROM normalized
)

SELECT * FROM surrogate




