{{ config(
    materialized='view'
) }}

WITH src_player_countries AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_player_countries') }}
),

normalized AS (
    SELECT
        COALESCE(TRIM(continent),'{{ var("unknown_country_code") }}') AS continent
    FROM src_player_countries
),

surrogated AS (
    SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['continent']) }}::varchar AS continent_id, 
    TRIM(continent) AS continent_name,
        FROM normalized

)

SELECT *
FROM surrogated
