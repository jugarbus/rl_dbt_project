{{ config(
    materialized='table'
) }}

WITH players AS (
    SELECT * FROM {{ ref('stg_rocket_league__players') }}
),

countries AS (
    SELECT * FROM {{ ref('stg_rocket_league__player_countries') }}
),

continents AS (
    SELECT * FROM {{ ref('stg_rocket_league__continents') }}
)

SELECT
    -- 1. Datos del Jugador (Base)
    p.player_id,
    p.player_tag,
    p.player_name,
    p.player_url,

    -- 2. Traemos datos del País (Nivel 1)
    c.player_country_nk AS country_code,
    c.country_name,

    -- 3. Traemos datos del Continente (Nivel 2)
    cont.continent_name,
    p.data_load

FROM players p
-- Jugador con País
LEFT JOIN countries c 
    ON p.player_country_id = c.player_country_id

-- Unimos el resultado anterior con Continente

LEFT JOIN continents cont 
    ON c.continent_id = cont.continent_id