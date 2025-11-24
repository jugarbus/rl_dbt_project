{{ config(
    materialized='view'
) }}

WITH matches AS (
    SELECT * FROM {{ ref('stg_rocket_league__matches') }}
),

match_rounds AS (
    SELECT * FROM {{ ref('stg_rocket_league__match_rounds') }}
),

match_formats AS (
    SELECT * FROM {{ ref('stg_rocket_league__match_formats') }}
)

SELECT
match_id,
match_round_id,
match_format_id


FROM matches m
-- Jugador con País
LEFT JOIN match_rounds mr
    ON  = 

-- Unimos el resultado anterior con Continente

LEFT JOIN match_format mf 
    ON  = 