{{ config(
    materialized='ephemeral' 
) }}

WITH games AS (
    SELECT * FROM {{ ref('stg_rocket_league__games') }}
),

matches AS (
    SELECT * FROM {{ ref('stg_rocket_league__matches') }}
),

stages AS (
    SELECT * FROM {{ ref('stg_rocket_league__stages') }}
),

events AS (
    -- Solo necesitamos el ID para validar, o si quieres traer atributos aquí
    SELECT * FROM {{ ref('stg_rocket_league__events') }} 
)

SELECT
    -- Base (El hijo menor)
    g.game_id,
    -- Nivel 1: Match
    m.match_id,

    
    -- Nivel 2: Stage
    s.stage_id,
   
    -- Nivel 3: Event
    s.event_id, -- Lo sacamos del stage directamente


    g.data_load

FROM games g
LEFT JOIN matches m ON g.match_id = m.match_id
LEFT JOIN stages s  ON m.stage_id = s.stage_id
LEFT JOIN events e  ON s.event_id = e.event_id