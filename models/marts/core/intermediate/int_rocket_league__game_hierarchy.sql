{{ config(
    materialized='view' 
) }}

WITH games AS (
    SELECT * FROM {{ ref('stg_rocket_league__games') }}
),


matches AS (
    SELECT match_id, stage_id FROM {{ ref('stg_rocket_league__matches') }}
),

stages AS (
    SELECT stage_id, event_id FROM {{ ref('stg_rocket_league__stages') }}
)

SELECT
    g.game_id,
    g.game_date_utc,
    g.map_id,   
    g.match_id,
    m.stage_id,
    s.event_id,
    g.data_load


FROM games g
LEFT JOIN matches m ON g.match_id = m.match_id
LEFT JOIN stages s  ON m.stage_id = s.stage_id