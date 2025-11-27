{{ config(
    materialized='table'
) }}

WITH teams AS (
    SELECT * FROM {{ ref('stg_rocket_league__teams') }}
),

regions AS (
    SELECT * FROM {{ ref('stg_rocket_league__regions') }}
)

SELECT
    t.team_sk,
    t.team_nk,
    t.team_name,
    t.team_url,
    r.region_name,

    t.valid_from,
    t.valid_to,
    t.is_current,

    t.data_load

FROM teams t
-- Equipo con Región
LEFT JOIN regions r 
    ON t.team_region_id = r.region_id

