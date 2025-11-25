{{ config(
    materialized='ephemeral' 
) }}

WITH events AS (
    SELECT * FROM {{ ref('stg_rocket_league__events') }}
),

event_names AS (
    SELECT * FROM {{ ref('stg_rocket_league__event_names') }}
),

event_splits AS (
    SELECT * FROM {{ ref('stg_rocket_league__event_splits') }}
),

event_tiers AS (
    SELECT * FROM {{ ref('stg_rocket_league__event_tiers') }}
),

event_phases AS (
    SELECT * FROM {{ ref('stg_rocket_league__event_phases') }}
),

regions AS (
    SELECT * FROM {{ ref('stg_rocket_league__regions') }}
)

SELECT
    -- 1. Identificadores Principales
    e.event_id,
    e.event_nk, 

    -- 2. Atributos Desnormalizados (Traemos los NOMBRES, ignoramos los IDs intermedios)
    en.event_name,
    es.event_split_name AS event_split,
    r.region_name       AS event_region,
    et.event_tier_name  AS event_tier,
    ep.event_phase_name AS event_phase,

    -- 3. Métricas y Fechas
    e.prize_money AS prize_dollars,
    e.event_start_date_utc,
    e.event_end_date_utc,
    e.event_url ,

    e.data_load

FROM events e
LEFT JOIN event_names en  ON e.event_name_id = en.event_name_id
LEFT JOIN event_splits es ON e.event_split_id = es.event_split_id
LEFT JOIN event_tiers et  ON e.event_tier_id = et.event_tier_id
LEFT JOIN event_phases ep ON e.event_phase_id = ep.event_phase_id
LEFT JOIN regions r       ON e.event_region_id = r.region_id