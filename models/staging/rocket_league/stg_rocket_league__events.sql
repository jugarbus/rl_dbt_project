{{
  config(
    materialized='view'
  )
}}

WITH src_main AS (
    SELECT * 
    FROM {{ source('rocket_league', 'raw_main') }}
),

normalized AS (
    SELECT DISTINCT
        event_id, -- salen duplicados igualmente (no se ha captado el grano)
        event AS event_descr,
        event_split,
        event_region,
        event_start_date, -- normalizar fecha
        event_end_date, -- normalizar fecha
        event_tier,
        event_phase,
        prize_money AS prize_money_dollars,
        {{ dbt_utils.generate_surrogate_key(['stage']) }} AS stage_id, -- sacar stage_id del distinct. Parece que price varia en funcion del stage_id o algo de menor granularidad
        {{ dbt_utils.generate_surrogate_key(['location_venue']) }} AS location_id, -- cambiar id de location
        liquipedia_link
    FROM src_main
)

SELECT * 
FROM normalized




