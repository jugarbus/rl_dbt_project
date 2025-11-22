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
        event_id::varchar AS event_id, -- salen duplicados igualmente (no se ha captado el grano)
        event::varchar AS event_descr,
        event_split::varchar AS event_split,
        event_region::varchar AS event_region,
        CONVERT_TIMEZONE('UTC', event_start_date) AS event_start_date_utc, 
        CONVERT_TIMEZONE('UTC', event_end_date) AS event_end_date_utc
        event_tier::varchar AS event_tier, -- No esta a grano de event_id. Hay duplicados justamente 2 por event_id
        event_phase::varchar AS event_phase, -- No esta a grano de event_id. Hay duplicados justamente 2 y 3 por event_id
        prize_money::number(38,4),
        {{ dbt_utils.generate_surrogate_key(['location_venue']) }} AS location_id, -- cambiar id de location. hay un registro un event_id con dos distintos location_venue distintas
        liquipedia_link::varchar AS liquipedia_link
    FROM src_main
)

SELECT * 
FROM normalized




