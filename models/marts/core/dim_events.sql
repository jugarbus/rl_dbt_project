{{ config(
    materialized='table' 
) }}

WITH joined_events AS (
    SELECT * FROM {{ ref('int_events__joined') }}
)

SELECT
    event_id,
    event_nk,
    event_name,
    event_region,
    event_split,
    event_tier,
    event_phase,
    prize_dollars,
    event_start_date_utc,
    event_end_date_utc,
    event_url,
    data_load

FROM joined_events