{{ config(
    materialized='view'
) }}

WITH stages AS (
    SELECT * FROM {{ ref('stg_rocket_league__stages') }}
),

stage_names AS (
    SELECT * FROM {{ ref('stg_rocket_league__stage_names') }}
)

SELECT
    s.stage_id,
    sn.stage_name,
    s.stage_step,
    s.stage_is_lan,
    s.stage_is_qualifier,
    s.stage_start_date_utc,
    s.stage_end_date_utc

FROM stages s

LEFT JOIN stage_names sn
    ON s.stage_name_id = sn.stage_name_id 


