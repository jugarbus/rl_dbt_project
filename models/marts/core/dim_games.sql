{{ config(
    materialized='view'
) }}


SELECT
    game_id,
    game_number,
    game_date_utc, 
    game_duration_secs,
    is_overtime
FROM {{ ref('stg_rocket_league__games') }}
ORDER BY game_date_utc ASC

  