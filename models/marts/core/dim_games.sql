{{ config(
    materialized='table'
) }}

WITH source_games AS (
    SELECT * FROM {{ ref('stg_rocket_league__games') }}
),

calculation_for_ordering AS (
    SELECT 
        *,
        MIN(game_date_utc) OVER (PARTITION BY match_id) AS match_start_block
    FROM source_games
)

SELECT
    game_id,
    game_number,
    is_overtime,
    game_duration_secs,
    game_date_utc,
    data_load

FROM calculation_for_ordering

ORDER BY
    match_start_block ASC, 
    match_id,             
    game_number ASC       