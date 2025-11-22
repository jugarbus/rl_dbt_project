{{ config(
    materialized='view'
) }}

WITH src_games_players AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_games_players') }}
),

filtered AS (
    SELECT DISTINCT
        car_id::varchar AS car_id,
        car_name::varchar AS car_name
    FROM src_games_players
),

surrogated_key AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['car_id']) }}::varchar AS car_id,
        car_name
    FROM filtered
)

SELECT 
    * 
FROM surrogated_key
