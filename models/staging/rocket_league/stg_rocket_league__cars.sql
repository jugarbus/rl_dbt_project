{{ config(
    materialized='view'
) }}

SELECT DISTINCT 
    car_id,
    car_name,
    data_load
FROM {{ ref('base_rocket_league__games_players') }} 
