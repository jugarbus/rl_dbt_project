{{ config(
    materialized='view'
) }}


SELECT
    car_id,
    car_name,
    data_load
FROM {{ ref('stg_rocket_league__cars') }}

  