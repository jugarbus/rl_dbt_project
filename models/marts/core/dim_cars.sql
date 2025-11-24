{{ config(
    materialized='view'
) }}


SELECT
    car_id,
    car_name
FROM {{ ref('stg_rocket_league__cars') }}

  