{{ config(
    materialized='view'
) }}


SELECT
    map_id,
    map_name
FROM {{ ref('stg_rocket_league__maps') }}