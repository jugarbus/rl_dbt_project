{{ config(
    materialized='table'
) }}


SELECT
    map_id,
    map_name,
    data_load
FROM {{ ref('stg_rocket_league__maps') }}