{{ config(
    materialized='table'
) }}


SELECT
    platform_id,
    platform,
    data_load

FROM {{ ref('stg_rocket_league__platforms') }}