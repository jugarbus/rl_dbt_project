{{ config(
    materialized='view'
) }}


SELECT
    platform_id,
    platform
FROM {{ ref('stg_rocket_league__platforms') }}