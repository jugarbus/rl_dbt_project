{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

normalize AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(["LOWER(TRIM(map_name::varchar))"]) }} AS map_id,   
        LOWER(TRIM(map_name::varchar)) AS map_name
            FROM src_main
            WHERE game_id IS NOT NULL       

)

SELECT *
FROM normalize

