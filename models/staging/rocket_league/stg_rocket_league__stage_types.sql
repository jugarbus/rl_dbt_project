{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

normalize AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(["TRIM(stage::varchar)"]) }} AS stage_name_id,   
        TRIM(stage::varchar) AS stage_name
            FROM src_main
)

SELECT *
FROM normalize

