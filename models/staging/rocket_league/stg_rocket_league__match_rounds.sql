{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

normalize AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(["TRIM(match_round::varchar)"]) }} AS match_round_id,   
        TRIM(match_round::varchar) AS match_round_name
            FROM src_main
            WHERE match_id IS NOT NULL       

)

SELECT *
FROM normalize

