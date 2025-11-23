{{ config(
    materialized='view'
) }}

WITH src_games_players AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_games_players') }}
),

filtered AS (
    SELECT DISTINCT
    LOWER(TRIM(
        CASE 
            WHEN LOWER(platform) = 'psynet' THEN '{{ var("unknown_platform") }}'
            ELSE LOWER(platform)
        END
     ))::varchar AS platform
    FROM src_games_players
),

surrogated_key AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['platform']) }}::varchar AS platform_id,
        platform
    FROM filtered
)

SELECT 
    *
FROM surrogated_key
