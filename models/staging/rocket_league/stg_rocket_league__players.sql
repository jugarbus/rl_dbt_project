{{
  config(
    materialized='view'
  )
}}

WITH src_players AS (
    SELECT * 
    FROM {{ source('rocket_league', 'raw_players') }}
),

normalized AS (
    SELECT 
        player_id::varchar AS player_id,
        TRIM(player_tag)::varchar AS player_tag,
        TRIM(player_name)::varchar AS player_name,
    COALESCE(TRIM(player_country), '{{ var("unknown_country_code") }}')::varchar AS player_country_clean

    FROM src_players
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS player_id,
        player_tag,
        player_name,
        {{ dbt_utils.generate_surrogate_key(['player_country_clean']) }} AS player_country_id
    FROM normalized
)

SELECT * FROM surrogate




