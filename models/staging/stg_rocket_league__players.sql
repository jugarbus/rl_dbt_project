{{
  config(
    materialized='incremental'
  )
}}

WITH src_players AS (
    SELECT * 
    FROM {{ source('rocket_league', 'raw_players') }}
),

normalized AS (
    SELECT 
        player_id,
        player_slug,
        player_tag,
        player_name,

        COALESCE(player_country, var('unknown_country_code')) AS player_country_clean
    FROM src_players
),

surrogate AS (
    SELECT
        *,
        {{ dbt_utils.generate_surrogate_key(['player_country_clean']) }} AS player_country_id
    FROM normalized
)

SELECT * FROM surrogate




