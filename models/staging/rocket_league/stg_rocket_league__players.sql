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
        LOWER(TRIM(player_id::varchar)) AS player_id,
        TRIM(player_tag::varchar) AS player_tag,
        TRIM(player_slug::varchar) AS player_url,
        COALESCE(TRIM(player_name::varchar), '{{ var("unknown_var") }}') AS player_name_clean,
        LOWER(COALESCE(TRIM(player_country::varchar), '{{ var("unknown_var") }}')) AS player_country_clean

    FROM src_players
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS player_id,
        player_tag,
        player_url,
        player_name_clean AS player_name,
        {{ dbt_utils.generate_surrogate_key(['player_country_clean']) }} AS player_country_id
    FROM normalized
)

SELECT * FROM surrogate




