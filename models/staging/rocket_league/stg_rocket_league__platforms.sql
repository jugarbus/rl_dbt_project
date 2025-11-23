{{ config(
    materialized='view'
) }}

SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['platform::varchar']) }} AS platform_id,
    platform,
    data_load
FROM {{ ref('base_rocket_league__games_players') }} 
