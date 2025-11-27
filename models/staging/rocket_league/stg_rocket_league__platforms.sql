{{ config(
    materialized='view'
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['platform']) }} AS platform_id,
    platform,
    MAX(data_load) AS data_load

FROM {{ ref('base_rocket_league__games_players') }}
GROUP BY 1, 2