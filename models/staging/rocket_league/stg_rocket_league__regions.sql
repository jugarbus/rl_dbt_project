{{ config(
    materialized='view'
) }}

WITH regions_from_events AS (
    SELECT * FROM {{ ref('base_rocket_league__event_regions') }}
),

regions_from_teams AS (
    SELECT * FROM {{ ref('base_rocket_league__team_regions') }}
),

unioned_regions AS (
    -- Se unifica el nombre de la columna a 'region_name'
    SELECT event_region_name AS region_name 
    FROM regions_from_events
    
    UNION
    
    SELECT team_region_name AS region_name 
    FROM regions_from_teams
),

final AS (
    SELECT
        -- Generamos la SK sobre la lista TOTAL unificada
        {{ dbt_utils.generate_surrogate_key(['region_name']) }} AS region_id,
        
        region_name

    FROM unioned_regions
)

SELECT * FROM final