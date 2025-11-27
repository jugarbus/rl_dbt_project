{{ config(
    materialized='view'
) }}

WITH regions_from_events AS (
    SELECT * FROM {{ ref('base_rocket_league__event_regions') }}
),

regions_from_teams AS (
    SELECT * FROM {{ ref('base_rocket_league__team_regions') }}
),

-- Union 
unioned_raw AS (
    SELECT 
        event_region_name AS region_name,
        data_load
    FROM regions_from_events
    
    UNION ALL 
    
    SELECT 
        team_region_name AS region_name,
        data_load 
    FROM regions_from_teams
),

-- Deduplicación: Agrupamos por nombre y nos quedamos la fecha más reciente
deduplicated AS (
    SELECT 
        region_name,
        MAX(data_load) AS data_load
    FROM unioned_raw
    WHERE region_name IS NOT NULL
    GROUP BY region_name 
),

final AS (
    SELECT
        -- Generamos la SK sobre la lista única
        {{ dbt_utils.generate_surrogate_key(['region_name']) }} AS region_id,
        
        region_name,
        data_load

    FROM deduplicated
)

SELECT * FROM final