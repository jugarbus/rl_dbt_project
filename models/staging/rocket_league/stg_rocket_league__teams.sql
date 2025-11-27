{{ config(materialized='view') }}

WITH snapshot_data AS (
    -- Leemos la historia ya limpia
    SELECT * FROM {{ ref('sns_rocket_league__teams') }}
),

final AS (
    SELECT
        dbt_scd_id AS team_sk, -- Esta es la Primary Key única para la historia     

        -- Datos de Negocio 
        {{ dbt_utils.generate_surrogate_key(['team_id_clean']) }} AS team_nk, -- Se hace surrogada para que luego en fct haga match con la de int_rocket_league__add_kpis
        team_name_clean AS team_name,
        team_url_clean AS team_url,
        
        -- Generamos FK a región
        {{ dbt_utils.generate_surrogate_key(['team_region_clean']) }} AS team_region_id,

        -- Metadatos SCD Tipo 2
        dbt_valid_from AS valid_from,
        COALESCE(dbt_valid_to, '9999-12-31'::timestamp) AS valid_to,

            -- Útil para filtrar rápidamente "dame la foto de hoy"
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,        
        data_load

    FROM snapshot_data
)

SELECT * FROM final