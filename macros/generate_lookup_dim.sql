{% macro generate_lookup_dim(source_name, table_name, source_column, id_alias, name_alias) %}

WITH src_data AS (
    SELECT * FROM {{ source(source_name, table_name) }}
),

distinct_values AS (
    SELECT DISTINCT
    LOWER(
        TRIM(
            COALESCE(
                -- Si es NULL, usamos tu variable de proyecto
                {{ source_column }}::varchar,
                '{{ var("unknown_country_code", "Unknown") }}'
            )
        )
     ) AS clean_value
    FROM src_data
),

final AS (
    SELECT
        -- Generamos la Surrogate Key sobre el valor limpio (ahora incluyendo 'Unknown')
        {{ dbt_utils.generate_surrogate_key(['clean_value']) }} AS {{ id_alias }},
        
        -- El nombre limpio
        clean_value AS {{ name_alias }}
    FROM distinct_values
)

SELECT * FROM final

{% endmacro %}