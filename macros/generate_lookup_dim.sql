{% macro generate_lookup_dim(source_name, table_name, source_column, id_alias, name_alias) %}

WITH src_data AS (
    SELECT * FROM {{ source(source_name, table_name) }}
),

grouped_values AS (
    SELECT 
        LOWER(
            TRIM(
                COALESCE(
                    {{ source_column }}::varchar,
                    '{{ var("unknown_country_code", "unknown") }}'
                )
            )
        ) AS clean_value,

        MAX(CONVERT_TIMEZONE('UTC', data_load)) AS data_load

    FROM src_data
    
    GROUP BY 1 
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['clean_value']) }} AS {{ id_alias }},
        
        clean_value AS {{ name_alias }},
        
        data_load
    FROM grouped_values
)

SELECT * FROM final

{% endmacro %}