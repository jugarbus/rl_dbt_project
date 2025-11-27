

{% test is_percentage(model, column_name) %}
    select *
    from {{ model }}
    where 
        TRY_CAST({{ column_name }} AS FLOAT) < 0 
        OR TRY_CAST({{ column_name }} AS FLOAT) > 100
        OR ({{ column_name }} IS NOT NULL AND TRY_CAST({{ column_name }} AS FLOAT) IS NULL) -- Falla si no es número
{% endtest %}