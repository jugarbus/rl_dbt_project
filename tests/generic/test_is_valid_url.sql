{% test is_valid_url(model, column_name) %}

    select *
    from {{ model }}
    -- El test falla si encuentra filas que NO coinciden con el patrón
    where NOT REGEXP_CONTAINS({{ column_name }}, r'^https?://[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}(/.*)?$')
    and {{ column_name }} is not null -- Ignoramos nulos si quieres

{% endtest %}