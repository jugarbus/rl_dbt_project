

{% test is_valid_url(model, column_name) %}

    select *
    from {{ model }}

    where NOT REGEXP_CONTAINS({{ column_name }}, r'^https?://[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}(/.*)?$')
    and {{ column_name }} is not null 

{% endtest %}