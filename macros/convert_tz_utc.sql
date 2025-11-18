-- Definición de Macros para que sean reutilizable

{% macro convert_timezone_utc(model_name) %}
    {# 
      Esta macro toma un modelo o una fuente, inspecciona las columnas, 
      y genera expresiones SQL que convierten las columnas tipo date/timestamp 
      a UTC con el sufijo "_utc".
    #}

    {# Obtener la información de las columnas del modelo #}
    {% set columns = adapter.get_columns_in_relation(ref(model_name)) %}

    {% set converted_columns = [] %}

    {% for col in columns %}
        {% if col.data_type | lower in ['timestamp', 'datetime', 'date'] %}
            {% do converted_columns.append("CONVERT_TIMEZONE('UTC', " ~ col.name ~ ") AS " ~ col.name ~ "_utc") %}
        {% else %}
            {% do converted_columns.append(col.name) %}
        {% endif %}
    {% endfor %}

    {{ converted_columns | join(',\n    ') }}
{% endmacro %}