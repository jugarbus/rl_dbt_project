{{ config(
    materialized='view'
) }}

{{ generate_lookup_dim(
    source_name='rocket_league',
    table_name='raw_main',
    source_column='event',
    id_alias='event_name_id',
    name_alias='event_name'
) }}