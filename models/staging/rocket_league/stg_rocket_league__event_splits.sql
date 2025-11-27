{{ config(
    materialized='view'
) }}

{{ generate_lookup_dim(
    source_name='rocket_league',
    table_name='raw_main',
    source_column='event_split',
    id_alias='event_split_id',
    name_alias='event_split_name'
) }}