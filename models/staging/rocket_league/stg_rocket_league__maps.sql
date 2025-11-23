{{ config(
    materialized='view'
) }}

{{ generate_lookup_dim(
    source_name='rocket_league',
    table_name='raw_main',
    source_column='map_name',
    id_alias='map_id',
    name_alias='map_name'
) }}