{{ config(
    materialized='view'
) }}

{{ generate_lookup_dim(
    source_name='rocket_league',
    table_name='raw_main',
    source_column='stage',
    id_alias='stage_id',
    name_alias='stage_name'
) }}
