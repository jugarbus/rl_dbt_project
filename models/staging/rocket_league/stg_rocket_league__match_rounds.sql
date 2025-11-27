{{ config(
    materialized='view'
) }}

{{ generate_lookup_dim(
    source_name='rocket_league',
    table_name='raw_main',
    source_column='match_round',
    id_alias='match_round_id',
    name_alias='match_round_name'
) }}