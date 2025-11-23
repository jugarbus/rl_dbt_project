{{ config(
    materialized='view'
) }}

{{ generate_lookup_dim(
    source_name='rocket_league',
    table_name='raw_player_countries',
    source_column='continent',
    id_alias='continent_id',
    name_alias='continent_name'
) }}



