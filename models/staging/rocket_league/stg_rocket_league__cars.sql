{{ config(
    materialized='view'
) }}

{{ generate_lookup_dim(
    source_name='rocket_league',
    table_name='raw_games_players',
    source_column='car_name',
    id_alias='car_id',
    name_alias='car_name'
) }}