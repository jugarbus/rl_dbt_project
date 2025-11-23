{{
  config(
    materialized='view'
  )
}}

WITH base_games_players AS (
    SELECT * FROM {{ ref('base_rocket_league__games_players') }} 
),


filter_columns AS (
    SELECT
    game_player_id,
    game_id,
    player_id,
    team_id, 
    platform_id, 
    car_id,
    boost_bpm,
    boost_bcpm,
    boost_avg_amnt,
    boost_amnt_collected,
    boost_amnt_stolen,
    movement_avg_speed,
    movement_percent_slow_speed,
    movement_percent_supersonic_speed,
    movement_percent_ground,
    movement_percent_high_air,
    positioning_percent_defensive_third,
    positioning_percent_offensive_third,
    positioning_percent_behind_ball,
    positioning_percent_most_back,
    positioning_percent_most_forward,
    shots,
    goals,
    saves,
    assists,
    demo_inflicted,
    demo_taken,
    is_mvp,
    is_winner,
    data_load

    FROM base_games_players
)

SELECT * FROM filter_columns