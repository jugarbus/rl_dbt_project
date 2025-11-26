{{ config(materialized='ephemeral') }}

WITH player_stats AS (
    SELECT * FROM {{ ref('stg_rocket_league__games_players') }}
)

SELECT
    game_player_id,
    game_id, 
    player_id,
    team_id,
    platform_id,
    car_id,
    
    -- Métricas base
    goals,
    shots,
    saves,
    assists,
    demo_inflicted,
    demo_taken,
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
    

    
    -- Puntería
    CASE 
        WHEN shots > 0 THEN (goals::numeric / shots::numeric) 
        ELSE 0 
    END AS shooting_percentage,

    -- Perfil Agresivo
    (positioning_percent_offensive_third + positioning_percent_most_forward) / 2 AS offensive_intensity_score,
    (demo_inflicted + (boost_amnt_stolen / 100)) AS disruption_score, 

    -- Perfil Defensivo
    (positioning_percent_defensive_third + positioning_percent_most_back) / 2 AS defensive_discipline_score,
    positioning_percent_behind_ball AS safety_score,
    
    
    -- Metadatos
    is_mvp,
    is_winner,
    data_load

FROM player_stats