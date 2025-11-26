{{ config(materialized='ephemeral') }}

WITH player_stats AS (
    SELECT * FROM {{ ref('stg_rocket_league__games_players') }}
)

SELECT
    -- Identificadores
    game_player_id,
    game_id, 
    player_id,
    team_id,
    platform_id,
    car_id,
    
    -- Métricas base (Raw)
    goals,
    shots,
    saves,
    assists,
    demo_inflicted,
    demo_taken,
    boost_bpm, -- boost collected per minute
    boost_bcpm, -- boost consumed per minute
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


    -- KPIs CALCULADOS

    CASE 
        WHEN shots > 0 THEN (goals / shots) 
        ELSE 0 
    END AS shooting_percentage,
    
    CASE 
        WHEN shots > 0 THEN ((goals + assists) / shots)
        ELSE 0
    END AS offensive_conversion_rate,

    -- PERFIL AGRESIVO 
    (positioning_percent_offensive_third + positioning_percent_most_forward) / 2 AS offensive_intensity_score,

    -- Capacidad de molestar (físico + robo de recursos)
    (demo_inflicted + (boost_amnt_stolen / 100)) AS disruption_score,

    -- Hambre de recursos 
    CASE 
        WHEN boost_amnt_collected > 0 THEN ((boost_bcpm + boost_amnt_stolen) / boost_amnt_collected)
        ELSE 0
    END AS boost_hunger_score,

    --PERFIL DEFENSIVO 
    -- Disciplina posicional atrás
    (positioning_percent_defensive_third + positioning_percent_most_back) / 2 AS defensive_discipline_score,

    (saves / (demo_taken + 1)) AS survival_rating,

    -- Gestión conservadora de recursos
    CASE 
        WHEN boost_bpm > 0 THEN (boost_avg_amnt / boost_bcpm)
        ELSE 0
    END AS resource_management_score,

    -- ESTILO DE JUEGO
    -- Ratio de velocidad (Supersónico vs Lento)
    CASE 
        WHEN movement_percent_slow_speed > 0 THEN (movement_percent_supersonic_speed / movement_percent_slow_speed)
        ELSE 0
    END AS speed_ratio,

    -- Amenaza aérea
    (movement_percent_high_air * movement_percent_supersonic_speed) / 100 AS aerial_threat_index,

    -- Metadatos
    is_mvp,
    is_winner,
    data_load

FROM player_stats