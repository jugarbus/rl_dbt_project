{{
  config(
    materialized='incremental',
    unique_key='game_player_id',
    on_schema_change='append_new_columns'
  )
}}

WITH player_stats AS (
    SELECT * FROM {{ ref('int_rocket_league__add_kpis') }}
    {% if is_incremental() %}
    WHERE data_load > (SELECT max(data_load) FROM {{ this }})
{% endif %}

),

game_hierarchy AS (
    SELECT * FROM {{ ref('int_rocket_league__game_hierarchy') }}
)

SELECT
    -- PK
    s.game_player_id AS game_player_id,

    -- FKs (Todas vienen limpias del hierarchy)
    s.game_id,
    h.match_id,
    h.stage_id,
    h.event_id,
    
    s.player_id,
    s.team_id,
    s.car_id,
    s.platform_id,

    -- Métricas
    s.goals,
    s.shots,
    s.shooting_percentage,
    s.saves,
    s.assists,

    s.demo_inflicted,
    s.demo_taken,


    s.boost_bpm,
    s.boost_bcpm,
    s.boost_avg_amnt,
    s.boost_amnt_collected,
    s.boost_amnt_stolen,
    s.movement_avg_speed,
    s.movement_percent_slow_speed,
    s.movement_percent_supersonic_speed,
    s.movement_percent_ground,
    s.movement_percent_high_air,
    s.positioning_percent_defensive_third,
    s.positioning_percent_offensive_third,
    s.positioning_percent_behind_ball,
    s.positioning_percent_most_back,
    s.positioning_percent_most_forward,

-- KPIS
    -- Agresivo
    s.offensive_conversion_rate,
    s.offensive_intensity_score,
    s.disruption_score,
    s.boost_hunger_score,


    -- Defensivo
    s.defensive_discipline_score,
    s.survival_rating,
    s.resource_management_score,


    -- ESTILO DE JUEGO
    s.speed_ratio,
    s.aerial_threat_index,


    s.is_mvp,
    s.is_winner,
    s.data_load AS data_load

FROM player_stats s
LEFT JOIN game_hierarchy h 
    ON s.game_id = h.game_id

