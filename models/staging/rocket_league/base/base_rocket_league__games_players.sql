{{
  config(
    materialized='incremental',
    unique_key='game_player_id',
    on_schema_change='append_new_columns'
  )
}}

WITH src_games_players AS (
    SELECT * FROM {{ source('rocket_league', 'raw_games_players') }}

        {% if is_incremental() %}
      where data_load > (select max(data_load) from {{ this }})
    {% endif %}
),

normalized AS (
    SELECT
        LOWER(TRIM(game_id::varchar)) AS game_id_clean,
        LOWER(TRIM(player_id::varchar)) AS player_id_clean,
        LOWER(TRIM(team_id::varchar)) AS team_id_clean,
        
    LOWER(COALESCE(TRIM(
        CASE 
            WHEN LOWER(platform::varchar) = 'psynet' THEN '{{ var("unknown_platform") }}'
            ELSE LOWER(platform::varchar)
        END 
    ), '{{ var("unknown_var") }}') ) AS platform_clean,

        LOWER(COALESCE(TRIM(car_name::varchar), '{{ var("unknown_var") }}')) AS car_name_clean,

        -- BOOST
        boost_bpm::number(38,0) AS boost_bpm,
        boost_bcpm::number(38,4) AS boost_bcpm,
        boost_avg_amount::number(38,4) AS boost_avg_amnt,
        boost_amount_collected::number(38,4) AS boost_amnt_collected,
        boost_amount_stolen::number(38,0) AS boost_amnt_stolen,

        -- MOVEMENT
        movement_avg_speed::number(38,4) AS movement_avg_speed,
        movement_percent_slow_speed::number(38,4) AS movement_percent_slow_speed,
        movement_percent_supersonic_speed::number(38,4) AS movement_percent_supersonic_speed,
        movement_percent_ground::number(38,4) AS movement_percent_ground,
        movement_percent_high_air::number(38,4) AS movement_percent_high_air,

        -- POSITIONING
        positioning_percent_defensive_third::number(38,4) AS positioning_percent_defensive_third,
        positioning_percent_offensive_third::number(38,4) AS positioning_percent_offensive_third,
        positioning_percent_behind_ball::number(38,4) AS positioning_percent_behind_ball,
        positioning_percent_most_back::number(38,4) AS positioning_percent_most_back,
        positioning_percent_most_forward::number(38,4) AS positioning_percent_most_forward,

        -- CORE STATS
        core_shots::int AS shots,
        core_goals::int AS goals,
        core_saves::int AS saves,
        core_assists::int AS assists,

        -- DEMOS
        demo_inflicted::int AS demo_inflicted,
        demo_taken::int AS demo_taken,

        -- MVP
        advanced_mvp::boolean AS is_mvp,

        winner::boolean AS is_winner,
        CONVERT_TIMEZONE('UTC', data_load) AS data_load

    FROM src_games_players
)

SELECT 

        {{ dbt_utils.generate_surrogate_key(['game_id_clean', 'player_id_clean']) }} AS game_player_id,
        {{ dbt_utils.generate_surrogate_key(['game_id_clean']) }} AS game_id,
        {{ dbt_utils.generate_surrogate_key(['player_id_clean']) }} AS player_id,
        {{ dbt_utils.generate_surrogate_key(['team_id_clean']) }} AS team_id, 
        {{ dbt_utils.generate_surrogate_key(['platform_clean']) }} AS platform_id, 
        {{ dbt_utils.generate_surrogate_key(['car_name_clean']) }} AS car_id,

        platform_clean AS platform,
        car_name_clean AS car_name,

        -- BOOST
        boost_bpm,
        boost_bcpm,
        boost_avg_amnt,
        boost_amnt_collected,
        boost_amnt_stolen,

        -- MOVEMENT
        movement_avg_speed,
        movement_percent_slow_speed,
        movement_percent_supersonic_speed,
        movement_percent_ground,
        movement_percent_high_air,

        -- POSITIONING
        positioning_percent_defensive_third,
        positioning_percent_offensive_third,
        positioning_percent_behind_ball,
        positioning_percent_most_back,
        positioning_percent_most_forward,

        -- CORE STATS
        shots,
        goals,
        saves,
        assists,

        -- DEMOS
        demo_inflicted,
        demo_taken,

        -- MVP
        is_mvp,
        is_winner,
        data_load
FROM
normalized
