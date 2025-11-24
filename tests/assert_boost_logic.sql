SELECT *
FROM {{ ref('stg_rocket_league__player_stats') }}
WHERE boost_amnt_stolen > boost_amnt_collected