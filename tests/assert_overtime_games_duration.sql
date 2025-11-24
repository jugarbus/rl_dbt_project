-- Este test falla si encuentra algún partido con Overtime que dure 5 minutos o menos
SELECT *
FROM {{ ref('stg_rocket_league__games') }}
WHERE is_overtime = TRUE 
  AND game_duration_secs <= 300