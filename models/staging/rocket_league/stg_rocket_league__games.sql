{{ config(
    materialized='view'
) }}

WITH src_main AS (
    SELECT *
    FROM {{ source('rocket_league', 'raw_main') }}
),

-- 1. Filtrado y Normalización
filtered AS (
    SELECT
        LOWER(TRIM(game_id::varchar)) AS game_id,
        LOWER(TRIM(match_id::varchar)) AS match_id,
        
        -- Fechas y Tiempos
        CONVERT_TIMEZONE('UTC', match_date) AS match_date_utc, 
        CONVERT_TIMEZONE('UTC', game_date) AS original_game_date_utc, 
        
        -- Datos del juego
        game_number::int AS game_number,
        game_duration::int AS game_duration_secs,
        LOWER(COALESCE(TRIM(map_name::varchar), '{{ var("unknown_var") }}')) AS map_name, 
        overtime::boolean AS overtime

    FROM src_main
    WHERE game_id IS NOT NULL
),

-- 2. Cálculo del Offset (Tiempo acumulado)
time_offset_calculation AS (
    SELECT
        *,
        -- Suma acumulada de la duración de los juegos ANTERIORES
        -- + 300 segundos de descanso entre cada juego.
        SUM(COALESCE(game_duration_secs, 300) + 300) OVER (
            PARTITION BY match_id 
            ORDER BY game_number ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS seconds_offset
    FROM filtered
),

-- 3. Imputación de Fechas Nulas
imput_null_game_dates AS (
    SELECT
        game_id,
        match_id,
        game_number,
        
        -- LÓGICA: Si no hay fecha de juego, usa fecha de partido + offset
        COALESCE(
            original_game_date_utc, 
            DATEADD(second, COALESCE(seconds_offset, 0), match_date_utc)
        ) AS final_game_date_utc,
        
        game_duration_secs,
        map_name,
        overtime
    FROM time_offset_calculation
),

-- 4. Deduplicación
uniques AS (
    SELECT DISTINCT
        game_id,         
        match_id,        
        game_number,
        final_game_date_utc AS game_date_utc, 
        game_duration_secs,
        map_name,         
        overtime
    FROM imput_null_game_dates
),

-- 5. Generación de Claves (Hashes)
surrogate AS (
    SELECT 
        -- PK
        {{ dbt_utils.generate_surrogate_key(['game_id']) }} AS game_id,
        
        -- Foreign Keys
        {{ dbt_utils.generate_surrogate_key(['match_id']) }} AS match_id,
        {{ dbt_utils.generate_surrogate_key(['map_name']) }} AS map_id,
        
        -- Datos
        game_number,
        game_date_utc, 
        game_duration_secs,
        overtime AS is_overtime
    FROM uniques
)

SELECT *
FROM surrogate