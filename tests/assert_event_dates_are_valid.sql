-- tests/assert_event_dates_are_valid.sql

SELECT *
FROM {{ ref('stg_rocket_league__events') }}
WHERE event_start_date_utc > event_end_date_utc
-- Si esto devuelve filas, significa que hay eventos que terminan antes de empezar (ERROR)