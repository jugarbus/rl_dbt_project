{{ config(
    materialized='view'
) }}

WITH matches AS (
    SELECT * FROM {{ ref('stg_rocket_league__matches') }}
),

match_rounds AS (
    SELECT * FROM {{ ref('stg_rocket_league__match_rounds') }}
),

match_formats AS (
    SELECT * FROM {{ ref('stg_rocket_league__match_formats') }}
)

SELECT
m.match_id,
m.match_number,
mr.match_round_name AS match_round,
mf.match_format_name AS match_format,
m.reverse_sweep_attempt AS is_reverse_sweep_attempt,
m.reverse_sweep AS is_reverse_sweep,
m.match_date_utc,
m.match_url

FROM matches m

LEFT JOIN match_rounds mr
    ON m.match_round_id = mr.match_round_id 

LEFT JOIN match_formats mf 
    ON  m.match_format_id = mf.match_format_id 
ORDER BY 
    m.match_date_utc ASC,  
    m.match_number ASC     