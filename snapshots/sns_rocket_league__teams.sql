{% snapshot sns_rocket_league__teams %}

{{
    config(
      target_database='DEV_SILVER',
      target_schema='snapshots',
      unique_key='team_id_clean', 
      strategy='check',
      check_cols=['team_name_clean'],
    )
}}

WITH teams AS (
    SELECT * FROM {{ source('rocket_league', 'raw_games_teams') }}
),

matches AS (
    -- Traemos la fecha real del match
    SELECT game_id, match_date FROM {{ source('rocket_league', 'raw_main') }}
),

joined_data AS (
    SELECT 
        t.*,
        m.match_date
    FROM teams t
    LEFT JOIN matches m ON t.game_id = m.game_id
),

normalized AS (
    SELECT 
        LOWER(TRIM(team_id::varchar)) AS team_id_clean,
        TRIM(team_slug::varchar) AS team_url_clean,
        TRIM(team_name::varchar) AS team_name_clean,
        LOWER(COALESCE(TRIM(team_region::varchar), '{{ var("unknown_var") }}')) AS team_region_clean,
        
        -- Fechas convertidas
        CONVERT_TIMEZONE('UTC', data_load) AS data_load_utc,
        CONVERT_TIMEZONE('UTC', match_date) AS match_date_utc,
        CONVERT_TIMEZONE('UTC', data_load) AS data_load 


    FROM joined_data
    WHERE team_id IS NOT NULL
),

deduplicated_source AS (
    SELECT *
    FROM normalized
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY team_id_clean 

        -- Prioridad absoluta: La fecha en que ocurrió el partido.
        -- Desempate: Si jugaron 2 partidos el mismo día, se usa data_load.
        ORDER BY match_date_utc DESC, data_load_utc DESC 
    ) = 1
)

SELECT * FROM deduplicated_source

{% endsnapshot %}