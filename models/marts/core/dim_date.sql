{{ config(
    materialized='table',
    unique_key='date_id'
) }}

WITH date_spine AS (
    -- 1. Generamos una lista de días usando la macro de dbt_utils
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

calculated AS (
    SELECT
        date_day,
        -- Clave entera para joins más rápidos en Power BI (YYYYMMDD)
        CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INT) AS date_id, -- PK: 20251125        
        -- Año
        EXTRACT(YEAR FROM date_day) AS year,
        
        -- Trimestre
        EXTRACT(QUARTER FROM date_day) AS quarter,
        'Q' || EXTRACT(QUARTER FROM date_day) AS quarter_label, 
        
        -- Mes
        EXTRACT(MONTH FROM date_day) AS month_num,
        TO_CHAR(date_day, 'MMMM') AS month_name, 
        TO_CHAR(date_day, 'MON') AS month_short, 
        
        -- Semana
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        
        -- Día
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DOW FROM date_day) AS day_of_week_num, 
        TO_CHAR(date_day, 'Day') AS day_name, 
        TO_CHAR(date_day, 'Dy') AS day_short, 

        CASE 
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend
    
    FROM date_spine
)

SELECT
    date_id,
    date_day,
    year,
    quarter,
    quarter_label,
    month_num,
    month_name,
    month_short,
    week_of_year,
    day_of_month,
    day_of_week_num,
    day_name,
    day_short,
    is_weekend,
    

    CAST(year AS VARCHAR) || '-Q' || CAST(quarter AS VARCHAR) AS year_quarter,
    

    CAST(year AS VARCHAR) || '-' || LPAD(CAST(month_num AS VARCHAR), 2, '0') AS year_month_code

FROM calculated