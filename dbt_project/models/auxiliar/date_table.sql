{{ config(
    materialized="table",
    schema="aux_table"
) }}

WITH date_series AS (
    SELECT 
      DATE_ADD(DATE('2023-01-02'), INTERVAL day_offset DAY) AS fecha
    FROM UNNEST(GENERATE_ARRAY(0, 365 * 3)) AS day_offset
    WHERE DATE_ADD(DATE('2023-01-02'), INTERVAL day_offset DAY) <= DATE('2025-12-31')
),
week_series AS (
    SELECT
        fecha,
        EXTRACT(YEAR  FROM fecha)  AS year,
        EXTRACT(MONTH FROM fecha)  AS month,
        EXTRACT(DAY   FROM fecha)  AS day,

        -- Lunes como "week_start"
        DATE_ADD(
            fecha,
            INTERVAL 
                -1 * MOD(EXTRACT(DAYOFWEEK FROM fecha) + 5, 7)
            DAY
        ) AS week_start,

        -- Domingo como "week_end"
        DATE_ADD(
            fecha,
            INTERVAL 
                6 - MOD(EXTRACT(DAYOFWEEK FROM fecha) + 5, 7)
            DAY
        ) AS week_end
    FROM date_series
)
SELECT
    fecha AS date_raw,
    year,
    month,
    CASE month
        WHEN 1  THEN 'Enero'
        WHEN 2  THEN 'Febrero'
        WHEN 3  THEN 'Marzo'
        WHEN 4  THEN 'Abril'
        WHEN 5  THEN 'Mayo'
        WHEN 6  THEN 'Junio'
        WHEN 7  THEN 'Julio'
        WHEN 8  THEN 'Agosto'
        WHEN 9  THEN 'Septiembre'
        WHEN 10 THEN 'Octubre'
        WHEN 11 THEN 'Noviembre'
        WHEN 12 THEN 'Diciembre'
    END AS month_name,
    CONCAT(CAST(year AS STRING), '/', CAST(month AS STRING)) AS year_month,
    day,
    
    -- Asigna número de semana manual con inicio lunes
    DENSE_RANK() OVER (
      PARTITION BY EXTRACT(YEAR FROM week_start)
      ORDER BY week_start
    ) AS week_of_year,
    
    EXTRACT(DAYOFWEEK FROM fecha) AS weekday,

    -- Día de la semana en español
    CASE EXTRACT(DAYOFWEEK FROM fecha)
        WHEN 1 THEN 'Domingo'
        WHEN 2 THEN 'Lunes'
        WHEN 3 THEN 'Martes'
        WHEN 4 THEN 'Miércoles'
        WHEN 5 THEN 'Jueves'
        WHEN 6 THEN 'Viernes'
        WHEN 7 THEN 'Sábado'
    END AS weekday_name,

    -- Semana del mes
    DENSE_RANK() OVER (PARTITION BY year, month ORDER BY week_start) AS week_of_month,
    
    CONCAT(
      FORMAT_DATE('%Y-%m-%d', week_start),
      ' / ',
      FORMAT_DATE('%Y-%m-%d', week_end)
    ) AS week_range,
    
    CASE 
        WHEN month BETWEEN 1 AND 6 THEN 'S1'
        ELSE 'S2'
    END AS semester,
    
    CASE
        WHEN month BETWEEN 1 AND 3   THEN 'Q1'
        WHEN month BETWEEN 4 AND 6   THEN 'Q2'
        WHEN month BETWEEN 7 AND 9   THEN 'Q3'
        ELSE 'Q4'
    END AS quarter

FROM week_series
ORDER BY date_raw DESC

