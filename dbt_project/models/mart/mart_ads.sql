{{
  config(
    materialized = 'table',
    schema = 'mart'
    )
}}

WITH fbads AS (
SELECT *
FROM {{ ref('stg_meta_ads') }}
),

date_table AS (

  SELECT *
  FROM {{ ref('date_table') }}

)

SELECT *
FROM fbads f
LEFT JOIN date_table dt
ON f.campaign_date = dt.date_raw