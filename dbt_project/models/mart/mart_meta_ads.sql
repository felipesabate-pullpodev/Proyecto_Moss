{{
  config(
    materialized = 'table',
    schema = 'mart'
    )
}}

WITH meta_ads AS (


    SELECT *
    FROM {{ ref('stg_metads') }}

),

creative AS (

    SELECT *
    FROM {{ ref('stg_creative_ads') }}


),

-- Agregar filtro para poder contar ads

adset_number AS (
   SELECT 
    COUNT(DISTINCT ad_id) AS cantidad_ads,
    adset_id,
    date_start
  FROM meta_ads
  WHERE spend > 0
  GROUP BY ALL 
)


SELECT 
m.*,
c.*,
ad.cantidad_ads,
(CASE WHEN CAST(m.adset_daily_budget AS FLOAT64) > 1 THEN CAST(m.adset_daily_budget AS FLOAT64) ELSE  CAST(m.campaign_daily_budget AS FLOAT64) END) / ad.cantidad_ads AS distributed_budget

FROM meta_ads m 
LEFT JOIN creative c  ON m.ad_id = c.ad_id_c
LEFT JOIN adset_number ad
  ON m.adset_id = ad.adset_id AND m.date_start = ad.date_start