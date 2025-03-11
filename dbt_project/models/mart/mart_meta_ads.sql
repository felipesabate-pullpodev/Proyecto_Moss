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
adset_number AS (
  SELECT 
    COUNT(DISTINCT ad_id) AS cantidad_ads,
    adset_id
  FROM meta_ads
  WHERE ad_is_active = 'Active'
  GROUP BY ALL 
)


SELECT 
m.*,
c.*,
ad.cantidad_ads
FROM meta_ads m 
LEFT JOIN creative c  ON m.ad_id = c.ad_id_c
LEFT JOIN adset_number ad
  ON m.adset_id = ad.adset_id