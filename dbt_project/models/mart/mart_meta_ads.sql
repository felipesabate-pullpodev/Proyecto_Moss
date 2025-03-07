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

)

SELECT *
FROM meta_ads m 
LEFT JOIN creative c  ON m.ad_id = c.ad_id_c
