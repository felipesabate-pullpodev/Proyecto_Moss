{{
  config(
    materialized = 'table',
    schema = 'mart'
    )
}}


SELECT *
FROM {{ ref('stg_meta_ads') }}