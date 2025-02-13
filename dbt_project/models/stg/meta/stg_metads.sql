{{
  config(
    materialized = 'table',
    schema = 'test',
    )
}}

SELECT

  adset_name,
  campaign_id,
  campaign_name,
  impressions,
  clicks,
  ctr,
  spend,
  age,
  gender,
  date_start,
  date_stop,
  purchases,
  status,
  effective_status,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'page_engagement' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS page_engagement,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'landing_page_view' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS landing_page_view,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'onsite_web_view_content' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS onsite_web_view_content,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'post_engagement' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS post_engagement,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'view_content' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS view_content,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'video_view' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS video_view,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'purchase' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS purchase,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'add_to_cart' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS add_to_cart,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'initiate_checkout' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS initiate_checkout,

  SUM(CASE WHEN JSON_VALUE(action, '$.action_type') = 'add_payment_info' 
    THEN CAST(NULLIF(JSON_VALUE(action, '$.value'), '') AS INT64) ELSE 0 END) AS add_payment_info

FROM 
  `moss-448416.dataset.meta_insights`,
  UNNEST(JSON_EXTRACT_ARRAY(actions)) AS action

GROUP BY ALL

ORDER BY 
  date_start DESC
