{{
  config(
    materialized = 'ephemeral'
  )
}}

WITH base AS (
  SELECT
    ad_id,
    ad_name,
    adset_name,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    ctr,
    CAST(spend AS FLOAT64) AS spend,
    date(date_start) AS date_start,
    date(date_stop) AS date_stop,
    status,
    effective_status,
    action_values,
    actions
  FROM `moss-448416.dataset.meta_insights`
),
agg_actions AS (
  SELECT
    ad_id,
    date_start,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'page_engagement' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS page_engagement,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'landing_page_view' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS landing_page_view,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'onsite_web_view_content' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS onsite_web_view_content,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'post_engagement' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS post_engagement,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'view_content' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS view_content,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'video_view' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS video_view,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'purchase' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS purchase,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'add_to_cart' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS add_to_cart,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'initiate_checkout' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS initiate_checkout,
    SUM(CASE 
          WHEN JSON_VALUE(a, '$.action_type') = 'add_payment_info' 
          THEN CAST(NULLIF(JSON_VALUE(a, '$.value'), '') AS INT64) 
          ELSE 0 END) AS add_payment_info
  FROM base,
       UNNEST(JSON_QUERY_ARRAY(actions)) AS a
  GROUP BY ad_id, date_start
),
agg_action_values AS (
  SELECT
    ad_id,
    date_start,
    SUM(CASE 
          WHEN JSON_VALUE(av, '$.action_type') = 'purchase' 
          THEN CAST(NULLIF(JSON_VALUE(av, '$.value'), '') AS FLOAT64) 
          ELSE 0 END) AS purchase_amount
  FROM base,
       UNNEST(JSON_QUERY_ARRAY(action_values)) AS av
  GROUP BY ad_id, date_start
),

recent_activity AS (
  SELECT
    ad_id,
    SUM(CAST(impressions AS FLOAT64)) AS total_spend
  FROM base
  -- Tomamos sólo data de los últimos 2 días
  WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY ad_id
)
SELECT
  b.ad_id,
  b.ad_name,
  b.adset_name,
  b.campaign_id,
  b.campaign_name,
  b.impressions,
  b.clicks,
  b.ctr,
  b.spend,
  CAST(aa.purchase AS FLOAT64) AS purchase,
  CAST(aav.purchase_amount AS FLOAT64) AS purchase_amount,
  b.date_start,
  b.date_stop,
  b.status,
  b.effective_status,
  b.action_values,
  b.actions,
  aa.page_engagement,
  aa.landing_page_view,
  aa.onsite_web_view_content,
  aa.post_engagement,
  aa.view_content,
  aa.video_view,
  aa.add_to_cart,
  aa.initiate_checkout,
  aa.add_payment_info,
   CASE
    WHEN (r.total_spend > 0) THEN 'Active'
    ELSE 'Inactive'
    END AS ad_is_active
FROM base b
LEFT JOIN agg_actions aa
  ON b.ad_id = aa.ad_id AND b.date_start = aa.date_start
LEFT JOIN agg_action_values aav
  ON b.ad_id = aav.ad_id AND b.date_start = aav.date_start
LEFT JOIN recent_activity r ON b.ad_id = r.ad_id
ORDER BY b.date_start DESC
