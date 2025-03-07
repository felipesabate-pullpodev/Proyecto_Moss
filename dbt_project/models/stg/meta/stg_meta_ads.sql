{{ config(materialized='ephemeral') }}

SELECT
    ad_id,
    ad_name,
    adset_id,
    adset_name,
    campaign_id,
    campaign_name,
    DATE_TRUNC(date_start, DAY) AS campaign_date,  -- o simplemente date_start
    date_stop,
    spend,
    impressions,
    clicks,
    reach,
    cpm,
    ctr,
    cpc,
    cpp,
    publisher_platform,
    platform_position,
    impression_device,
    -- si gustas, incluyes otras columnas relevantes
    -- por ejemplo:
    account_id,
    account_name,
    frequency
FROM `moss-448416.dataset.meta_ads_insights_platform_and_device`