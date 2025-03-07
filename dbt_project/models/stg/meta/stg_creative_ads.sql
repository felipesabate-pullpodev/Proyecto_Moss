{{
  config(
    materialized = 'ephemeral'
    )
}}


SELECT 
ad_id AS ad_id_c, -- es el ad id 
ad_creative_id,
body,
title,
video_id,
thumbnail_url,
call_to_action_type,
status as status_creative

FROM `moss-448416.dataset.meta_dim_creative`