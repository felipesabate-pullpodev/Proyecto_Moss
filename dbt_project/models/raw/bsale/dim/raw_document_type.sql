{{
  config(
    materialized = 'ephemeral',
    )
}}

SELECT * FROM `moss-448416.dataset.dim_document_type`