{{
  config(
    materialized = 'ephemeral',
    )
}}

WITH stock AS (
    SELECT 
        id,
        CAST(quantity AS FLOAT64) AS quantity,
        CAST(quantityReserved AS FLOAT64) AS quantity_reserved,
        CAST(quantityAvailable AS FLOAT64) AS quantity_available,
        JSON_VALUE(variant, '$.id') AS variant_id,
        JSON_VALUE(variant, '$.description') AS variant_description,
        JSON_VALUE(variant, '$.barCode') AS variant_barcode,
        JSON_VALUE(variant, '$.code') AS variant_code,
        JSON_VALUE(variant, '$.product.id') AS product_id,
        JSON_VALUE(office, '$.id') AS office_id,
        JSON_VALUE(office, '$.name') AS office_name,
        JSON_VALUE(office, '$.address') AS office_address,
        JSON_VALUE(office, '$.city') AS office_city,
        JSON_VALUE(office, '$.country') AS office_country
    FROM `moss-448416.dataset.bsale_stock_actual`
)

SELECT * FROM stock