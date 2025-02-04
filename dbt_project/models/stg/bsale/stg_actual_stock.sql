WITH stock AS (
SELECT *
FROM `moss-448416.dataset.bsale_stock_actual`
)

SELECT 
stock_id,
variant_id,
variant_code,
office_id,
quantity,
quantity_reserved,
quantity_available
FROM stock

