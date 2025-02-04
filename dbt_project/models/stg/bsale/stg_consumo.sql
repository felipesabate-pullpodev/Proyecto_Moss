WITH consumo AS (

    SELECT *
    FROM `moss-448416.dataset.bsale_stock_consumptions`



)

SELECT 
consumption_id,
DATE(TIMESTAMP_SECONDS(consumption_date)) as fecha_consumo,
note,
office_id,
office_name,
variant_id,
quantity,
cost
FROM consumo 