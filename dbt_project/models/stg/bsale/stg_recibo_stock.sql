WITH recibo AS (
    SELECT *
    FROM `moss-448416.dataset.bsale_stock_receptions`
)

SELECT 
recepcion_id,
DATE(TIMESTAMP_SECONDS(admission_date)) as fecha_recibo,
document_number,
note,
office_id,
office_name,
variant_id,
quantity,
cost
FROM recibo