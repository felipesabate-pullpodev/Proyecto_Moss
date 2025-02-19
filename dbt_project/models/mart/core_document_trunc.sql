{{
  config(
    materialized = 'table',
    schema = 'mart'
    )
}}



SELECT 
    DATE_TRUNC(fecha_documento, DAY) AS fecha,  -- Corrección aquí
    SUM(detail_quantity) AS unidades_vendidas,
    SUM(detail_net_amount) AS venta_neta,
    MAX(precio_unitario) AS precio_unitario,
    variant_description,
    orden_tallas,
    tipo_producto,
    nombre_producto,
    grupo_producto,
    variant_id,
    categoria,
    color,
    office as sucursal,
    year,
    month,
    day,
    week_range
FROM {{ ref('core_document_explode') }}
WHERE document_name IN ('NOTA DE CRÉDITO ELECTRÓNICA T', 'FACTURA DE COMPRA ELECTRÓNICA', 'BOLETA ELECTRÓNICA T', 'FACTURA ELECTRÓNICA T', 'NOTA DE DÉBITO ELECTRÓNICA T')
GROUP BY ALL
