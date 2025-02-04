{{
  config(
    materialized = 'table',
    schema = 'mart'
    )
}}

WITH stock AS (
    SELECT 
        stock_id,
        variant_id,
        variant_code,
        CAST(office_id AS STRING) AS office_id,  -- 🔹 Convertimos office_id a STRING
        quantity AS stock_actual,
        quantity_reserved,
        quantity_available
    FROM `moss-448416.dataset.bsale_stock_actual`
),

recibos AS (
    SELECT 
        recepcion_id,
        DATE(TIMESTAMP_SECONDS(admission_date)) AS fecha_recibo,
        document_number,
        note AS nota_recibo,
        CAST(office_id AS STRING) AS office_id,  -- 🔹 Convertimos office_id a STRING
        office_name,
        variant_id,
        quantity AS cantidad_recibida,
        cost AS costo_recibo
    FROM `moss-448416.dataset.bsale_stock_receptions`
    
),

consumos AS (
    SELECT 
        consumption_id,
        DATE(TIMESTAMP_SECONDS(consumption_date)) AS fecha_consumo,
        note AS nota_consumo,
        CAST(office_id AS STRING) AS office_id,  -- 🔹 Convertimos office_id a STRING
        office_name,
        variant_id,
        quantity AS cantidad_consumida,
        cost AS costo_consumo
    FROM `moss-448416.dataset.bsale_stock_consumptions`
    WHERE variant_id = '9232'
),

ventas_historicas AS (
    SELECT 
        CAST(variant_id AS STRING) AS variant_id,
        variant_code,
       CAST(office_id AS STRING) AS office_id ,  -- 🔹 Convertimos office a STRING
        fecha_documento AS fecha_venta,  -- 🔹 Aseguramos que exista fecha_venta
        SUM(detail_quantity) AS cantidad_vendida
    FROM {{ ref('core_document_explode') }}
     WHERE variant_id = 9232
    GROUP BY variant_id, variant_code, office_id, fecha_documento
),

stock_historico AS (
    SELECT 
        s.variant_id,
        s.variant_code,
        s.office_id,
        
        s.stock_actual,
        s.quantity_reserved,
        s.quantity_available,

        r.fecha_recibo,
        r.cantidad_recibida,
        r.costo_recibo,
        r.nota_recibo,

        c.fecha_consumo,
        c.cantidad_consumida,
        c.costo_consumo,
        c.nota_consumo,

        v.fecha_venta,
        v.cantidad_vendida

    FROM stock s
    LEFT JOIN recibos r ON s.variant_id = r.variant_id AND s.office_id = r.office_id
    LEFT JOIN consumos c ON s.variant_id = c.variant_id AND s.office_id = c.office_id
    LEFT JOIN ventas_historicas v ON s.variant_id = v.variant_id AND s.office_id = v.office_id
)

SELECT * FROM stock_historico
