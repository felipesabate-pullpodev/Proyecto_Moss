{{
  config(
    materialized = 'table',
    schema = 'mart'
  )
}}

WITH stock AS (
    SELECT 
        stock_id,
        CAST(variant_id AS STRING) AS variant_id,
        variant_code,
        CAST(office_id AS STRING) AS office_id,
        quantity AS stock_actual,
        quantity_reserved,
        quantity_available
    FROM `moss-448416.dataset.bsale_stock_actual`
),

recibos AS (
    SELECT 
        CAST(variant_id AS STRING) AS variant_id,
        CAST(office_id AS STRING) AS office_id,
        DATE(TIMESTAMP_SECONDS(admission_date)) AS fecha_recibo,
        quantity AS cantidad_recibida
    FROM `moss-448416.dataset.bsale_stock_receptions`
),

consumos AS (
    SELECT 
        CAST(variant_id AS STRING) AS variant_id,
        CAST(office_id AS STRING) AS office_id,
        DATE(TIMESTAMP_SECONDS(consumption_date)) AS fecha_consumo,
        quantity AS cantidad_consumida
    FROM `moss-448416.dataset.bsale_stock_consumptions`
),

ventas_historicas AS (
    SELECT 
        CAST(variant_id AS STRING) AS variant_id,
        CAST(office_id AS STRING) AS office_id,
        fecha_documento AS fecha_venta,
        SUM(detail_quantity) AS cantidad_vendida
    FROM {{ ref('core_document_explode') }}
    WHERE document_type_id IN (9, 1, 18, 6, 19)  -- 🔹 Filtra por document_type_id
    GROUP BY variant_id, office_id, fecha_documento
),

-- 🔹 GENERAMOS UNA TABLA DE FECHAS CON TODOS LOS DÍAS DEL AÑO
fechas AS (
    SELECT 
        DATE_ADD(DATE('2024-01-01'), INTERVAL n DAY) AS fecha
    FROM UNNEST(GENERATE_ARRAY(0, 364)) AS n
),

movimientos AS (
    SELECT 
        fecha_recibo AS fecha,
        variant_id,
        office_id,
        cantidad_recibida AS ingreso,
        0 AS salida
    FROM recibos

    UNION ALL

    SELECT 
        fecha_consumo AS fecha,
        variant_id,
        office_id,
        0 AS ingreso,
        cantidad_consumida AS salida
    FROM consumos

    UNION ALL

    SELECT 
        fecha_venta AS fecha,
        variant_id,
        office_id,
        0 AS ingreso,
        cantidad_vendida AS salida
    FROM ventas_historicas
),

-- 🔹 UNIMOS LAS FECHAS CON LOS MOVIMIENTOS PARA TENER UNA BASE COMPLETA
base_stock AS (
    SELECT 
        f.fecha,
        s.variant_id,
        s.office_id,
        COALESCE(m.ingreso, 0) AS ingreso,
        COALESCE(m.salida, 0) AS salida
    FROM fechas f
    CROSS JOIN stock s
    LEFT JOIN movimientos m 
        ON f.fecha = m.fecha 
        AND s.variant_id = m.variant_id 
        AND s.office_id = m.office_id
),

-- 🔹 CALCULAMOS EL STOCK ACUMULADO DÍA A DÍA
stock_acumulado_base AS (
    SELECT 
        fecha,
        variant_id,
        office_id,
        SUM(ingreso - salida) OVER(
            PARTITION BY variant_id, office_id 
            ORDER BY fecha 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS stock_final
    FROM base_stock
),

-- 🔹 COMPLETAMOS LOS DÍAS SIN MOVIMIENTOS CON EL ÚLTIMO STOCK DISPONIBLE
stock_acumulado_lleno AS (
    SELECT 
        fecha,
        variant_id,
        office_id,
        stock_final,
        -- 🔹 Llenamos los valores nulos con el último valor no nulo
        IFNULL(
            LAST_VALUE(stock_final IGNORE NULLS) OVER (
                PARTITION BY variant_id, office_id 
                ORDER BY fecha
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS stock_final_lleno
    FROM stock_acumulado_base
)

SELECT * FROM stock_acumulado_lleno;
