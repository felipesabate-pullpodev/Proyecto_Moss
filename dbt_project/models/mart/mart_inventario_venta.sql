{{
  config(
    materialized = 'table',
    schema = 'mart'
    )
}}


WITH 
stock_actual AS (
    SELECT
        variant_code,
        variant_id,
        SUM(quantity) AS stock_actual
    FROM {{ ref('stg_actual_stock') }}
    GROUP BY variant_code, variant_id -- Aseguramos agrupar correctamente
),
-- La columna sku es como 
maestra_productos AS (
    SELECT *
    FROM {{ ref('raw_master_product') }}
),
raw_maestra_producto2 AS (
    SELECT 
        CAST(int64_field_0 AS STRING) AS variante_code_desc, -- Convertimos a STRING si no lo es
        string_field_1 AS grupo_producto,
        string_field_2 AS categoria
    FROM `moss-448416.dataset.maestra_producto_2`
),
color_camisas AS (
    SELECT *
    FROM {{ ref('color_camisas') }}
),
ventas AS (
    SELECT 
        variant_code,
        variant_id,
        fecha_documento,
        detail_quantity
    FROM {{ ref('core_document_explode') }}
    WHERE fecha_documento IS NOT NULL -- Aseguramos que no haya valores nulos
),
ventas_filtradas AS (
    SELECT 
        variant_code,
        variant_id,
        DATE_TRUNC(fecha_documento, WEEK(MONDAY)) AS semana, -- Agrupamos por semanas comenzando los lunes
        SUM(detail_quantity) AS total_quantity,
        COUNT(DISTINCT fecha_documento) AS dias_venta
    FROM ventas
    WHERE fecha_documento < CURRENT_DATE -- Excluimos la semana actual
    GROUP BY variant_code, variant_id, semana -- Agregamos variant_code al GROUP BY
),
ventas_rankeadas AS (
    SELECT 
        variant_id,
        variant_code,
        semana,
        total_quantity,
        dias_venta,
        ROW_NUMBER() OVER (PARTITION BY variant_id ORDER BY semana DESC) AS semana_num -- Numeramos semanas por cada variant_id
    FROM ventas_filtradas
),
venta_final AS (
    SELECT 
        variant_id,
        variant_code, -- Agregamos variant_code al SELECT
        AVG(CASE WHEN semana_num BETWEEN 2 AND 5 THEN total_quantity / dias_venta END) AS avg_cuatro_semanas,
        AVG(CASE WHEN semana_num BETWEEN 2 AND 3 THEN total_quantity / dias_venta END) AS avg_dos_semanas,
        AVG(CASE WHEN semana_num = 2 THEN total_quantity / dias_venta END) AS avg_ultima_semana
    FROM ventas_rankeadas
    GROUP BY variant_id, variant_code -- Agregamos variant_code al GROUP BY
)

SELECT 
    s.variant_id,
    m.*,
    CASE
  WHEN m.variante = 'S' THEN 1
  WHEN m.variante = 'M' THEN 2
  WHEN m.variante = 'L' THEN 3
  WHEN m.variante = 'XL' THEN 4
  WHEN m.variante = 'XXL' THEN 5
  WHEN m.variante = '3XL' THEN 6
  WHEN m.variante = '4XL' THEN 7
END AS orden_tallas,
    mg.variante_code_desc,
    mg.grupo_producto,
    mg.categoria,
    vf.avg_cuatro_semanas,
    vf.avg_dos_semanas,
    vf.avg_ultima_semana,
    s.stock_actual,
    
FROM stock_actual s
LEFT JOIN maestra_productos m 
    ON s.variant_code = m.sku
LEFT JOIN raw_maestra_producto2 mg 
    ON LEFT(CAST(s.variant_code AS STRING), 4) = mg.variante_code_desc
LEFT JOIN venta_final vf
    ON s.variant_id = CAST(vf.variant_id AS STRING)
