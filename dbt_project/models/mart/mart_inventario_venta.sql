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
        SUM(detail_quantity) as unidades_vendidas
    FROM {{ ref('core_document_explode') }}
    group by all
 
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
    v.unidades_vendidas,
    s.stock_actual
    
FROM stock_actual s
LEFT JOIN maestra_productos m 
    ON s.variant_code = m.sku
LEFT JOIN raw_maestra_producto2 mg 
    ON LEFT(CAST(s.variant_code AS STRING), 4) = mg.variante_code_desc
LEFT JOIN ventas v
    ON s.variant_id = CAST(v.variant_id AS STRING)
