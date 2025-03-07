{{
  config(
    materialized = 'table',
    schema = 'mart'
    )
}}

WITH documents AS (
    SELECT 
        doc_id,
        document_number,
        DATE(TIMESTAMP_SECONDS(emissionDate)) AS fecha_documento,
        DATE(TIMESTAMP_SECONDS(expirationDate)) AS expiration_date,
        DATE(TIMESTAMP_SECONDS(generationDate)) AS generation_date,
        document_type_id,
        document_name,
        office,
        office_id,
        CASE WHEN document_type_id = 9 THEN total_amount * -1 ELSE total_amount END AS total_amount,
        CASE WHEN document_type_id = 9 THEN net_amount * -1 ELSE net_amount END AS net_amount,
        CASE WHEN document_type_id = 9 THEN tax_amount * -1 ELSE tax_amount END AS tax_amount,
        CASE WHEN document_type_id = 9 THEN detail_quantity * -1 ELSE detail_quantity END AS detail_quantity,
        CASE WHEN document_type_id = 9 THEN detail_unit_value * -1 ELSE detail_unit_value END AS detail_unit_value,
        CASE WHEN document_type_id = 9 THEN detail_total_unit_value * -1 ELSE detail_total_unit_value END AS detail_total_unit_value,
        CASE WHEN document_type_id = 9 THEN detail_net_amount * -1 ELSE detail_net_amount END AS detail_net_amount,
        CASE WHEN document_type_id = 9 THEN detail_tax_amount * -1 ELSE detail_tax_amount END AS detail_tax_amount,
        CASE WHEN document_type_id = 9 THEN detail_total_amount * -1 ELSE detail_total_amount END AS detail_total_amount,
        CASE WHEN document_type_id = 9 THEN detail_net_discount * -1 ELSE detail_net_discount END AS detail_net_discount,
        CASE WHEN document_type_id = 9 THEN detail_total_discount * -1 ELSE detail_total_discount END AS detail_total_discount,
        
        -- Columnas sin cambios
        variant_description,
        orden_tallas,
        variant_code,
        variant_id

    FROM {{ ref('stg_document_explote') }}
)
,
fbads AS (
    SELECT 
         CAST(date_start AS DATE) AS date_fb,
        SUM(CAST(spend AS FLOAT64)) AS costo_clp_fbads 
    FROM {{ ref('mart_meta_ads') }}
    GROUP BY date_start
    ORDER BY date_start DESC
)
,
agg AS (
    SELECT
        fecha_documento,
        COUNT(document_number) AS compras_mismo_dia
    FROM documents
    WHERE document_name IN ('NOTA DE CRÉDITO ELECTRÓNICA T', 'FACTURA DE COMPRA ELECTRÓNICA', 'BOLETA ELECTRÓNICA T', 'FACTURA ELECTRÓNICA T', 'NOTA DE DÉBITO ELECTRÓNICA T')
    GROUP BY fecha_documento
) ,

maestra_productos AS (

SELECT *
FROM {{ ref('raw_master_product') }}

)

,
raw_maestra_producto2 AS (
    SELECT 
        CAST(int64_field_0 AS STRING) AS variante_code_desc,  -- Convertimos a STRING si no lo es
        string_field_1 AS grupo_producto,
        string_field_2 AS categoria
    FROM `moss-448416.dataset.maestra_producto_2`
),

date_table AS (

    SELECT * 
    FROM {{ ref('date_table') }}

),

stock_actual AS (


    SELECT
    variant_id,
    SUM(quantity) as stock_actual
    FROM {{ ref('stg_actual_stock') }}
    GROUP BY ALL

)

SELECT 
    d.*,
    CASE
        WHEN d.document_name NOT IN (
            'NOTA DE CRÉDITO ELECTRÓNICA T', 'FACTURA DE COMPRA ELECTRÓNICA', 'BOLETA ELECTRÓNICA T', 'FACTURA ELECTRÓNICA T', 'NOTA DE DÉBITO ELECTRÓNICA T'
        ) THEN 0
        WHEN agg.compras_mismo_dia < 2 OR agg.compras_mismo_dia IS NULL THEN f.costo_clp_fbads
        ELSE f.costo_clp_fbads / agg.compras_mismo_dia 
    END AS inversion_desglosada_fbads,
    mp.tipo_producto,
    mp.nombre_producto,
    CASE WHEN document_type_id = 9 THEN mp.costo_neto * -1 ELSE mp.costo_neto END AS costo_neto,
    CASE WHEN document_type_id = 9 THEN mp.precio_unitario * -1 ELSE mp.precio_unitario END AS precio_unitario,
    rmp2.grupo_producto,
    rmp2.categoria,
    rmp2.variante_code_desc,
    st.stock_actual,
    dt.*,
    CASE
  WHEN variant_code LIKE '%AREN%' THEN 'Arena'
  WHEN variant_code LIKE '%AZUL%' THEN 'Azul'
  WHEN variant_code LIKE '%AZMA%' THEN 'Azul Marino'
  WHEN variant_code LIKE '%BLAN%' THEN 'Blanco'
  WHEN variant_code LIKE '%CALI%' THEN 'Calipso'
  WHEN variant_code LIKE '%CELE%' THEN 'Celeste'
  WHEN variant_code LIKE '%DAMA%' THEN 'Damasco'
  WHEN variant_code LIKE '%ROYA%' THEN 'Royal'
  WHEN variant_code LIKE '%SAND%' THEN 'Sandia'
  WHEN variant_code LIKE '%TURQ%' THEN 'Turquesa'
  WHEN variant_code LIKE '%CAQU%' THEN 'Caqui'
  WHEN variant_code LIKE '%INDG%' THEN 'Indigo'
  WHEN variant_code LIKE '%CEMA%' THEN 'Celeste Mar'
  WHEN variant_code LIKE '%CORA%' THEN 'Coral'
  WHEN variant_code LIKE '%CRUD%' THEN 'Crudo'
  WHEN variant_code LIKE '%CAPA%' THEN 'Calipso Pastel'
  WHEN variant_code LIKE '%AQUA%' THEN 'Aqua'
  WHEN variant_code LIKE '%GRPI%' THEN 'Gris Piedra'
  WHEN variant_code LIKE '%BEIG%' THEN 'Beige'
  WHEN variant_code LIKE '%VERD%' THEN 'Verde'
  WHEN variant_code LIKE '%ROSA%' THEN 'Rosa'
  WHEN variant_code LIKE '%CAFE%' THEN 'Cafe'
  WHEN variant_code LIKE '%NEGR%' THEN 'Negro'
  WHEN variant_code LIKE '%BLD%' THEN 'Blanco Diseño'
  WHEN variant_code LIKE '%MARE%' THEN 'Marengo'
  WHEN variant_code LIKE '%BLOY%' THEN 'Blanco/Royal'
  WHEN variant_code LIKE '%BLAM%' THEN 'Blanco/Azul Marino'
  WHEN variant_code LIKE '%MOST%' THEN 'Mostaza'
  WHEN variant_code LIKE '%VECL%' THEN 'Verde Claro'
  WHEN variant_code LIKE '%TERR%' THEN 'Terracota'
  WHEN variant_code LIKE '%MOVI%' THEN 'Mostaza Vintage'
  WHEN variant_code LIKE '%GRIS%' THEN 'Gris'
  WHEN variant_code LIKE '%LILA%' THEN 'Lila'
  WHEN variant_code LIKE '%CEDE%' THEN 'Celeste Denin'
  WHEN variant_code LIKE '%AZDE%' THEN 'Azul Denin'
  WHEN variant_code LIKE '%BEVE%' THEN 'Beige/Verde'
  WHEN variant_code LIKE '%OLIV%' THEN 'Oliva'
  WHEN variant_code LIKE '%ROJO%' THEN 'Rojo'
  WHEN variant_code LIKE '%BURD%' THEN 'Burdeo'
  WHEN variant_code LIKE '%MENT%' THEN 'Menta'
  WHEN variant_code LIKE '%PETR%' THEN 'Petroleo'
  WHEN variant_code LIKE '%ZULI%' THEN 'Azulino'
  WHEN variant_code LIKE '%MELO%' THEN 'Melon'
  WHEN variant_code LIKE '%LADR%' THEN 'Ladrillo'
  WHEN variant_code LIKE '%AMAR%' THEN 'Amarillo'
  WHEN variant_code LIKE '%CEOS%' THEN 'Celeste Oscuro'
  WHEN variant_code LIKE '%MORA%' THEN 'Morado'
  WHEN variant_code LIKE '%ROVI%' THEN 'Rojo Vintage'
  WHEN variant_code LIKE '%CAVE%' THEN 'Cafe Verdoso'
  WHEN variant_code LIKE '%AZAC%' THEN 'Azul Acero'
  WHEN variant_code LIKE '%VEBO%' THEN 'Verde Botella'
  WHEN variant_code LIKE '%AZPE%' THEN 'Azul Petrol'
  WHEN variant_code LIKE '%AZLA%' THEN 'Azul Lavanda'
  WHEN variant_code LIKE '%AZNA%' THEN 'Azul Navy'
  WHEN variant_code LIKE '%MOOS%' THEN 'Morado Oscuro'
  WHEN variant_code LIKE '%AROS%' THEN 'Arena Oscuro'
  WHEN variant_code like '%AZGR%' THEN 'Azul/Gris'
  WHEN variant_code LIKE '%AZSA%' THEN 'Azul/Sandia'
  WHEN variant_code LIKE '%AZVE%' THEN 'Azul/Verde'
  WHEN variant_code LIKE '%DEAZ%' THEN 'Denim/Azul'
  WHEN variant_code LIKE '%SADE%' THEN 'Sandia/Denim'
  WHEN variant_code LIKE '%CETU%' THEN 'Celeste/Turquesa'
  WHEN variant_code LIKE '%TOST%' THEN 'Tostado'
  WHEN variant_code LIKE '%RGME%' THEN 'Gris Medio'
  WHEN variant_code LIKE '%BETA%' THEN 'Beige Tan'
  WHEN variant_code LIKE '%VECA%' THEN 'Verde Caqui'
  WHEN variant_code LIKE '%TAUP%' THEN 'Taupe'
  WHEN variant_code LIKE '%MASI%' THEN 'Masilla'
  WHEN variant_code LIKE '%DENI%' THEN 'Denim'
  WHEN variant_code LIKE '%BLAZ%' THEN 'Blanco/Azul'
  ELSE 'Otro'
END AS color
FROM documents d
LEFT JOIN fbads f ON d.fecha_documento = f.date_fb
LEFT JOIN agg ON d.fecha_documento = agg.fecha_documento
LEFT JOIN maestra_productos mp
    ON d.variant_code = mp.sku
LEFT JOIN raw_maestra_producto2 rmp2 
    ON LEFT(CAST(d.variant_code AS STRING), 4) = rmp2.variante_code_desc
LEFT JOIN stock_actual st ON CAST(d.variant_id AS STRING) = st.variant_id
LEFT JOIN date_table dt ON d.fecha_documento = dt.date_raw
