{{
  config(
    materialized = 'table',
    schema = 'mart'
    )
}}

WITH documents AS(
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
    variant_code,
    variant_id,
    tipo_producto,
    nombre_producto,
    
    CASE WHEN document_type_id = 9 THEN costo_neto * -1 ELSE costo_neto END AS costo_neto,
    CASE WHEN document_type_id = 9 THEN precio_unitario * -1 ELSE precio_unitario END AS precio_unitario


FROM {{ ref('stg_document_explote') }}
)
,
fbads AS (

    SELECT 
    campaign_date as date_fb,
    sum(spend)  as costo_clp_fbads 
    FROM {{ ref('stg_meta_ads') }}
    GROUP by all
)
,
agg AS (
    SELECT
        fecha_documento ,
        COUNT(document_number) AS compras_mismo_dia
    FROM documents
  WHERE document_name IN ('NOTA DE CRÉDITO ELECTRÓNICA T', 'FACTURA DE COMPRA ELECTRÓNICA', 'BOLETA ELECTRÓNICA T', 'FACTURA ELECTRÓNICA T', 'NOTA DE DÉBITO ELECTRÓNICA T')
    GROUP BY fecha_documento
)

SELECT 
d.* ,
CASE
    WHEN d.document_name NOT IN (
        'NOTA DE CRÉDITO ELECTRÓNICA T', 'FACTURA DE COMPRA ELECTRÓNICA', 'BOLETA ELECTRÓNICA T', 'FACTURA ELECTRÓNICA T', 'NOTA DE DÉBITO ELECTRÓNICA T'
    ) THEN 0
    WHEN agg.compras_mismo_dia < 2 OR agg.compras_mismo_dia IS NULL THEN f.costo_clp_fbads
    ELSE f.costo_clp_fbads / agg.compras_mismo_dia
END AS inversion_desglosada_fbads
FROM documents d 
LEFT JOIN fbads f ON d.fecha_documento = f.date_fb
LEFT JOIN agg ON d.fecha_documento = agg.fecha_documento
