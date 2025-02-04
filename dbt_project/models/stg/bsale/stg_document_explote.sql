{{
  config(
    materialized = 'view',
    schema = 'stg'
    )
}}

WITH documents AS(

SELECT *
FROM {{ ref('raw_document') }}
),

document_type AS (

SELECT 
id AS document_id,
name AS document_name,
codeSii AS code_sii
FROM {{ ref('raw_document_type') }}

),

maestra_productos AS (

SELECT *
FROM {{ ref('raw_master_product') }}

),

office AS (

    SELECT *
    FROM {{ ref('raw_offices') }}
)

SELECT 
d.doc_id,
d.document_number,
d.emissionDate,
d.expirationDate,
d.generationDate,
d.document_type_id,
dt.document_name,
d.totalAmount AS total_amount,
d.netAmount AS net_amount,
d.taxAmount AS tax_amount,
d.detail_quantity,
d.detail_netUnitValue AS detail_unit_value,
d.detail_totalUnitValue AS detail_total_unit_value,
d.detail_netAmount AS detail_net_amount,
d.detail_taxAmount AS detail_tax_amount,
d.detail_totalAmount AS detail_total_amount,
d.detail_netDiscount AS detail_net_discount,
d.detail_totalDiscount AS detail_total_discount, 
d.office_id,
d.variant_description,
d.variant_code,
d.variant_id,
mp.tipo_producto,
mp.nombre_producto,
mp.costo_neto,
mp.precio_unitario,
o.name AS office
FROM documents d
LEFT JOIN document_type dt 
    ON document_type_id = dt.document_id 
LEFT JOIN maestra_productos mp
    ON d.variant_code = mp.sku
LEFT JOIN office o
    ON d.office_id = o.id
