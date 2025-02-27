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
CASE 
        WHEN office_id = 1 THEN 'PROVIDENCIA'
        WHEN office_id = 2 THEN 'BUCAREST'
        WHEN office_id = 3 THEN 'MONTEVIDEO'
        WHEN office_id = 4 THEN 'APUMANQUE'
        WHEN office_id = 5 THEN 'HUÉRFANOS'
        ELSE 'OTRA OFICINA'  -- En caso de que haya más valores no contemplados
    END office,
d.variant_description,
CASE
  WHEN d.variant_description = 'S' THEN 1
  WHEN d.variant_description = 'M' THEN 2
  WHEN d.variant_description = 'L' THEN 3
  WHEN d.variant_description = 'XL' THEN 4
  WHEN d.variant_description = 'XXL' THEN 5
  WHEN d.variant_description = '3XL' THEN 6
  WHEN d.variant_description = '4XL' THEN 7
END AS orden_tallas,
d.variant_code,
d.variant_id

FROM documents d
LEFT JOIN document_type dt 
    ON document_type_id = dt.document_id 
