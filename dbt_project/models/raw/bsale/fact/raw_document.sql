{{
  config(
    materialized = 'ephemeral',
    )
}}

WITH ----------------------------------------------------------------------------
-- (1) CTE "documents": selecciona las columnas principales y extrae IDs top-level
documents AS (
  SELECT
    -- Campos normales del documento
    id AS doc_id,
    emissionDate,
    expirationDate,
    generationDate,
    number AS document_number,
    totalAmount,
    netAmount,
    taxAmount,

    -- Campos JSON originales (si los quisieras conservar)
    document_type,
    client,
    office,
    user,
    references,
    sellers,
    payments,
    details,

    -- Extrae el "id" de cada objeto JSON top-level (para JOIN con tablas de dimensión)
    SAFE_CAST(JSON_EXTRACT_SCALAR(document_type, '$.id') AS INT64) AS document_type_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(client,        '$.id') AS INT64) AS client_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(office,        '$.id') AS INT64) AS office_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(user,          '$.id') AS INT64) AS user_id

  FROM `moss-448416.dataset.bsale_documents`

),

-- (2) CTE "flattened_details": aplanamos el array items en la columna "details"
flattened_details AS (
  SELECT
    d.doc_id,
    -- Mantén aquí los campos del documento que quieres replicar en cada fila de detail
    d.document_number,
    d.emissionDate,
    d.expirationDate,
    d.generationDate,
    d.totalAmount,
    d.netAmount,
    d.taxAmount,

    d.document_type_id,
    d.client_id,
    d.office_id,
    d.user_id,

    -- Cada elemento del array "items" de "details"
    detail_item
  FROM documents d
  -- Desanidamos con UNNEST
  CROSS JOIN UNNEST(
    JSON_EXTRACT_ARRAY(d.details, '$.items')
  ) AS detail_item
),

-- (3) CTE "flattened_references": aplanamos el array de "references"
--     Suponiendo que "references" sea un array en JSON (a veces es un array vacío o nulo).
--     Si no está anidado como un array, ajusta la ruta.
flattened_references AS (
  SELECT
    d.doc_id,
    -- Campos principales del documento
    d.document_number,
    d.emissionDate,
    d.expirationDate,
    d.generationDate,
    d.totalAmount,
    d.netAmount,
    d.taxAmount,

    d.document_type_id,
    d.client_id,
    d.office_id,
    d.user_id,

    -- Cada elemento del array "references"
    reference_item
  FROM documents d
  CROSS JOIN UNNEST(
    -- Ajusta '$' o '$.items' según la estructura real de "references" en tu JSON
    JSON_EXTRACT_ARRAY(d.references, '$')
  ) AS reference_item
)

-- (4) Consulta final: unimos details + references + campos del documento
--     Ten en cuenta que si un doc tiene N 'details' y M 'references', obtendrás N * M filas.
SELECT
  -- Campos principales (desde flattened_details o "documents")
  fd.doc_id,
  fd.document_number,
  fd.emissionDate,
  fd.expirationDate,
  fd.generationDate,
  fd.totalAmount,
  fd.netAmount,
  fd.taxAmount,
    -- Campos de la línea "detail_item"
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.id') AS INT64) AS detail_id,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.lineNumber') AS INT64) AS detail_lineNumber,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.quantity') AS FLOAT64) AS detail_quantity,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.netUnitValue') AS FLOAT64) AS detail_netUnitValue,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.totalUnitValue') AS FLOAT64) AS detail_totalUnitValue,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.netAmount') AS FLOAT64) AS detail_netAmount,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.taxAmount') AS FLOAT64) AS detail_taxAmount,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.totalAmount') AS FLOAT64) AS detail_totalAmount,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.netDiscount') AS FLOAT64) AS detail_netDiscount,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.totalDiscount') AS FLOAT64) AS detail_totalDiscount,
  JSON_EXTRACT_SCALAR(fd.detail_item, '$.note') AS detail_note,
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.relatedDetailId') AS INT64) AS detail_relatedDetailId,

  -- Subobjeto "variant"
  SAFE_CAST(JSON_EXTRACT_SCALAR(fd.detail_item, '$.variant.id') AS INT64) AS variant_id,
  JSON_EXTRACT_SCALAR(fd.detail_item, '$.variant.description') AS variant_description,
  JSON_EXTRACT_SCALAR(fd.detail_item, '$.variant.code') AS variant_code,


  -- IDs top-level
  fd.document_type_id,
  fd.client_id,
  fd.office_id,
  fd.user_id,

  ----------------------------------------------------------------------------
  ----------------------------------------------------------------------------
  -- Campos de la referencia (si existiera). 
  -- Si el documento no tiene references o los flatten suman muchas filas, ajusta a tus necesidades.
  -- Ejemplo: id y reason de la reference
  SAFE_CAST(JSON_EXTRACT_SCALAR(fr.reference_item, '$.id') AS INT64) AS reference_id,
  JSON_EXTRACT_SCALAR(fr.reference_item, '$.reason') AS reference_reason

FROM flattened_details fd
-- LEFT JOIN con flattened_references para no perder los detail, 
-- aunque un doc no tenga references (o viceversa).
LEFT JOIN flattened_references fr
  ON fd.doc_id = fr.doc_id
